#!/usr/bin/env python3
"""
Rolling replacement of QueryNodes across resource groups.

Prerequisites:
- 2 replicas, each using a separate RG (e.g., rg1, rg2), each with N QueryNodes.
- Equal number of new QNs already in __recycle_resource_group (2*N total).

Workflow:
1. Suspend QueryCoord balance.
2. For each RG (one replica at a time):
   a. Create __dirty_query_node_{rg_name} with requests=N, limits=N.
   b. Set RG requests=0, limits=0 to make all old nodes redundant.
   c. Coordinator auto-moves old nodes from RG to dirty (redundant → missing).
   d. Wait until RG has 0 nodes and dirty has N nodes.
   e. Set RG requests=N, limits=N to pull new nodes from recycle.
   f. Wait until RG has N available nodes (filled from recycle).
   g. For each old node in dirty, transfer_segment to a new node in RG.
   h. Wait until all old nodes have no segments.
3. Resume QueryCoord balance.

During step 2, the replica being processed is unavailable.
The other replica remains fully functional to serve queries.

Checkpoint/resume: state is saved to a JSON file after each key step.
If the script is interrupted, re-run with the same arguments to resume.

End state:
- __recycle_resource_group: empty
- __dirty_query_node_{rg_name}: all old QNs (no segments)
- Each RG: N new QNs with all segments loaded
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

import requests
from pymilvus import MilvusClient, utility
from pymilvus.client.types import ResourceGroupConfig

logger = logging.getLogger("rolling_replace")

RECYCLE_RG = "__recycle_resource_group"
RG_PREFIX = "rg_for_replica_"


def dirty_rg_name(rg_name: str) -> str:
    """Generate dirty resource group name for a given RG."""
    return f"__dirty_query_node_{rg_name}"


def setup_logging(log_dir: str = "logs"):
    """Setup logging to both console and file."""
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"rolling_replace_{timestamp}.log")

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info("Log file: %s", os.path.abspath(log_file))
    return log_file


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------


class Checkpoint:
    """Persist progress to a JSON file for resume after interruption.

    State schema:
    {
        "rg_names": ["rg1", "rg2"],
        "rg_old_nodes": {"rg1": [1,2,3], "rg2": [4,5,6]},
        "completed_rgs": ["rg1"],
        "current_rg": "rg2",
        "current_phase": "drain_old" | "fill_new" | "transfer_segments" | null,
        "transferred_old_nodes": [4, 5]   # old nodes whose segments are already cleared
    }
    """

    def __init__(self, path: str):
        self.path = path
        self.state: dict = {}

    def load(self) -> bool:
        """Load checkpoint. Returns True if a valid checkpoint was loaded."""
        if not os.path.exists(self.path):
            return False
        with open(self.path, "r") as f:
            self.state = json.load(f)
        logger.info("Loaded checkpoint from %s: %s", self.path, self.state)
        return True

    def save(self):
        """Persist current state to disk."""
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.state, f, indent=2)
        os.replace(tmp, self.path)
        logger.debug("Checkpoint saved: %s", self.state)

    def init(self, rg_names: list, rg_old_nodes: dict):
        """Initialize checkpoint for a fresh run."""
        self.state = {
            "rg_names": rg_names,
            "rg_old_nodes": {k: sorted(v) for k, v in rg_old_nodes.items()},
            "completed_rgs": [],
            "current_rg": None,
            "current_phase": None,
            "transferred_old_nodes": [],
        }
        self.save()

    @property
    def completed_rgs(self) -> list:
        return self.state.get("completed_rgs", [])

    @property
    def current_rg(self) -> str | None:
        return self.state.get("current_rg")

    @property
    def current_phase(self) -> str | None:
        return self.state.get("current_phase")

    @property
    def transferred_old_nodes(self) -> list:
        return self.state.get("transferred_old_nodes", [])

    @property
    def rg_old_nodes(self) -> dict:
        return self.state.get("rg_old_nodes", {})

    def begin_rg(self, rg_name: str):
        self.state["current_rg"] = rg_name
        self.state["current_phase"] = "drain_old"
        self.state["transferred_old_nodes"] = []
        self.save()

    def phase_done(self, phase: str, next_phase: str | None):
        self.state["current_phase"] = next_phase
        self.save()

    def mark_node_transferred(self, node_id: int):
        self.state["transferred_old_nodes"].append(node_id)
        self.save()

    def complete_rg(self, rg_name: str):
        self.state["completed_rgs"].append(rg_name)
        self.state["current_rg"] = None
        self.state["current_phase"] = None
        self.state["transferred_old_nodes"] = []
        self.save()

    def remove(self):
        if os.path.exists(self.path):
            os.remove(self.path)
            logger.info("Checkpoint file removed: %s", self.path)


# ---------------------------------------------------------------------------
# Milvus ops client
# ---------------------------------------------------------------------------


class MilvusOpsClient:
    """Client for Milvus management/ops REST API (port 9091 by default)."""

    def __init__(self, host: str, port: int = 9091):
        self.base_url = f"http://{host}:{port}"

    def _post(self, path: str, data: dict = None) -> dict:
        url = f"{self.base_url}{path}"
        resp = requests.post(url, data=data, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        # Validate response status if present
        if isinstance(result, dict) and result.get("status", "ok") == "error":
            raise RuntimeError(f"API error on {path}: {result}")
        return result

    def suspend_balance(self):
        logger.info("Suspending QueryCoord balance...")
        result = self._post("/management/querycoord/balance/suspend")
        logger.info("Suspend balance result: %s", result)
        return result

    def resume_balance(self):
        logger.info("Resuming QueryCoord balance...")
        result = self._post("/management/querycoord/balance/resume")
        logger.info("Resume balance result: %s", result)
        return result

    def get_balance_status(self) -> str:
        result = self._post("/management/querycoord/balance/status")
        return result.get("status", "unknown")

    def list_query_nodes(self) -> list:
        result = self._post("/management/querycoord/node/list")
        return result.get("nodeInfos", [])

    def get_node_distribution(self, node_id: int) -> dict:
        return self._post(
            "/management/querycoord/distribution/get",
            data={"node_id": str(node_id)},
        )

    def transfer_segment(
        self, source_node_id: int, target_node_id: int, copy_mode: bool = False
    ):
        """Transfer all segments from source_node to target_node."""
        result = self._post(
            "/management/querycoord/transfer/segment",
            data={
                "source_node_id": str(source_node_id),
                "target_node_id": str(target_node_id),
                "copy_mode": str(copy_mode).lower(),
            },
        )
        logger.info(
            "Transfer segment from node %d to node %d result: %s",
            source_node_id,
            target_node_id,
            result,
        )
        return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_rg_node_ids(client: MilvusClient, rg_name: str) -> set:
    """Get the set of node IDs currently in a resource group."""
    rg_info = utility.describe_resource_group(rg_name, using=client._using)
    if hasattr(rg_info, "nodes") and rg_info.nodes:
        return {node.node_id for node in rg_info.nodes}
    return set()


def get_rg_available_count(client: MilvusClient, rg_name: str) -> int:
    """Get the number of available nodes in a resource group."""
    rg_info = utility.describe_resource_group(rg_name, using=client._using)
    return rg_info.num_available_node


def wait_for_rg_node_count(
    client: MilvusClient,
    rg_name: str,
    expected_count: int,
    timeout: int = 300,
    interval: int = 5,
):
    """Wait until resource group has expected available node count."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        available = get_rg_available_count(client, rg_name)
        logger.info(
            "RG '%s': available_nodes=%d, expected=%d",
            rg_name,
            available,
            expected_count,
        )
        if available == expected_count:
            return
        time.sleep(interval)
    raise TimeoutError(
        f"Timed out waiting for RG '{rg_name}' to have {expected_count} available nodes"
    )


def wait_for_node_segments_empty(
    ops: MilvusOpsClient,
    node_id: int,
    timeout: int = 600,
    interval: int = 5,
):
    """Wait until a query node has no segments."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        dist = ops.get_node_distribution(node_id)
        segments = dist.get("sealed_segmentIDs", [])
        logger.info("Node %d: segments=%d", node_id, len(segments))
        if len(segments) == 0:
            return
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for node {node_id} to have no segments")


def ensure_rg_exists(client: MilvusClient, rg_name: str, req: int, lim: int):
    """Create or update a resource group with given requests/limits."""
    config = ResourceGroupConfig(
        requests={"node_num": req},
        limits={"node_num": lim},
    )
    rgs = utility.list_resource_groups(using=client._using)
    if rg_name in rgs:
        logger.info("Updating RG '%s': requests=%d, limits=%d", rg_name, req, lim)
        utility.update_resource_groups({rg_name: config}, using=client._using)
    else:
        logger.info("Creating RG '%s': requests=%d, limits=%d", rg_name, req, lim)
        utility.create_resource_group(rg_name, config=config, using=client._using)


def update_rg_config(client: MilvusClient, rg_name: str, req: int, lim: int):
    """Update resource group requests and limits."""
    config = ResourceGroupConfig(
        requests={"node_num": req},
        limits={"node_num": lim},
    )
    logger.info("Updating RG '%s': requests=%d, limits=%d", rg_name, req, lim)
    utility.update_resource_groups({rg_name: config}, using=client._using)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def process_rg(
    client: MilvusClient,
    ops: MilvusOpsClient,
    rg_name: str,
    old_nodes: set,
    ckpt: Checkpoint,
    transfer_timeout: int,
    node_timeout: int,
):
    """Process a single RG: drain old nodes → fill new nodes → transfer segments."""
    n = len(old_nodes)
    drg = dirty_rg_name(rg_name)
    phase = ckpt.current_phase

    # --- Phase: drain_old ---
    if phase == "drain_old":
        logger.info("Phase drain_old: moving old nodes from '%s' to '%s'", rg_name, drg)

        # Create dirty RG with 0,0 first (no node demand yet)
        ensure_rg_exists(client, drg, req=0, lim=0)

        # Atomically update both: rg → release nodes, dirty → accept nodes
        dirty_config = ResourceGroupConfig(
            requests={"node_num": n},
            limits={"node_num": n},
        )
        rg_config = ResourceGroupConfig(
            requests={"node_num": 0},
            limits={"node_num": 0},
        )
        logger.info(
            "Atomically updating '%s' (requests=0, limits=0) "
            "and '%s' (requests=%d, limits=%d)...",
            rg_name,
            drg,
            n,
            n,
        )
        utility.update_resource_groups(
            {rg_name: rg_config, drg: dirty_config},
            using=client._using,
        )

        logger.info("Waiting for RG '%s' to drain (0 nodes)...", rg_name)
        wait_for_rg_node_count(client, rg_name, 0, timeout=node_timeout)
        logger.info("Waiting for dirty RG '%s' to fill (%d nodes)...", drg, n)
        wait_for_rg_node_count(client, drg, n, timeout=node_timeout)

        dirty_nodes = get_rg_node_ids(client, drg)
        if dirty_nodes != old_nodes:
            raise RuntimeError(
                f"Dirty RG '{drg}' has unexpected nodes. "
                f"Expected: {sorted(old_nodes)}, Got: {sorted(dirty_nodes)}"
            )
        logger.info("Phase drain_old complete. dirty nodes: %s", sorted(dirty_nodes))
        ckpt.phase_done("drain_old", "fill_new")
        phase = "fill_new"

    # --- Phase: fill_new ---
    if phase == "fill_new":
        logger.info(
            "Phase fill_new: pulling new nodes from '%s' to '%s'", RECYCLE_RG, rg_name
        )

        update_rg_config(client, rg_name, req=n, lim=n)

        logger.info("Waiting for RG '%s' to fill with %d new nodes...", rg_name, n)
        wait_for_rg_node_count(client, rg_name, n, timeout=node_timeout)

        new_nodes = get_rg_node_ids(client, rg_name)
        overlap = new_nodes & old_nodes
        if overlap:
            raise RuntimeError(
                f"RG '{rg_name}' contains old nodes after fill: {sorted(overlap)}"
            )
        logger.info("Phase fill_new complete. new nodes: %s", sorted(new_nodes))
        ckpt.phase_done("fill_new", "transfer_segments")
        phase = "transfer_segments"

    # --- Phase: transfer_segments ---
    if phase == "transfer_segments":
        already_done = set(ckpt.transferred_old_nodes)
        dirty_nodes = get_rg_node_ids(client, drg)
        new_nodes = get_rg_node_ids(client, rg_name)
        new_node_list = sorted(new_nodes)

        remaining = sorted(dirty_nodes - already_done)
        logger.info(
            "Phase transfer_segments: %d remaining (of %d total), targets: %s",
            len(remaining),
            len(dirty_nodes),
            new_node_list,
        )

        for i, old_node_id in enumerate(remaining):
            target_node_id = new_node_list[i % len(new_node_list)]
            logger.info(
                "Transferring segments from node %d to node %d (%d/%d)...",
                old_node_id,
                target_node_id,
                i + 1,
                len(remaining),
            )
            ops.transfer_segment(old_node_id, target_node_id, copy_mode=False)

            logger.info("Waiting for node %d to have no segments...", old_node_id)
            wait_for_node_segments_empty(ops, old_node_id, timeout=transfer_timeout)
            logger.info("Node %d segments cleared.", old_node_id)
            ckpt.mark_node_transferred(old_node_id)

        logger.info("Phase transfer_segments complete for RG '%s'.", rg_name)


def rolling_replace(
    milvus_host: str,
    milvus_port: int,
    ops_port: int,
    rg_names: list,
    transfer_timeout: int,
    node_timeout: int,
    checkpoint_file: str,
    dry_run: bool = False,
):
    """Execute rolling replacement of QueryNodes."""
    uri = f"http://{milvus_host}:{milvus_port}"
    client = MilvusClient(uri=uri)
    ops = MilvusOpsClient(milvus_host, ops_port)

    ckpt = Checkpoint(checkpoint_file)
    resumed = ckpt.load()

    # Collect initial state
    logger.info("=" * 60)
    logger.info("Initial State")
    logger.info("=" * 60)
    all_rgs = utility.list_resource_groups(using=client._using)
    logger.info("Resource groups: %s", all_rgs)
    for rg in all_rgs:
        rg_info = utility.describe_resource_group(rg, using=client._using)
        logger.info("  %s: available=%d", rg, rg_info.num_available_node)

    all_nodes = ops.list_query_nodes()
    logger.info("All query nodes: %s", all_nodes)

    # Show segment distribution per node
    logger.info("-" * 40)
    logger.info("Segment Distribution")
    logger.info("-" * 40)
    for node_info in all_nodes:
        node_id = node_info.get("ID", node_info.get("id"))
        if node_id is None:
            continue
        try:
            dist = ops.get_node_distribution(node_id)
            channels = dist.get("channel_names") or []
            segments = dist.get("sealed_segmentIDs") or []
            logger.info(
                "  Node %d: sealed_segments=%d %s, channels=%d %s",
                node_id,
                len(segments),
                segments,
                len(channels),
                channels,
            )
        except Exception as e:
            logger.warning("  Node %d: failed to get distribution: %s", node_id, e)

    # Auto-discover RGs if not specified
    if not rg_names:
        rg_names = sorted(rg for rg in all_rgs if rg.startswith(RG_PREFIX))
        if not rg_names:
            raise RuntimeError(
                f"No resource groups matching '{RG_PREFIX}*' found. "
                "Specify --rg explicitly."
            )
        logger.info("Auto-discovered RGs: %s", rg_names)

    if resumed:
        # Use old_nodes from checkpoint (original snapshot before any changes)
        rg_old_nodes = {k: set(v) for k, v in ckpt.rg_old_nodes.items()}
        logger.info(
            "Resuming from checkpoint. Original old nodes: %s", ckpt.rg_old_nodes
        )
    else:
        # Fresh run: identify old nodes per RG
        rg_old_nodes = {}
        for rg_name in rg_names:
            node_ids = get_rg_node_ids(client, rg_name)
            if not node_ids:
                raise RuntimeError(f"RG '{rg_name}' has no nodes")
            rg_old_nodes[rg_name] = node_ids
            logger.info("RG '%s' old nodes (%d): %s", rg_name, len(node_ids), node_ids)

        # Verify recycle RG has enough new nodes
        recycle_nodes = get_rg_node_ids(client, RECYCLE_RG)
        total_needed = sum(len(nodes) for nodes in rg_old_nodes.values())
        logger.info(
            "Recycle RG has %d nodes, need %d for replacement",
            len(recycle_nodes),
            total_needed,
        )
        if len(recycle_nodes) < total_needed:
            raise RuntimeError(
                f"Not enough new nodes in '{RECYCLE_RG}': "
                f"have {len(recycle_nodes)}, need {total_needed}"
            )

        ckpt.init(rg_names, rg_old_nodes)

    if dry_run:
        logger.info("[DRY RUN] Would process the following:")
        for rg_name, nodes in rg_old_nodes.items():
            logger.info(
                "  RG '%s': replace %d nodes %s", rg_name, len(nodes), sorted(nodes)
            )
            logger.info("    dirty RG: '%s'", dirty_rg_name(rg_name))
        return

    # Step 1: Suspend balance (idempotent — safe to call if already suspended)
    logger.info("=" * 60)
    logger.info("Step 1: Suspend QueryCoord balance")
    logger.info("=" * 60)
    ops.suspend_balance()
    status = ops.get_balance_status()
    logger.info("Balance status: %s", status)
    if status != "suspended":
        raise RuntimeError(
            f"Failed to suspend balance: expected 'suspended', got '{status}'"
        )

    try:
        # Step 2: Process each RG (one replica at a time)
        for rg_idx, rg_name in enumerate(rg_names):
            if rg_name in ckpt.completed_rgs:
                logger.info("Skipping RG '%s' (already completed)", rg_name)
                continue

            old_nodes = rg_old_nodes[rg_name]
            logger.info("=" * 60)
            logger.info(
                "Processing RG '%s' (%d/%d) - %d nodes to replace",
                rg_name,
                rg_idx + 1,
                len(rg_names),
                len(old_nodes),
            )
            logger.info("=" * 60)

            # Set starting phase for new or resumed RG
            if ckpt.current_rg != rg_name:
                ckpt.begin_rg(rg_name)

            process_rg(
                client,
                ops,
                rg_name,
                old_nodes,
                ckpt,
                transfer_timeout=transfer_timeout,
                node_timeout=node_timeout,
            )

            ckpt.complete_rg(rg_name)
            logger.info("RG '%s' replacement complete.", rg_name)

    except Exception:
        logger.exception("Error during rolling replacement!")
        logger.info(
            "Balance is still SUSPENDED. Manual intervention needed. "
            "Run: POST /management/querycoord/balance/resume to restore. "
            "Re-run this script with the same arguments to resume from checkpoint."
        )
        raise

    # Step 3: Resume balance (only on success)
    logger.info("=" * 60)
    logger.info("Step 3: Resume QueryCoord balance")
    logger.info("=" * 60)
    ops.resume_balance()
    status = ops.get_balance_status()
    logger.info("Balance status: %s", status)

    # Final state report
    logger.info("=" * 60)
    logger.info("Final State")
    logger.info("=" * 60)
    all_rgs = utility.list_resource_groups(using=client._using)
    for rg in all_rgs:
        rg_info = utility.describe_resource_group(rg, using=client._using)
        logger.info("  %s: available=%d", rg, rg_info.num_available_node)

    ckpt.remove()
    logger.info("Rolling replacement completed successfully!")


def main():
    parser = argparse.ArgumentParser(
        description="Rolling replacement of Milvus QueryNodes across resource groups",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Milvus proxy host (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=19530,
        help="Milvus proxy gRPC port (default: 19530)",
    )
    parser.add_argument(
        "--ops-port",
        type=int,
        default=9091,
        help="Milvus management/ops HTTP port (default: 9091)",
    )
    parser.add_argument(
        "--rg",
        nargs="+",
        default=None,
        help="Resource group names to process. If not specified, auto-discovers "
        "RGs matching '%s*' prefix" % RG_PREFIX,
    )
    parser.add_argument(
        "--transfer-timeout",
        type=int,
        default=600,
        help="Timeout in seconds for segment transfer per node (default: 600)",
    )
    parser.add_argument(
        "--node-timeout",
        type=int,
        default=300,
        help="Timeout in seconds waiting for node transfers (default: 300)",
    )
    parser.add_argument(
        "--checkpoint-file",
        default="rolling_replace_checkpoint.json",
        help="Checkpoint file for resume (default: rolling_replace_checkpoint.json)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only show what would be done, don't execute",
    )
    parser.add_argument(
        "--log-dir",
        default="logs",
        help="Directory for log files (default: logs)",
    )
    args = parser.parse_args()

    setup_logging(args.log_dir)

    rolling_replace(
        milvus_host=args.host,
        milvus_port=args.port,
        ops_port=args.ops_port,
        rg_names=args.rg,
        transfer_timeout=args.transfer_timeout,
        node_timeout=args.node_timeout,
        checkpoint_file=args.checkpoint_file,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
