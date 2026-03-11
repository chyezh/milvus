#!/usr/bin/env python3
"""
Rolling replacement of QueryNodes across resource groups.

Prerequisites:
- 2 replicas, each using a separate RG (e.g., rg1, rg2), each with N QueryNodes.
- Equal number of new QNs already added to __default_resource_group (2*N total).

Workflow:
1. Suspend QueryCoord balance.
2. For each RG, one-by-one for each old QN:
   a. transfer_node: move old QN from its RG to __recycle_resource_group.
      QueryCoord auto-fills the RG slot from __default_resource_group.
   b. Wait until the RG has the expected node count (new QN joined).
   c. Transfer all segments+channels from old QN to other nodes in the RG.
   d. Wait until old QN has no segments/channels.
3. Resume QueryCoord balance.

End state:
- __default_resource_group: empty
- __recycle_resource_group: all old QNs (no segments)
- Each RG: N new QNs with all segments
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime

import requests
from pymilvus import MilvusClient, utility

logger = logging.getLogger("rolling_replace")


def setup_logging(log_dir: str = "logs"):
    """Setup logging to both console and file."""
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"rolling_replace_{timestamp}.log")

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # File handler
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info("Log file: %s", os.path.abspath(log_file))
    return log_file


DEFAULT_RG = "__default_resource_group"
RECYCLE_RG = "__recycle_resource_group"


class MilvusOpsClient:
    """Client for Milvus management/ops REST API (port 9091 by default)."""

    def __init__(self, host: str, port: int = 9091):
        self.base_url = f"http://{host}:{port}"

    def _post(self, path: str, data: dict = None) -> dict:
        url = f"{self.base_url}{path}"
        resp = requests.post(url, data=data, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _get(self, path: str, params: dict = None) -> dict:
        url = f"{self.base_url}{path}"
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

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
        result = self._post(
            "/management/querycoord/distribution/get",
            data={"node_id": str(node_id)},
        )
        return result

    def transfer_segment(self, source_node_id: int, copy_mode: bool = False):
        """Transfer all segments from source_node to other nodes in the same RG."""
        result = self._post(
            "/management/querycoord/transfer/segment",
            data={
                "source_node_id": str(source_node_id),
                # omit target_node_id => ToAllNodes=true
                # omit segment_id => TransferAll=true
                "copy_mode": str(copy_mode).lower(),
            },
        )
        logger.info("Transfer segment from node %d result: %s", source_node_id, result)
        return result

    def transfer_channel(self, source_node_id: int, copy_mode: bool = False):
        """Transfer all channels from source_node to other nodes in the same RG."""
        result = self._post(
            "/management/querycoord/transfer/channel",
            data={
                "source_node_id": str(source_node_id),
                # omit target_node_id => ToAllNodes=true
                # omit channel_name => TransferAll=true
                "copy_mode": str(copy_mode).lower(),
            },
        )
        logger.info("Transfer channel from node %d result: %s", source_node_id, result)
        return result

    def suspend_node(self, node_id: int):
        """Suspend a query node (prevents new segment/channel loading)."""
        result = self._post(
            "/management/querycoord/node/suspend",
            data={"node_id": str(node_id)},
        )
        logger.info("Suspend node %d result: %s", node_id, result)
        return result

    def resume_node(self, node_id: int):
        """Resume a suspended query node."""
        result = self._post(
            "/management/querycoord/node/resume",
            data={"node_id": str(node_id)},
        )
        logger.info("Resume node %d result: %s", node_id, result)
        return result


def wait_for_rg_node_count(
    client: MilvusClient,
    rg_name: str,
    expected_count: int,
    timeout: int = 300,
    interval: int = 5,
):
    """Wait until resource group has exactly expected_count available nodes."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        rg_info = utility.describe_resource_group(rg_name, using=client._using)
        available = rg_info.num_available_node
        logger.info(
            "RG '%s': available_nodes=%d, expected=%d",
            rg_name,
            available,
            expected_count,
        )
        if available >= expected_count:
            return
        time.sleep(interval)
    raise TimeoutError(
        f"Timed out waiting for RG '{rg_name}' to have {expected_count} nodes"
    )


def wait_for_node_empty(
    ops: MilvusOpsClient,
    node_id: int,
    timeout: int = 600,
    interval: int = 5,
):
    """Wait until a query node has no segments and no channels."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        dist = ops.get_node_distribution(node_id)
        segments = dist.get("sealed_segmentIDs", [])
        channels = dist.get("channel_names", [])
        logger.info(
            "Node %d: segments=%d, channels=%d",
            node_id,
            len(segments),
            len(channels),
        )
        if len(segments) == 0 and len(channels) == 0:
            return
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for node {node_id} to become empty")


def setup_recycle_rg(client: MilvusClient):
    """Create or update __recycle_resource_group with requests=0, limits=100000."""
    from pymilvus import utility as util
    from pymilvus.client.types import ResourceGroupConfig

    rgs = util.list_resource_groups(using=client._using)
    config = ResourceGroupConfig(
        requests={"node_num": 0},
        limits={"node_num": 100000},
    )
    if RECYCLE_RG in rgs:
        logger.info("Updating existing %s", RECYCLE_RG)
        util.update_resource_groups(
            {RECYCLE_RG: config},
            using=client._using,
        )
    else:
        logger.info("Creating %s", RECYCLE_RG)
        util.create_resource_group(
            RECYCLE_RG,
            config=config,
            using=client._using,
        )


def rolling_replace(
    milvus_host: str,
    milvus_port: int,
    ops_port: int,
    rg_names: list,
    nodes_per_rg: int,
    transfer_timeout: int,
    node_timeout: int,
    dry_run: bool = False,
):
    """Execute rolling replacement of QueryNodes."""
    # Connect to Milvus
    uri = f"http://{milvus_host}:{milvus_port}"
    client = MilvusClient(uri=uri)
    ops = MilvusOpsClient(milvus_host, ops_port)

    # Step 0: Setup recycle RG
    logger.info("=" * 60)
    logger.info("Step 0: Setup recycle resource group")
    logger.info("=" * 60)
    setup_recycle_rg(client)

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

    # Identify old nodes per RG by listing current nodes via ops list and
    # cross-referencing with RG membership.
    # We rely on describe_resource_group to get node info.
    rg_old_nodes = {}
    for rg_name in rg_names:
        rg_info = utility.describe_resource_group(rg_name, using=client._using)
        logger.info("RG '%s' info: %s", rg_name, rg_info)
        # Extract node IDs - nodes is a dict {node_id: num_segments} in pymilvus
        if hasattr(rg_info, "nodes") and rg_info.nodes:
            node_ids = list(rg_info.nodes.keys())
        else:
            raise RuntimeError(
                f"Cannot get node IDs from RG info for '{rg_name}'. "
                f"Check pymilvus version supports describe_resource_group().nodes"
            )
        rg_old_nodes[rg_name] = node_ids
        logger.info("RG '%s' old nodes: %s", rg_name, node_ids)

    if dry_run:
        logger.info("[DRY RUN] Would process the following:")
        for rg_name, nodes in rg_old_nodes.items():
            logger.info("  RG '%s': replace %d nodes %s", rg_name, len(nodes), nodes)
        return

    # Step 1: Suspend balance
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
        # Step 2-3: Process each RG
        for rg_idx, rg_name in enumerate(rg_names):
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

            for node_idx, old_node_id in enumerate(old_nodes):
                logger.info("-" * 40)
                logger.info(
                    "Replacing node %d (%d/%d) in RG '%s'",
                    old_node_id,
                    node_idx + 1,
                    len(old_nodes),
                    rg_name,
                )
                logger.info("-" * 40)

                # Step 2a: Suspend the old node first to prevent new loads
                logger.info("Suspending node %d...", old_node_id)
                ops.suspend_node(old_node_id)

                # Step 2b: Transfer node from RG to recycle RG
                logger.info(
                    "Transferring node %d from '%s' to '%s'...",
                    old_node_id,
                    rg_name,
                    RECYCLE_RG,
                )
                utility.transfer_node(
                    rg_name,
                    RECYCLE_RG,
                    1,
                    using=client._using,
                )

                # Step 2c: Wait for coord to assign a new QN from default RG.
                # After transfer, the RG lost 1 node. Coord auto-fills from
                # default_rg because the RG config requests=N pulls from default.
                logger.info(
                    "Waiting for RG '%s' to have %d available nodes...",
                    rg_name,
                    nodes_per_rg,
                )
                wait_for_rg_node_count(
                    client, rg_name, nodes_per_rg, timeout=node_timeout
                )

                # Step 3a: Transfer all segments from old node to other nodes
                logger.info("Transferring all segments from node %d...", old_node_id)
                ops.transfer_segment(old_node_id, copy_mode=False)

                # Step 3b: Transfer all channels from old node
                logger.info("Transferring all channels from node %d...", old_node_id)
                ops.transfer_channel(old_node_id, copy_mode=False)

                # Step 3c: Wait for old node to be empty
                logger.info("Waiting for node %d to become empty...", old_node_id)
                wait_for_node_empty(ops, old_node_id, timeout=transfer_timeout)
                logger.info("Node %d is now empty.", old_node_id)

    except Exception:
        logger.exception("Error during rolling replacement!")
        logger.info(
            "Balance is still SUSPENDED. Manual intervention needed. "
            "Run: POST /management/querycoord/balance/resume to restore."
        )
        raise

    # Step 4: Resume balance (only on success)
    logger.info("=" * 60)
    logger.info("Step 4: Resume QueryCoord balance")
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
        required=True,
        help="Resource group names to process (e.g., rg1 rg2)",
    )
    parser.add_argument(
        "--nodes-per-rg",
        type=int,
        required=True,
        help="Expected number of nodes per RG",
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
        help="Timeout in seconds waiting for new node to join RG (default: 300)",
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
        nodes_per_rg=args.nodes_per_rg,
        transfer_timeout=args.transfer_timeout,
        node_timeout=args.node_timeout,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
