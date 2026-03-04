# QueryView State Machine Per-Node Analysis

> This document provides a detailed per-node, per-state analysis of the QueryView state machine.
> For each state, it covers: entry conditions, automatic behavior, valid transitions, and possible peer states with how the current node reacts to each.
> Reference: [Distributed Query View Design](distributed_query_view_design.md), [view.proto](../../pkg/proto/view.proto)

## 1. Coord State Machine

Coord is the leader of the global state machine. It generates QueryViews, drives state transitions, and persists state to ETCD for crash recovery.

Persisted states: **Preparing**, **Up**, **Down**, **Unrecoverable** (write-ahead), **Dropped** (deletion).

### 1.1 Preparing

**Entry Conditions:**
- Balancer generates a new view (triggers: DataView version change, balance request, QN online/offline, previous view becomes Unrecoverable, load config change such as LoadPartition/ReleasePartition).
- Recovery: loaded from ETCD in Preparing state.

**Automatic Behavior:**
1. Persist Preparing to ETCD (write-ahead to prevent state loss on crash).
2. Push Preparing to target SN and all QNs via SyncQueryView.
3. Wait for all nodes to report Ready.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Ready | All SN and QNs report Ready | None |
| Unrecoverable | Any node reports Unrecoverable, or Coord detects a QN loss that has segments in this view | Persist Unrecoverable to ETCD |

**Possible Peer States (and Coord's reaction):**
- **SN**: Not visible / Preparing / Ready / Unrecoverable / Up
  - Not visible (push not yet delivered) → Coord re-pushes Preparing.
  - Preparing (async preparation in progress) → Coord waits.
  - Ready → Coord marks SN as ready; checks if all nodes are ready.
  - Unrecoverable → Coord transitions to Unrecoverable.
  - Up (recovery scenario: SN restored an old Up view from persistence) → Coord fast-forwards to Up.
- **QN**: Not visible / Preparing / Ready / Unrecoverable
  - Not visible → Coord re-pushes Preparing.
  - Preparing → Coord waits.
  - Ready → Coord marks this QN as ready; checks if all nodes are ready.
  - Unrecoverable → Coord transitions to Unrecoverable.

### 1.2 Ready

**Entry Conditions:**
- All SN and QNs have reported Ready (automatic transition from Preparing).

**Automatic Behavior:**
1. Push Up to SN.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Up | SN confirms Up | Persist Up to ETCD |
| Unrecoverable | Coord detects a QN loss that has segments in this view | Persist Unrecoverable to ETCD |

**Possible Peer States (and Coord's reaction):**
- **SN**: Ready / Up
  - Ready (Up push not yet delivered) → Coord re-pushes Up.
  - Up → Coord transitions to Up.
- **QN**: Ready / Not visible (node lost)
  - Ready → Coord does nothing; normal.
  - Not visible (node lost) → Coord transitions to Unrecoverable.

> Note: Ready is NOT persisted. On Coord crash recovery, ETCD still shows Preparing; Coord re-pushes and catches up from node responses.

### 1.3 Up

**Entry Conditions:**
- SN confirmed Up (automatic transition from Ready).
- Recovery: loaded from ETCD in Up state.

**Automatic Behavior:**
1. Persist Up to ETCD (if transitioning from Ready).
2. View is now actively serving queries.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Down | Higher-version view has been Up for the lease period / ReleaseCollection | Persist Down to ETCD; push Down to SN |
| Unrecoverable | Coord detects a QN loss that has segments in this view, or SN reports Unrecoverable (e.g., UpRecovering failed due to OOM) | Persist Unrecoverable to ETCD |

**Possible Peer States (and Coord's reaction):**
- **SN**: Up / Unrecoverable
  - Up → Coord does nothing; normal.
  - Unrecoverable (SN failed during UpRecovering, e.g., OOM) → Coord transitions to Unrecoverable.
- **QN**: Ready / Not visible (node lost)
  - Ready → Coord does nothing; normal.
  - Not visible (node lost) → Coord transitions to Unrecoverable.

### 1.4 Down

**Entry Conditions:**
- Higher-version view has been Up for the lease period (old view expires).
- ReleaseCollection.
- Recovery: loaded from ETCD in Down state.

**Automatic Behavior:**
1. Persist Down to ETCD (if transitioning from Up).
2. Push Down to SN.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Dropping | SN confirms Down | None (no additional persistence) |

**Possible Peer States (and Coord's reaction):**
- **SN**: Up / Down
  - Up (Down push not yet delivered) → Coord re-pushes Down.
  - Down → Coord transitions to Dropping.
- **QN**: Ready
  - Ready → Coord does nothing.

### 1.5 Unrecoverable

**Entry Conditions:**
- Any node reports Unrecoverable during Preparing (automatic transition).
- Coord detects a QN loss while in Preparing, Ready, or Up (the lost QN has segments assigned in this view).
- SN reports Unrecoverable while in Up (e.g., UpRecovering failed due to OOM after SN crash recovery).

> Note: SN is bound to the vchannel and never experiences "node lost" from Coord's perspective. SN unavailability is handled at the channel assignment level, not per-view.

**Automatic Behavior:**
1. Persist Unrecoverable to ETCD (if not already persisted).
2. Immediately transition to Dropping.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Dropping | Immediate (may be pushed atomically with a new view's Preparing) | None |

**Possible Peer States (and Coord's reaction):**
- **SN**: Preparing / Ready / Up / Unrecoverable
  - Coord does not need to handle individually; proceeds directly to Dropping flow.
- **QN**: Preparing / Ready / Unrecoverable
  - Same as SN above.

> Note: Unrecoverable is a transient state; after persistence it immediately transitions to Dropping.

### 1.6 Dropping

**Entry Conditions:**
- SN confirmed Down in the Down phase (automatic transition from Down).
- Immediate transition from Unrecoverable (which itself can be reached from Preparing, Ready, or Up).

**Automatic Behavior:**
1. Push Dropped to all nodes (SN + all QNs).

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Dropped | All nodes confirm Dropped | Delete the view from ETCD |

**Possible Peer States (and Coord's reaction):**
- **SN**: Preparing / Ready / Up / Down / Unrecoverable / Dropped
  - Preparing / Ready / Up / Down / Unrecoverable (Dropped push not yet delivered) → Coord re-pushes Dropped.
  - Dropped → Coord marks SN as cleaned up.
- **QN**: Preparing / Ready / Unrecoverable / Dropped
  - Preparing / Ready / Unrecoverable (Dropped push not yet delivered) → Coord re-pushes Dropped.
  - Dropped → Coord marks this QN as cleaned up; checks if all nodes are cleaned up.

> Note: Dropping is NOT independently persisted. On Coord crash recovery, it recovers from Down or Unrecoverable and re-executes the Dropping flow.

### 1.7 Dropped

**Entry Conditions:**
- All nodes confirmed Dropped (automatic transition from Dropping).

**Automatic Behavior:**
1. Delete the view from ETCD.
2. Destroy the state machine instance.

**Transitions:** None (terminal state).

**Possible Peer States (and Coord's reaction):** None (view has been removed from all nodes).

---

## 2. StreamingNode State Machine

StreamingNode manages growing data and generates query plans. It persists only the Up recovery info for crash recovery.

Persisted states: **Up** recovery info only (the latest Up view).

### 2.1 Preparing

**Entry Conditions:**
- Received Preparing sync signal from Coord via SyncQueryView.

**Automatic Behavior:**
1. Check replica information.
2. Transition growing segments to queryable state.
3. Check whether the local Flusher's data_version > the view's data_version.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Ready | Resource preparation succeeded | Report Ready to Coord |
| Unrecoverable | data_version expired (growing segments already flushed and released) | Report Unrecoverable to Coord |
| Dropped | Received Dropped push from Coord (Coord aborted this view) | Release any prepared resources |

**Possible Coord States (and this node's reaction):**
- Coord in Preparing → SN continues preparing; normal scenario.
- Coord pushes Dropped → SN transitions to Dropped (Coord has abandoned this view).
- Other signals → SN ignores (invalid transition).

### 2.2 Ready

**Entry Conditions:**
- Preparing resource preparation succeeded (internal automatic transition).

**Automatic Behavior:** None; waiting for Coord to push Up.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Up | Received Up push from Coord | Persist recovery info; activate view |
| Dropped | Received Dropped push from Coord | Release resources |

**Possible Coord States (and this node's reaction):**
- Coord in Preparing / Ready → SN waits for Up push.
- Coord pushes Up → SN transitions to Up.
- Coord pushes Dropped → SN transitions to Dropped.

### 2.3 Up

**Entry Conditions:**
- Received Up push from Coord.

**Automatic Behavior:**
1. Persist recovery info (retain only the latest Up view, overwriting previous).
2. Activate the view for query plan generation.
3. Report Up to Coord.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Down | Received Down push from Coord | Delete persisted recovery info; stop generating query plans from this view (but can still serve query execution requests) |

**Possible Coord States (and this node's reaction):**
- Coord in Up / Down → SN does nothing; normal.
- Coord pushes Down → SN transitions to Down.
- Other signals → SN ignores.

### 2.4 UpRecovering (Local Transient)

**Entry Conditions:**
- SN crash recovery: the highest-version Up view is rebuilt from persisted recovery info.
- WAL consumption has not yet caught up; growing segment data ([A2] portion) is incomplete.

**Automatic Behavior:**
1. Replay WAL from the checkpoint position to recover growing segments.
2. Do NOT serve queries (data is incomplete).

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Up | WAL consumption catches up to current position | Begin serving queries |
| Down | Received Down push from Coord | Delete recovery info; abandon WAL catch-up |
| Unrecoverable | Local resource failure during WAL recovery (e.g., OOM) | Report Unrecoverable to Coord |

**Possible Coord States (and this node's reaction):**
- Coord considers this view to be in Up state (Coord is unaware of UpRecovering).
- Coord pushes Down → SN transitions to Down directly.
- Coord pushes Preparing (recovery scenario) → SN responds with Up to allow Coord to fast-forward.

> Note: UpRecovering is a transient local state on SN. It does NOT participate in Coord's state machine synchronization.

### 2.5 Down

**Entry Conditions:**
- Received Down push from Coord.

**Automatic Behavior:**
1. Delete persisted recovery info.
2. Stop generating query plans from this view (but can still serve query execution requests under plans already generated).
3. Report Down to Coord.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Dropped | Received Dropped push from Coord | Release all view-related resources |

**Possible Coord States (and this node's reaction):**
- Coord in Down / Dropping → SN does nothing; normal.
- Coord pushes Dropped → SN transitions to Dropped.

### 2.6 Unrecoverable

**Entry Conditions:**
- data_version check failed during Preparing (growing segments already flushed to sealed and released).
- Local resource failure during UpRecovering (e.g., OOM while replaying WAL to recover growing segments).

**Automatic Behavior:**
1. Report Unrecoverable to Coord.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Dropped | Received Dropped push from Coord | Release any prepared resources |

**Possible Coord States (and this node's reaction):**
- Coord in Preparing / Unrecoverable / Dropping → SN does nothing; normal.
- Coord pushes Dropped → SN transitions to Dropped.

### 2.7 Dropped

**Entry Conditions:**
- Received Dropped push from Coord (from Down / Unrecoverable / Preparing).

**Automatic Behavior:**
1. Release all view-related resources.
2. Report Dropped to Coord.

**Transitions:** None (terminal state; state machine instance destroyed).

---

## 3. QueryNode State Machine

QueryNode is fully stateless with no persistence and no recovery process. It does NOT observe Up, Down, or Dropping states — it can serve queries as soon as it reaches Ready.

### 3.1 Preparing

**Entry Conditions:**
- Received Preparing sync signal from Coord via SyncQueryView.

**Automatic Behavior:**
1. Asynchronously load segments from object storage.
2. Subscribe to the pure delete stream from SN.
3. Mark each segment as ready progressively; report incremental progress to Coord via `ready_segment_id_deltas` in responses.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Ready | All segments loaded successfully | Report Ready to Coord |
| Unrecoverable | Resource preparation failed (OOM, disk full, etc.) | Report Unrecoverable to Coord |
| Dropped | Received Dropped push from Coord (Coord aborted this view) | Release loaded resources; disconnect delete stream |

**Possible Coord States (and this node's reaction):**
- Coord in Preparing → QN continues preparing; normal.
- Coord pushes Dropped → QN transitions to Dropped directly.
- Other signals → QN ignores (invalid transition).

### 3.2 Ready

**Entry Conditions:**
- All segments loaded successfully (internal automatic transition from Preparing).

**Automatic Behavior:** None; can serve query requests. QN serves queries in Ready state — query plans are generated by SN, and QN only needs data to be ready.

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Dropped | Received Dropped push from Coord | Release segments; disconnect delete stream |

**Possible Coord States (and this node's reaction):**
- Coord may be in any state (Preparing / Ready / Up / Down / Dropping).
- QN is unaware of Up/Down; QN continues to serve as long as it is Ready.
- Coord pushes Dropped → QN transitions to Dropped.
- Other signals → QN ignores.

### 3.3 Unrecoverable

**Entry Conditions:**
- Resource preparation failed during Preparing (OOM, disk full, etc.).

**Automatic Behavior:**
1. Report Unrecoverable to Coord.
2. Retain already-loaded resources without rollback (waiting for Coord to orchestrate unified cleanup).

**Transitions:**

| Target State | Trigger | Transition Behavior |
|---|---|---|
| Dropped | Received Dropped push from Coord | Release all resources |

**Possible Coord States (and this node's reaction):**
- Coord in Preparing / Unrecoverable / Dropping → QN does nothing; normal.
- Coord pushes Dropped → QN transitions to Dropped.

### 3.4 Dropped

**Entry Conditions:**
- Received Dropped push from Coord.

**Automatic Behavior:**
1. Release all view-related segments.
2. Disconnect pure delete stream subscriptions.
3. Report Dropped to Coord.

**Transitions:** None (terminal state; state machine instance destroyed).

---

## 4. Summary Comparison

|  | Coord | StreamingNode | QueryNode |
|---|---|---|---|
| State Count | 7 | 7 (incl. UpRecovering) | 4 |
| Persistence | ETCD (Preparing/Up/Down/Unrecoverable/deletion) | Recovery info (Up only) | None |
| Unique States | Ready (transient), Dropping | UpRecovering (local transient) | — |
| Unaware States | — | Dropping (receives Dropped directly) | Up, Down, Dropping |

## 5. Open Questions

1. **SN Preparing → Dropped**: If Coord aborts a view because some QN reported Unrecoverable, SN may still be in Preparing. SN must accept Dropped from any non-terminal state.
2. **QN incremental progress reporting during Preparing**: The proto has `ready_segment_id_deltas` to support incremental segment-ready reporting, but the design document does not explicitly describe this behavior. This is useful for Coord to track loading progress.
3. **UpRecovering receiving Coord's re-push of Preparing (recovery scenario)**: SN should respond with Up directly to allow Coord to fast-forward, rather than re-entering the Preparing flow.
