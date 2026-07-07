# QueryView Event-Based Observability

## Package

The QueryView event model is defined in:

```text
internal/views/qviews/observe
```

Package layout:

```text
internal/views/qviews/observe
    event.go
    observer.go
```

`observe` depends on `internal/views/qviews` and the standard library. Coord,
QueryNode, and StreamingNode owner packages depend on `observe`.

## Observer

```go
type Observer interface {
    Observe(context.Context, Event)
}
```

## Event Interface

```go
type Event interface {
    isQueryViewEvent()
}

type BaseEvent struct{}

func (BaseEvent) isQueryViewEvent() {}
```

Every observable event is represented by one concrete Go type and embeds
`BaseEvent`. Consumers inspect events with type assertions or type switches.

```go
var _ Event = CoordViewQueryNodeLostAppliedEvent{}
var _ Event = QueryNodeSegmentUnrecoverableEvent{}

func Observe(ctx context.Context, event Event) {
    switch e := event.(type) {
    case CoordViewQueryNodeLostAppliedEvent:
        _ = e.Node
    case QueryNodeSegmentFailureEvent:
        _ = e.Err
    }
}
```

## Cardinality

Event type is split by cardinality. Each event payload carries the identity of
the observed object. The supported cardinalities are `node`, `view`,
`segment`, `view-segments`, `resource`, `persist`, and `sync`.

## Shared Types

```go
type ViewStateTransition struct {
    View qviews.QueryViewKey
    From qviews.QueryViewState
    To   qviews.QueryViewState
}
```

Events that describe a QueryView state-machine transition embed
`ViewStateTransition`.

## Events By Cardinality

### Node

Node events observe one worker node.

#### Coord

```go
// CoordQueryNodeLostDetectedEvent is emitted when Coord sync code observes
// QueryNode loss.
type CoordQueryNodeLostDetectedEvent struct {
    BaseEvent
    Node qviews.QueryNode
}
```

### View

View events observe one QueryView.

#### Coord

```go
// CoordViewCreatedEvent is emitted after Coord creates a new Preparing view.
type CoordViewCreatedEvent struct {
    BaseEvent
    View  qviews.QueryViewKey
    State qviews.QueryViewState
}

// CoordViewPreemptedEvent is emitted after Coord preempts a Preparing or Ready
// view while adding a new Preparing view.
type CoordViewPreemptedEvent struct {
    BaseEvent
    ViewStateTransition
    PreemptingDataVersion qviews.DataVersion
}

// CoordViewAdvancedFromUnrecoverableEvent is emitted after Coord advances an
// Unrecoverable view to Dropping.
type CoordViewAdvancedFromUnrecoverableEvent struct {
    BaseEvent
    ViewStateTransition
}

// CoordViewReleaseRequestedEvent is emitted after ShardViewManager applies
// RequestRelease to a view.
type CoordViewReleaseRequestedEvent struct {
    BaseEvent
    ViewStateTransition
}

// CoordViewHandoffToNewUpEvent is emitted after ShardViewManager transitions
// the previous Up view to Down because another view became Up.
type CoordViewHandoffToNewUpEvent struct {
    BaseEvent
    ViewStateTransition
    NewUpView qviews.QueryViewKey
}

// CoordViewReportAppliedEvent is emitted after ShardViewManager applies a
// work-node report to a view. ResourceReadyPercent is the report-side resource
// preparation progress in [0, 100].
type CoordViewReportAppliedEvent struct {
    BaseEvent
    ViewStateTransition
    Node                 qviews.WorkNode
    ReportedState        qviews.QueryViewState
    ResourceReadyPercent int64
}

// CoordViewQueryNodeLostAppliedEvent is emitted after ShardViewManager applies
// QueryNode loss to a view.
type CoordViewQueryNodeLostAppliedEvent struct {
    BaseEvent
    ViewStateTransition
    Node qviews.QueryNode
}
```

#### QueryNode

```go
// QueryNodeApplyCoordViewEvent is emitted after QueryNode applies a Coord view
// state.
type QueryNodeApplyCoordViewEvent struct {
    BaseEvent
    ViewStateTransition
}

// QueryNodeSegmentUnrecoverableEvent is emitted after the segment
// unrecoverable callback moves a view to Unrecoverable.
type QueryNodeSegmentUnrecoverableEvent struct {
    BaseEvent
    ViewStateTransition
    Err error
}

// QueryNodeReportViewEvent is emitted when QueryNode reports local view state.
type QueryNodeReportViewEvent struct {
    BaseEvent
    View  qviews.QueryViewKey
    State qviews.QueryViewState
}

// QueryNodeReleaseDoneEvent is emitted after QueryNode observes release
// completion for a view.
type QueryNodeReleaseDoneEvent struct {
    BaseEvent
    ViewStateTransition
}
```

#### StreamingNode

```go
// StreamingNodeApplyCoordViewEvent is emitted after StreamingNode applies a
// Coord view state.
type StreamingNodeApplyCoordViewEvent struct {
    BaseEvent
    ViewStateTransition
}

// StreamingNodeRecoveringDoneEvent is emitted after StreamingNode observes
// recovery completion for a view.
type StreamingNodeRecoveringDoneEvent struct {
    BaseEvent
    ViewStateTransition
}

// StreamingNodeReportViewEvent is emitted when StreamingNode reports local view
// state.
type StreamingNodeReportViewEvent struct {
    BaseEvent
    View  qviews.QueryViewKey
    State qviews.QueryViewState
}

// StreamingNodeReleaseDoneEvent is emitted after StreamingNode observes release
// completion for a view.
type StreamingNodeReleaseDoneEvent struct {
    BaseEvent
    ViewStateTransition
}
```

### Segment

Segment events observe one segment inside one QueryView.

#### QueryNode

```go
// QueryNodeSegmentFailureEvent is emitted when physical segment load or
// transform-log catch-up fails.
type QueryNodeSegmentFailureEvent struct {
    BaseEvent
    View      qviews.QueryViewKey
    SegmentID int64
    Err       error
}
```

### View-Segments

View-segments events observe one segment batch inside one QueryView.

#### QueryNode

```go
// QueryNodeAcquireSegmentsEvent is emitted when QueryNode starts acquiring
// segments for a new Preparing view.
type QueryNodeAcquireSegmentsEvent struct {
    BaseEvent
    View         qviews.QueryViewKey
    SegmentCount int
}

// QueryNodeSegmentsReadyEvent is emitted after the segment readiness callback
// moves a view forward.
type QueryNodeSegmentsReadyEvent struct {
    BaseEvent
    ViewStateTransition
    ReadySegmentCount int
}

// QueryNodeReleaseSegmentsEvent is emitted when QueryNode starts releasing
// segments for a view.
type QueryNodeReleaseSegmentsEvent struct {
    BaseEvent
    View qviews.QueryViewKey
}
```

### Resource

Resource events observe one StreamingNode resource for one QueryView.

#### StreamingNode

```go
// StreamingNodeAcquireResourceEvent is emitted when StreamingNode starts
// acquiring resources for a new Preparing view.
type StreamingNodeAcquireResourceEvent struct {
    BaseEvent
    View qviews.QueryViewKey
}

// StreamingNodeRecoverAcquireResourceEvent is emitted when StreamingNode starts
// acquiring resources for a recovered Up view.
type StreamingNodeRecoverAcquireResourceEvent struct {
    BaseEvent
    View qviews.QueryViewKey
}

// StreamingNodeResourceReadyEvent is emitted after the resource ready callback
// moves a view forward.
type StreamingNodeResourceReadyEvent struct {
    BaseEvent
    ViewStateTransition
}

// StreamingNodeReleaseResourceEvent is emitted when StreamingNode starts
// releasing resources for a view.
type StreamingNodeReleaseResourceEvent struct {
    BaseEvent
    View qviews.QueryViewKey
}
```

### Persist

Persist events observe one persisted QueryView state write.

#### Coord

```go
// CoordPersistViewEvent is emitted when ShardViewManager.flush persists a view
// state.
type CoordPersistViewEvent struct {
    BaseEvent
    View  qviews.QueryViewKey
    State qviews.QueryViewState
}
```

#### StreamingNode

```go
// StreamingNodePersistViewEvent is emitted when StreamingNode persists local
// view state.
type StreamingNodePersistViewEvent struct {
    BaseEvent
    View  qviews.QueryViewKey
    State qviews.QueryViewState
}
```

### Sync

Sync events observe one QueryView sync to one worker node.

#### Coord

```go
// CoordSyncViewEvent is emitted when ShardViewManager.flush syncs a view state
// to a worker node.
type CoordSyncViewEvent struct {
    BaseEvent
    View  qviews.QueryViewKey
    Node  qviews.WorkNode
    State qviews.QueryViewState
}

// CoordSyncViewFailedEvent is emitted when ShardViewManager.flush fails to sync
// a view state to a worker node.
type CoordSyncViewFailedEvent struct {
    BaseEvent
    View  qviews.QueryViewKey
    Node  qviews.WorkNode
    State qviews.QueryViewState
    Err   error
}
```

## Emission Semantics

Event emission runs in the owner layer that observes the action.

Transition-carrying events are emitted in the same critical section that
observes the state transition. `From` and `To` must match the owner-visible
state before and after the state-machine input.

Observer invocation is synchronous. Observer implementations must not block the
owner workflow. Observer failures must not change QueryView state-machine,
persistence, sync, or resource-release behavior.
