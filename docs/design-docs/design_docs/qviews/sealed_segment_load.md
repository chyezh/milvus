# QueryNode Sealed Segment Load Design

This document has moved to
[QueryNode QueryView Resource Preparation Design](qnview/querynode_queryview_resource_preparation.md).

The old content described an obsolete load-planner based design. The current
implementation uses `TransformAwareSegmentManager`,
`ViewAwareSealedSegmentManager`, and DataCoord-packed
`GetQueryViewSegmentLoadInfo`.
