package qview

var (
	_ QueryViewOfShardAtWorkNode = (*QueryViewOfShardAtQueryNode)(nil)
	_ QueryViewOfShardAtWorkNode = (*QueryViewOfShardAtStreamingNode)(nil)
)

type QueryViewOfShardAtWorkNode interface {
	WorkNode() WorkNode

	State() QueryViewState

	Version() QueryViewVersion
}
