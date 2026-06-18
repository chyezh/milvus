package snview

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func normalizePersistedSNQueryView(view *viewpb.QueryViewOfShard) *viewpb.QueryViewOfShard {
	if qviews.QueryViewState(view.GetMeta().GetState()) == qviews.QueryViewStateUp {
		return view
	}
	dropped := proto.Clone(view).(*viewpb.QueryViewOfShard)
	dropped.Meta.State = viewpb.QueryViewState_QueryViewStateDropped
	return dropped
}

func assertQueryViewBelongsToPChannel(pchannel string, view *viewpb.QueryViewOfShard) {
	vchannel := view.GetMeta().GetVchannel()
	if funcutil.ToPhysicalChannel(vchannel) != pchannel {
		panic(fmt.Sprintf("query view vchannel %s does not belong to pchannel %s", vchannel, pchannel))
	}
}

func assertQueryViewsBelongToPChannel(pchannel string, views []*viewpb.QueryViewOfShard) {
	for _, view := range views {
		assertQueryViewBelongsToPChannel(pchannel, view)
	}
}
