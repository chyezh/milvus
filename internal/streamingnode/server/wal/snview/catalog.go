package snview

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

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
