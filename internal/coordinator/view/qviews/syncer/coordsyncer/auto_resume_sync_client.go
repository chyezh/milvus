package coordsyncer

import "github.com/milvus-io/milvus/internal/coordinator/view/qviews"

type autoResumeSyncClient struct {
	resumeChan chan qviews.WorkNode // When resume happens, the work node will be sent from these channel.
}
