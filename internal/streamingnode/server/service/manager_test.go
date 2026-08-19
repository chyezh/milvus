package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_walmanager"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestManagerServiceAssignRemoveWALReplica(t *testing.T) {
	walManager := mock_walmanager.NewMockManager(t)
	manager := NewManagerService(walManager)
	pchannel := types.PChannelInfo{Name: "p0", Term: 3, AccessMode: types.AccessModeRO}

	walManager.EXPECT().OpenWALReplica(mock.Anything, pchannel, int64(2), int64(5)).Return(nil)
	_, err := manager.Assign(context.Background(), &streamingpb.StreamingNodeManagerAssignRequest{
		Pchannel:        types.NewProtoFromPChannelInfo(pchannel),
		WalReplicaId:    2,
		AssignmentEpoch: 5,
	})
	require.NoError(t, err)

	walManager.EXPECT().RemoveWALReplica(mock.Anything, pchannel, int64(2), int64(5)).Return(nil)
	_, err = manager.Remove(context.Background(), &streamingpb.StreamingNodeManagerRemoveRequest{
		Pchannel:        types.NewProtoFromPChannelInfo(pchannel),
		WalReplicaId:    2,
		AssignmentEpoch: 5,
	})
	require.NoError(t, err)
}

func TestManagerServiceValidateRuntime(t *testing.T) {
	manager := NewManagerService(mock_walmanager.NewMockManager(t))

	t.Run("analyzer validation success", func(t *testing.T) {
		resp, err := manager.ValidateRuntime(context.Background(), &streamingpb.StreamingNodeManagerValidateRuntimeRequest{
			Validation: &streamingpb.StreamingNodeManagerValidateRuntimeRequest_Analyzer{
				Analyzer: &streamingpb.StreamingNodeRuntimeAnalyzerValidation{
					AnalyzerInfos: []*streamingpb.StreamingNodeRuntimeAnalyzerInfo{
						{
							Field:  "test_field",
							Name:   "test_analyzer",
							Params: `{}`,
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})

	t.Run("analyzer validation failure", func(t *testing.T) {
		resp, err := manager.ValidateRuntime(context.Background(), &streamingpb.StreamingNodeManagerValidateRuntimeRequest{
			Validation: &streamingpb.StreamingNodeManagerValidateRuntimeRequest_Analyzer{
				Analyzer: &streamingpb.StreamingNodeRuntimeAnalyzerValidation{
					AnalyzerInfos: []*streamingpb.StreamingNodeRuntimeAnalyzerInfo{
						{
							Field:  "test_field",
							Name:   "test_analyzer",
							Params: `{"invalid": "params"}`,
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.NotEqual(t, commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	})
}
