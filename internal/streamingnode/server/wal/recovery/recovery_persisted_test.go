package recovery

import (
	"context"
	"os"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()

	code := m.Run()
	if code != 0 {
		os.Exit(code)
	}
}

func TestRecoverRecoveryInfoFromMetaLoadsCatalogMeta(t *testing.T) {
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().ListSegmentAssignment(mock.Anything, "test_channel").Return([]*streamingpb.SegmentAssignmentMeta{
		validRecoverySegmentMeta(10),
	}, nil)
	snCatalog.EXPECT().ListVChannel(mock.Anything, "test_channel").Return([]*streamingpb.VChannelMeta{
		validRecoveryVChannelMeta("v1"),
	}, nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog))

	channel := types.PChannelInfo{Name: "test_channel"}
	rs := newRecoveryStorage(channel, newTestRecoveryCheckpoint(110))

	require.NoError(t, rs.recoverRecoveryInfoFromMeta(context.Background(), channel))
	require.NotNil(t, rs.checkpoint)
	assert.Equal(t, utility.RecoveryMagicStreamingInitialized, rs.checkpoint.Magic)
	assert.True(t, rs.checkpoint.MessageID.EQ(rmq.NewRmqID(110)))
}

func TestRecoverRecoveryInfoFromMetaRequiresRecoveryCheckpoint(t *testing.T) {
	resource.InitForTest(t)
	channel := types.PChannelInfo{Name: "test_channel"}
	rs := newRecoveryStorage(channel, nil)

	err := rs.recoverRecoveryInfoFromMeta(context.Background(), channel)
	require.Error(t, err)
	assert.ErrorContains(t, err, "missing recovery checkpoint")
}

func TestRecoverRecoveryInfoFromMetaRequiresDataCheckpoint(t *testing.T) {
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().ListSegmentAssignment(mock.Anything, "test_channel").Return([]*streamingpb.SegmentAssignmentMeta{}, nil)
	snCatalog.EXPECT().ListVChannel(mock.Anything, "test_channel").Return([]*streamingpb.VChannelMeta{
		validRecoveryVChannelMeta("v1"),
	}, nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog))

	channel := types.PChannelInfo{Name: "test_channel"}
	rs := newRecoveryStorage(channel, utility.NewWALCheckpointFromProto(&streamingpb.WALCheckpoint{
		MessageId:     rmq.NewRmqID(110).IntoProto(),
		TimeTick:      110,
		RecoveryMagic: utility.RecoveryMagicStreamingInitialized,
	}))

	err := rs.recoverRecoveryInfoFromMeta(context.Background(), channel)
	require.Error(t, err)
	assert.ErrorContains(t, err, "missing data checkpoint")
}

func TestRecoveryMetaMapsRejectDuplicateOwners(t *testing.T) {
	_, err := vchannelMetaMap([]*streamingpb.VChannelMeta{
		validRecoveryVChannelMeta("v1"),
		validRecoveryVChannelMeta("v1"),
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "duplicate vchannel owner")

	_, err = segmentAssignmentMetaMap([]*streamingpb.SegmentAssignmentMeta{
		validRecoverySegmentMeta(10),
		validRecoverySegmentMeta(10),
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "duplicate segment owner")
}

func TestValidateRecoveredGrowingMeta(t *testing.T) {
	tests := []struct {
		name    string
		build   func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta)
		wantErr string
	}{
		{
			name: "accepts empty retained meta",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				return nil, nil
			},
		},
		{
			name: "accepts cleaned partitions",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CollectionInfo.Partitions = nil
				return vchannelMap(vchannel), nil
			},
		},
		{
			name: "accepts retained segment without vchannel meta",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				segment := validRecoverySegmentMeta(10)
				segment.Vchannel = "missing-vchannel"
				return nil, segmentMap(segment)
			},
		},
		{
			name: "accepts retained segment from different collection on same vchannel",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				segment := validRecoverySegmentMeta(10)
				segment.CollectionId = 101
				return vchannelMap(validRecoveryVChannelMeta("v1")), segmentMap(segment)
			},
		},
		{
			name: "rejects vchannel missing owner",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("")
				return map[string]*streamingpb.VChannelMeta{"v1": vchannel}, nil
			},
			wantErr: "vchannel missing vchannel owner",
		},
		{
			name: "rejects vchannel missing collection owner",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CollectionInfo.CollectionId = 0
				return vchannelMap(vchannel), nil
			},
			wantErr: "vchannel missing collection owner",
		},
		{
			name: "rejects unknown vchannel state",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.State = streamingpb.VChannelState_VCHANNEL_STATE_UNKNOWN
				return vchannelMap(vchannel), nil
			},
			wantErr: "unknown vchannel state",
		},
		{
			name: "rejects unknown growing segment mode",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.GrowingSegmentMode = streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_UNKNOWN
				return vchannelMap(vchannel), nil
			},
			wantErr: "unknown growing segment mode",
		},
		{
			name: "rejects vchannel missing checkpoint",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CheckpointTimeTick = 0
				return vchannelMap(vchannel), nil
			},
			wantErr: "vchannel missing checkpoint timetick",
		},
		{
			name: "rejects vchannel missing latest data version",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.LatestDataVersion = nil
				return vchannelMap(vchannel), nil
			},
			wantErr: "missing latest data version",
		},
		{
			name: "rejects schema missing list",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CollectionInfo.Schemas = nil
				return vchannelMap(vchannel), nil
			},
			wantErr: "missing schemas",
		},
		{
			name: "rejects schema missing checkpoint",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CollectionInfo.Schemas[0].CheckpointTimeTick = 0
				return vchannelMap(vchannel), nil
			},
			wantErr: "missing schema checkpoint timetick",
		},
		{
			name: "rejects schema missing body",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CollectionInfo.Schemas[0].Schema = nil
				return vchannelMap(vchannel), nil
			},
			wantErr: "missing schema body",
		},
		{
			name: "rejects unknown schema state",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CollectionInfo.Schemas[0].State = streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_UNKNOWN
				return vchannelMap(vchannel), nil
			},
			wantErr: "unknown schema state",
		},
		{
			name: "rejects schema after vchannel checkpoint",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CollectionInfo.Schemas[0].CheckpointTimeTick = 11
				return vchannelMap(vchannel), nil
			},
			wantErr: "schema checkpoint after vchannel checkpoint",
		},
		{
			name: "rejects partition missing owner",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CollectionInfo.Partitions[0].PartitionId = 0
				return vchannelMap(vchannel), nil
			},
			wantErr: "partition missing partition owner",
		},
		{
			name: "rejects unknown partition state",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CollectionInfo.Partitions[0].State = streamingpb.PartitionState_PARTITION_STATE_UNKNOWN
				return vchannelMap(vchannel), nil
			},
			wantErr: "unknown partition state",
		},
		{
			name: "rejects dropped partition missing drop timetick",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CollectionInfo.Partitions[0].State = streamingpb.PartitionState_PARTITION_STATE_DROPPED
				return vchannelMap(vchannel), nil
			},
			wantErr: "dropped partition missing drop timetick",
		},
		{
			name: "rejects dropped partition after vchannel checkpoint",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CheckpointTimeTick = 9
				vchannel.CollectionInfo.Partitions[0].State = streamingpb.PartitionState_PARTITION_STATE_DROPPED
				vchannel.CollectionInfo.Partitions[0].TombstoneTimeTick = 10
				return vchannelMap(vchannel), nil
			},
			wantErr: "dropped partition checkpoint before drop timetick",
		},
		{
			name: "rejects tombstoned partition missing timetick",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CollectionInfo.Partitions[0].State = streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED
				return vchannelMap(vchannel), nil
			},
			wantErr: "tombstoned partition missing tombstone timetick",
		},
		{
			name: "rejects tombstoned partition before meta checkpoint",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CheckpointTimeTick = 9
				vchannel.DataCheckpointTimeTick = 10
				vchannel.CollectionInfo.Partitions[0].State = streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED
				vchannel.CollectionInfo.Partitions[0].TombstoneTimeTick = 10
				return vchannelMap(vchannel), nil
			},
			wantErr: "tombstoned partition checkpoint before tombstone timetick",
		},
		{
			name: "rejects tombstoned partition before data checkpoint",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CheckpointTimeTick = 10
				vchannel.DataCheckpointTimeTick = 9
				vchannel.CollectionInfo.Partitions[0].State = streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED
				vchannel.CollectionInfo.Partitions[0].TombstoneTimeTick = 10
				return vchannelMap(vchannel), nil
			},
			wantErr: "tombstoned partition data checkpoint before tombstone timetick",
		},
		{
			name: "rejects segment missing owner",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				segment := validRecoverySegmentMeta(10)
				segment.SegmentId = 0
				return nil, map[int64]*streamingpb.SegmentAssignmentMeta{10: segment}
			},
			wantErr: "segment missing segment owner",
		},
		{
			name: "rejects segment missing collection owner",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				segment := validRecoverySegmentMeta(10)
				segment.CollectionId = 0
				return nil, segmentMap(segment)
			},
			wantErr: "segment missing collection owner",
		},
		{
			name: "rejects segment missing partition owner",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				segment := validRecoverySegmentMeta(10)
				segment.PartitionId = 0
				return nil, segmentMap(segment)
			},
			wantErr: "segment missing partition owner",
		},
		{
			name: "rejects segment missing vchannel owner",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				segment := validRecoverySegmentMeta(10)
				segment.Vchannel = ""
				return nil, segmentMap(segment)
			},
			wantErr: "segment missing vchannel owner",
		},
		{
			name: "rejects unknown segment state",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				segment := validRecoverySegmentMeta(10)
				segment.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_UNKNOWN
				return nil, segmentMap(segment)
			},
			wantErr: "unknown segment state",
		},
		{
			name: "rejects segment missing checkpoint",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				segment := validRecoverySegmentMeta(10)
				segment.CheckpointTimeTick = 0
				return nil, segmentMap(segment)
			},
			wantErr: "segment missing checkpoint timetick",
		},
		{
			name: "rejects segment missing create timetick",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				segment := validRecoverySegmentMeta(10)
				segment.Stat.CreateSegmentTimeTick = 0
				return nil, segmentMap(segment)
			},
			wantErr: "missing create segment timetick",
		},
		{
			name: "rejects segment checkpoint before create timetick",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				segment := validRecoverySegmentMeta(10)
				segment.CheckpointTimeTick = 4
				segment.Stat.CreateSegmentTimeTick = 5
				return nil, segmentMap(segment)
			},
			wantErr: "segment checkpoint before create segment timetick",
		},
		{
			name: "rejects segment missing persisted storage",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				segment := validRecoverySegmentMeta(10)
				segment.PersistedStorage = nil
				return nil, segmentMap(segment)
			},
			wantErr: "segment missing persisted storage",
		},
		{
			name: "rejects tombstoned segment missing timetick",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				segment := tombstonedRecoverySegmentMeta(10, 0)
				return nil, segmentMap(segment)
			},
			wantErr: "tombstoned segment missing tombstone timetick",
		},
		{
			name: "rejects tombstoned segment before meta checkpoint",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				segment := tombstonedRecoverySegmentMeta(10, 10)
				segment.CheckpointTimeTick = 9
				return nil, segmentMap(segment)
			},
			wantErr: "tombstoned segment checkpoint before tombstone timetick",
		},
		{
			name: "rejects tombstoned segment before data checkpoint",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				segment := tombstonedRecoverySegmentMeta(10, 10)
				segment.DataCheckpointTimeTick = 9
				return nil, segmentMap(segment)
			},
			wantErr: "tombstoned segment data checkpoint before tombstone timetick",
		},
		{
			name: "rejects tombstoned vchannel missing timetick",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := tombstonedRecoveryVChannelMeta("v1", 0)
				return vchannelMap(vchannel), nil
			},
			wantErr: "tombstoned vchannel missing tombstone timetick",
		},
		{
			name: "rejects tombstoned vchannel before meta checkpoint",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := tombstonedRecoveryVChannelMeta("v1", 10)
				vchannel.CheckpointTimeTick = 9
				return vchannelMap(vchannel), nil
			},
			wantErr: "tombstoned vchannel checkpoint before tombstone timetick",
		},
		{
			name: "rejects tombstoned vchannel before data checkpoint",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := tombstonedRecoveryVChannelMeta("v1", 10)
				vchannel.DataCheckpointTimeTick = 9
				return vchannelMap(vchannel), nil
			},
			wantErr: "tombstoned vchannel data checkpoint before tombstone timetick",
		},
		{
			name: "rejects dropped vchannel with future segment",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
				vchannel.CheckpointTimeTick = 10
				segment := validRecoverySegmentMeta(10)
				segment.Stat.CreateSegmentTimeTick = 10
				return vchannelMap(vchannel), segmentMap(segment)
			},
			wantErr: "dropped vchannel has future segment",
		},
		{
			name: "rejects tombstoned vchannel with unfinished covered segment",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := tombstonedRecoveryVChannelMeta("v1", 10)
				segment := validRecoverySegmentMeta(10)
				segment.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
				return vchannelMap(vchannel), segmentMap(segment)
			},
			wantErr: "tombstoned vchannel has unfinished segment",
		},
		{
			name: "rejects tombstoned vchannel with future segment",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := tombstonedRecoveryVChannelMeta("v1", 10)
				segment := tombstonedRecoverySegmentMeta(10, 11)
				segment.Stat.CreateSegmentTimeTick = 11
				return vchannelMap(vchannel), segmentMap(segment)
			},
			wantErr: "tombstoned vchannel has future segment",
		},
		{
			name: "rejects tombstoned vchannel before covered segment tombstone",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := tombstonedRecoveryVChannelMeta("v1", 10)
				segment := tombstonedRecoverySegmentMeta(10, 12)
				return vchannelMap(vchannel), segmentMap(segment)
			},
			wantErr: "tombstoned vchannel before covered segment tombstone",
		},
		{
			name: "rejects dropped partition with future segment",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CollectionInfo.Partitions[0].State = streamingpb.PartitionState_PARTITION_STATE_DROPPED
				vchannel.CollectionInfo.Partitions[0].TombstoneTimeTick = 10
				segment := validRecoverySegmentMeta(10)
				segment.Stat.CreateSegmentTimeTick = 10
				return vchannelMap(vchannel), segmentMap(segment)
			},
			wantErr: "dropped partition has future segment",
		},
		{
			name: "rejects tombstoned partition with unfinished covered segment",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CollectionInfo.Partitions[0].State = streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED
				vchannel.CollectionInfo.Partitions[0].TombstoneTimeTick = 10
				segment := validRecoverySegmentMeta(10)
				segment.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED
				return vchannelMap(vchannel), segmentMap(segment)
			},
			wantErr: "tombstoned partition has unfinished segment",
		},
		{
			name: "rejects tombstoned partition with future segment",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CollectionInfo.Partitions[0].State = streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED
				vchannel.CollectionInfo.Partitions[0].TombstoneTimeTick = 10
				segment := tombstonedRecoverySegmentMeta(10, 11)
				segment.Stat.CreateSegmentTimeTick = 11
				return vchannelMap(vchannel), segmentMap(segment)
			},
			wantErr: "tombstoned partition has future segment",
		},
		{
			name: "rejects tombstoned partition before covered segment tombstone",
			build: func() (map[string]*streamingpb.VChannelMeta, map[int64]*streamingpb.SegmentAssignmentMeta) {
				vchannel := validRecoveryVChannelMeta("v1")
				vchannel.CheckpointTimeTick = 12
				vchannel.DataCheckpointTimeTick = 12
				vchannel.CollectionInfo.Partitions[0].State = streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED
				vchannel.CollectionInfo.Partitions[0].TombstoneTimeTick = 10
				segment := tombstonedRecoverySegmentMeta(10, 12)
				return vchannelMap(vchannel), segmentMap(segment)
			},
			wantErr: "tombstoned partition before covered segment tombstone",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vchannels, segments := tt.build()
			err := validateRecoveredGrowingMeta(vchannels, segments)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func vchannelMap(vchannels ...*streamingpb.VChannelMeta) map[string]*streamingpb.VChannelMeta {
	result := make(map[string]*streamingpb.VChannelMeta, len(vchannels))
	for _, vchannel := range vchannels {
		result[vchannel.GetVchannel()] = vchannel
	}
	return result
}

func segmentMap(segments ...*streamingpb.SegmentAssignmentMeta) map[int64]*streamingpb.SegmentAssignmentMeta {
	result := make(map[int64]*streamingpb.SegmentAssignmentMeta, len(segments))
	for _, segment := range segments {
		result[segment.GetSegmentId()] = segment
	}
	return result
}

func validRecoveryVChannelMeta(vchannel string) *streamingpb.VChannelMeta {
	return &streamingpb.VChannelMeta{
		Vchannel:               vchannel,
		State:                  streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 10,
		LatestDataVersion:      &viewpb.DataVersion{},
		GrowingSegmentMode:     streamingpb.GrowingSegmentMode_GROWING_SEGMENT_MODE_WRITE_ONLY,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 100,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 200, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "schema"},
					CheckpointTimeTick: 1,
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
				},
			},
		},
	}
}

func tombstonedRecoveryVChannelMeta(vchannel string, tombstoneTimeTick uint64) *streamingpb.VChannelMeta {
	meta := proto.Clone(validRecoveryVChannelMeta(vchannel)).(*streamingpb.VChannelMeta)
	meta.State = streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED
	meta.TombstoneTimeTick = tombstoneTimeTick
	meta.CheckpointTimeTick = max(meta.CheckpointTimeTick, tombstoneTimeTick)
	meta.DataCheckpointTimeTick = max(meta.DataCheckpointTimeTick, tombstoneTimeTick)
	return meta
}

func validRecoverySegmentMeta(segmentID int64) *streamingpb.SegmentAssignmentMeta {
	return &streamingpb.SegmentAssignmentMeta{
		SegmentId:              segmentID,
		CollectionId:           100,
		PartitionId:            200,
		Vchannel:               "v1",
		State:                  streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
		CheckpointTimeTick:     10,
		DataCheckpointTimeTick: 10,
		PersistedStorage:       &streamingpb.L1SegmentPersistedStorage{},
		Stat:                   &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 5},
	}
}

func tombstonedRecoverySegmentMeta(segmentID int64, tombstoneTimeTick uint64) *streamingpb.SegmentAssignmentMeta {
	meta := proto.Clone(validRecoverySegmentMeta(segmentID)).(*streamingpb.SegmentAssignmentMeta)
	meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED
	meta.TombstoneTimeTick = tombstoneTimeTick
	meta.CheckpointTimeTick = max(meta.CheckpointTimeTick, tombstoneTimeTick)
	meta.DataCheckpointTimeTick = max(meta.DataCheckpointTimeTick, tombstoneTimeTick)
	return meta
}

func newTestRecoveryCheckpoint(timetick uint64) *utility.WALCheckpoint {
	return utility.NewWALCheckpointFromProto(&streamingpb.WALCheckpoint{
		MessageId:     rmq.NewRmqID(int64(timetick)).IntoProto(),
		TimeTick:      timetick,
		RecoveryMagic: utility.RecoveryMagicStreamingInitialized,
		DataCheckpoint: &streamingpb.WALConsumeCheckpoint{
			MessageId: rmq.NewRmqID(int64(timetick)).IntoProto(),
			TimeTick:  timetick,
		},
	})
}
