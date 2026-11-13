package streamingnode

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func replayTestCheckpoint(id int64, tick uint64) *streamingpb.WALCheckpoint {
	return &streamingpb.WALCheckpoint{MessageId: walimplstest.NewTestMessageID(id).IntoProto(), TimeTick: tick, Term: 7, LogGeneration: 1}
}

func TestWALReplayPinsProtectPhysicalPositionAcrossTakeover(t *testing.T) {
	reader, writer, store := retentionCatalogs(t)
	ctx := t.Context()
	cp := replayTestCheckpoint(100, 1000)
	require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, cp)))
	// An outstanding transaction's physical restart can be much older than
	// another pin with a lower TimeTick. Choose by MessageID, never TimeTick.
	pins := []*streamingpb.WALReplayPin{
		{PinId: "transaction", SourceTerm: 7, Checkpoint: replayTestCheckpoint(10, 900)},
		{PinId: "ordinary", SourceTerm: 7, Checkpoint: replayTestCheckpoint(20, 800)},
	}
	for _, pin := range pins {
		require.NoError(t, reader.PinWALReplay(ctx, "p", pin))
		require.NoError(t, reader.PinWALReplay(ctx, "p", pin))
	}
	bound, err := writer.AuthorizeWALTruncation(ctx, "p", 7, cp)
	require.NoError(t, err)
	require.True(t, proto.Equal(pins[0].Checkpoint.MessageId, bound.MessageId))
	require.EqualValues(t, 900, bound.TimeTick)
	cp.Term = 8
	require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, cp)))
	_, err = writer.AuthorizeWALTruncation(ctx, "p", 7, cp)
	require.Error(t, err)
	bound, err = writer.AuthorizeWALTruncation(ctx, "p", 8, cp)
	require.NoError(t, err)
	require.True(t, proto.Equal(pins[0].Checkpoint.MessageId, bound.MessageId), "takeover does not release a live old transfer")
	require.NoError(t, reader.ReleaseWALReplay(ctx, "p", "transaction"))
	bound, err = writer.AuthorizeWALTruncation(ctx, "p", 8, cp)
	require.NoError(t, err)
	require.True(t, proto.Equal(pins[1].Checkpoint.MessageId, bound.MessageId))
	require.NoError(t, reader.ReleaseWALReplay(ctx, "p", "ordinary"))
	bound, err = writer.AuthorizeWALTruncation(ctx, "p", 8, cp)
	require.NoError(t, err)
	require.True(t, proto.Equal(cp.MessageId, bound.MessageId))
	// The backend request may still be in flight: its durable authorization
	// already prevents a newly captured baseline from pinning an older range.
	require.Error(t, reader.PinWALReplay(ctx, "p", &streamingpb.WALReplayPin{PinId: "late", SourceTerm: 8, Checkpoint: replayTestCheckpoint(99, 999)}))
	require.NoError(t, reader.PinWALReplay(ctx, "p", &streamingpb.WALReplayPin{PinId: "at-boundary", SourceTerm: 8, Checkpoint: replayTestCheckpoint(100, 999)}))
	bound, err = writer.AuthorizeWALTruncation(ctx, "p", 8, replayTestCheckpoint(50, 500))
	require.NoError(t, err)
	require.True(t, proto.Equal(cp.MessageId, bound.MessageId), "out-of-order truncation requests cannot regress the fence")
	_, err = writer.AuthorizeWALTruncation(ctx, "p", 8, replayTestCheckpoint(101, 1000))
	require.Error(t, err)
	_, err = writer.AuthorizeWALTruncation(ctx, "p", 8, replayTestCheckpoint(100, 1001))
	require.Error(t, err)
}

func TestWALReplayPinAndTruncationRace(t *testing.T) {
	for _, interleave := range []string{"pin-first", "truncate-first", "release-first", "takeover-first"} {
		t.Run(interleave, func(t *testing.T) {
			mockey.PatchConvey("capture and backend authorization share one CAS", t, func() {
				first, second, store := retentionCatalogs(t)
				cp := replayTestCheckpoint(100, 1000)
				require.NoError(t, store.Save(t.Context(), buildConsumeCheckpointKey("p"), checkpointBytesOf(t, cp)))
				pin := &streamingpb.WALReplayPin{PinId: "bootstrap", SourceTerm: 7, Checkpoint: replayTestCheckpoint(10, 100)}
				var original func(*catalog, context.Context, string, string, map[string]string, []string, ...predicates.Predicate) error
				once := false
				mockey.Mock((*catalog).commitRetention).Origin(&original).To(func(c *catalog, ctx context.Context, channel, epoch string, saves map[string]string, removes []string, guards ...predicates.Predicate) error {
					if c == first && !once {
						once = true
						switch interleave {
						case "pin-first":
							require.NoError(t, second.PinWALReplay(ctx, channel, pin))
						case "truncate-first":
							_, err := second.AuthorizeWALTruncation(ctx, channel, 7, cp)
							require.NoError(t, err)
						case "release-first":
							require.NoError(t, second.ReleaseWALReplay(ctx, channel, pin.PinId))
						case "takeover-first":
							newOwner := proto.CloneOf(cp)
							newOwner.Term++
							require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey(channel), checkpointBytesOf(t, newOwner)))
						}
					}
					return original(c, ctx, channel, epoch, saves, removes, guards...)
				}).Build()
				if interleave == "pin-first" || interleave == "takeover-first" {
					bound, err := first.AuthorizeWALTruncation(t.Context(), "p", 7, cp)
					require.Error(t, err)
					require.Nil(t, bound)
				} else {
					require.Error(t, first.PinWALReplay(t.Context(), "p", pin))
					require.Error(t, first.PinWALReplay(t.Context(), "p", pin))
				}
				require.True(t, once)
			})
		})
	}
}

func TestWALReplayPinGenerationIdentityAndBudget(t *testing.T) {
	c, _, store := retentionCatalogs(t)
	ctx := t.Context()
	cp := replayTestCheckpoint(100, 1000)
	require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, cp)))
	pin := &streamingpb.WALReplayPin{PinId: "bootstrap", SourceTerm: 7, Checkpoint: replayTestCheckpoint(10, 100)}
	require.NoError(t, c.PinWALReplay(ctx, "p", pin))
	changed := proto.CloneOf(pin)
	changed.Checkpoint.TimeTick++
	require.ErrorIs(t, c.PinWALReplay(ctx, "p", changed), merr.ErrDataIntegrity)
	require.NoError(t, c.ReleaseWALReplay(ctx, "p", pin.PinId))
	require.NoError(t, c.ReleaseWALReplay(ctx, "p", pin.PinId))
	require.Error(t, c.PinWALReplay(ctx, "p", pin))
	for i := range maxReplayPins {
		pin.PinId = fmt.Sprintf("pin-%d", i)
		require.NoError(t, c.PinWALReplay(ctx, "p", pin))
	}
	pin.PinId = "overflow"
	require.Error(t, c.PinWALReplay(ctx, "p", pin))
	cp.LogGeneration++
	cp.Term++
	cp.MessageId = walimplstest.NewTestMessageID(1).IntoProto()
	require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, cp)))
	_, err := c.AuthorizeWALTruncation(ctx, "p", 8, replayTestCheckpoint(1, 1000))
	require.Error(t, err, "never compare physical positions across log generations")
	bound, err := c.AuthorizeWALTruncation(ctx, "p", 8, cp)
	require.NoError(t, err)
	require.True(t, proto.Equal(cp.MessageId, bound.MessageId), "old-generation obligations do not truncate the new log")
	for _, bad := range []*streamingpb.WALReplayPin{nil, {}, {PinId: strings.Repeat("x", 257)}, {PinId: "no-position", SourceTerm: 8}} {
		require.Error(t, c.PinWALReplay(ctx, "p", bad))
	}
	require.Error(t, c.ReleaseWALReplay(ctx, "p", ""))
	_, err = c.AuthorizeWALTruncation(ctx, "p", 8, nil)
	require.Error(t, err)
}

func TestWALReplayRetentionRejectsCorruption(t *testing.T) {
	for _, failure := range []string{"state", "owner", "request-id", "watermark-id", "watermark-generation", "watermark-backend", "published-regressed", "pin-behind-watermark", "pin-id", "empty-pin", "pin-budget", "pin-encoding"} {
		t.Run(failure, func(t *testing.T) {
			c, _, store := retentionCatalogs(t)
			ctx := t.Context()
			cp := replayTestCheckpoint(100, 1000)
			require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, cp)))
			pin := &streamingpb.WALReplayPin{PinId: "r", SourceTerm: 7, Checkpoint: replayTestCheckpoint(20, 200)}
			state := &streamingpb.WALReplayRetentionState{Truncated: replayTestCheckpoint(10, 100), Pins: map[string]*streamingpb.WALReplayPin{}}
			switch failure {
			case "state":
				require.NoError(t, store.Save(ctx, replayStateKey("p"), "bad proto"))
			case "owner":
				require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), "bad proto"))
			case "request-id":
				cp.MessageId.Id = "invalid"
				pin.Checkpoint.MessageId.Id = "invalid"
			case "watermark-id":
				state.Truncated.MessageId.Id = "invalid"
			case "watermark-generation":
				state.Truncated.LogGeneration++
			case "watermark-backend":
				state.Truncated.MessageId.WALName = 0
			case "published-regressed":
				state.Truncated = replayTestCheckpoint(101, 1000)
			case "pin-behind-watermark":
				state.Pins["bad"] = &streamingpb.WALReplayPin{PinId: "bad", SourceTerm: 7, Checkpoint: replayTestCheckpoint(1, 1)}
			case "pin-id":
				bad := proto.CloneOf(pin)
				bad.Checkpoint.MessageId.Id = "invalid"
				state.Pins[bad.PinId] = bad
			case "empty-pin":
				state.Pins["bad"] = &streamingpb.WALReplayPin{}
			case "pin-budget":
				for i := range maxReplayPins + 1 {
					state.Pins[fmt.Sprint(i)] = proto.CloneOf(pin)
				}
			case "pin-encoding":
				pin.PinId = "invalid-\xff"
			}
			if failure != "state" {
				data, err := proto.Marshal(state)
				require.NoError(t, err)
				require.NoError(t, store.Save(ctx, replayStateKey("p"), string(data)))
			}
			if failure != "pin-behind-watermark" && failure != "pin-id" && failure != "empty-pin" {
				require.Error(t, c.PinWALReplay(ctx, "p", pin))
			}
			if failure != "pin-encoding" {
				bound, err := c.AuthorizeWALTruncation(ctx, "p", 7, cp)
				require.Error(t, err)
				require.Nil(t, bound)
			}
		})
	}
}

func TestWALReplayRetentionReadFailures(t *testing.T) {
	for _, failure := range []string{"epoch", "owner", "released", "state"} {
		t.Run(failure, func(t *testing.T) {
			mockey.PatchConvey("catalog uncertainty cannot grant replay or truncation", t, func() {
				c, _, store := retentionCatalogs(t)
				ctx := t.Context()
				cp := replayTestCheckpoint(100, 1000)
				require.NoError(t, store.Save(ctx, buildConsumeCheckpointKey("p"), checkpointBytesOf(t, cp)))
				expected := merr.WrapErrServiceUnavailable("catalog unavailable")
				key := map[string]string{"epoch": retentionPrefix("p") + "epoch", "owner": buildConsumeCheckpointKey("p"), "released": retentionKey("p", "replay-released", "r"), "state": replayStateKey("p")}[failure]
				var original func(context.Context, string) (string, error)
				mockey.Mock(mockey.GetMethod(store, "Load")).Origin(&original).To(func(ctx context.Context, actual string) (string, error) {
					if actual == key {
						return "", expected
					}
					return original(ctx, actual)
				}).Build()
				require.ErrorIs(t, c.PinWALReplay(ctx, "p", &streamingpb.WALReplayPin{PinId: "r", SourceTerm: 7, Checkpoint: cp}), expected)
				if failure != "released" {
					_, err := c.AuthorizeWALTruncation(ctx, "p", 7, cp)
					require.ErrorIs(t, err, expected)
				}
				if failure == "state" || failure == "epoch" {
					require.ErrorIs(t, c.ReleaseWALReplay(ctx, "p", "r"), expected)
				}
			})
		})
	}
}
