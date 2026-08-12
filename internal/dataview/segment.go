// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataview

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type SegmentStore interface {
	GetSegment(ctx context.Context, segID int64) *Segment
	GetSegments(ctx context.Context, segIDs []int64) []*Segment
	SaveSealedAtDataVersion(ctx context.Context, segmentID int64, version *viewpb.DataVersion) error
	ListAllSegmentsForVersionAllocation(ctx context.Context, collectionID int64) []*Segment
}

type Segment struct {
	ID                          int64
	CollectionID                int64
	PartitionID                 int64
	NumOfRows                   int64
	MemSize                     int64
	State                       commonpb.SegmentState
	Level                       datapb.SegmentLevel
	IsImporting                 bool
	IsInvisible                 bool
	StartPosition               *msgpb.MsgPosition
	CommitTimestamp             uint64
	TransformStartAfterTimetick uint64
	SealedAtDataVersion         *viewpb.DataVersion
}

func (s *Segment) GetID() int64 {
	if s == nil {
		return 0
	}
	return s.ID
}

func (s *Segment) GetCollectionID() int64 {
	if s == nil {
		return 0
	}
	return s.CollectionID
}

func (s *Segment) GetPartitionID() int64 {
	if s == nil {
		return 0
	}
	return s.PartitionID
}

func (s *Segment) GetNumOfRows() int64 {
	if s == nil {
		return 0
	}
	return s.NumOfRows
}

func (s *Segment) GetMemSize() int64 {
	if s == nil {
		return 0
	}
	return s.MemSize
}

func (s *Segment) GetState() commonpb.SegmentState {
	if s == nil {
		return commonpb.SegmentState_SegmentStateNone
	}
	return s.State
}

func (s *Segment) GetLevel() datapb.SegmentLevel {
	if s == nil {
		return datapb.SegmentLevel_Legacy
	}
	return s.Level
}

func (s *Segment) GetIsImporting() bool {
	return s != nil && s.IsImporting
}

func (s *Segment) GetIsInvisible() bool {
	return s != nil && s.IsInvisible
}

func (s *Segment) GetStartPosition() *msgpb.MsgPosition {
	if s == nil {
		return nil
	}
	return s.StartPosition
}

func (s *Segment) GetCommitTimestamp() uint64 {
	if s == nil {
		return 0
	}
	return s.CommitTimestamp
}

func (s *Segment) GetTransformStartAfterTimetick() uint64 {
	if s == nil {
		return 0
	}
	return s.TransformStartAfterTimetick
}

func (s *Segment) GetSealedAtDataVersion() *viewpb.DataVersion {
	if s == nil {
		return nil
	}
	return s.SealedAtDataVersion
}
