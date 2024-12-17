// Code generated by mockery v2.46.0. DO NOT EDIT.

package mock_metastore

import (
	context "context"

	mock "github.com/stretchr/testify/mock"

	streamingpb "github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
)

// MockStreamingNodeCataLog is an autogenerated mock type for the StreamingNodeCataLog type
type MockStreamingNodeCataLog struct {
	mock.Mock
}

type MockStreamingNodeCataLog_Expecter struct {
	mock *mock.Mock
}

func (_m *MockStreamingNodeCataLog) EXPECT() *MockStreamingNodeCataLog_Expecter {
	return &MockStreamingNodeCataLog_Expecter{mock: &_m.Mock}
}

// GetConsumeCheckpoint provides a mock function with given fields: ctx, pChannelName
func (_m *MockStreamingNodeCataLog) GetConsumeCheckpoint(ctx context.Context, pChannelName string) (*streamingpb.WALCheckpoint, error) {
	ret := _m.Called(ctx, pChannelName)

	if len(ret) == 0 {
		panic("no return value specified for GetConsumeCheckpoint")
	}

	var r0 *streamingpb.WALCheckpoint
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) (*streamingpb.WALCheckpoint, error)); ok {
		return rf(ctx, pChannelName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) *streamingpb.WALCheckpoint); ok {
		r0 = rf(ctx, pChannelName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*streamingpb.WALCheckpoint)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, pChannelName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockStreamingNodeCataLog_GetConsumeCheckpoint_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetConsumeCheckpoint'
type MockStreamingNodeCataLog_GetConsumeCheckpoint_Call struct {
	*mock.Call
}

// GetConsumeCheckpoint is a helper method to define mock.On call
//   - ctx context.Context
//   - pChannelName string
func (_e *MockStreamingNodeCataLog_Expecter) GetConsumeCheckpoint(ctx interface{}, pChannelName interface{}) *MockStreamingNodeCataLog_GetConsumeCheckpoint_Call {
	return &MockStreamingNodeCataLog_GetConsumeCheckpoint_Call{Call: _e.mock.On("GetConsumeCheckpoint", ctx, pChannelName)}
}

func (_c *MockStreamingNodeCataLog_GetConsumeCheckpoint_Call) Run(run func(ctx context.Context, pChannelName string)) *MockStreamingNodeCataLog_GetConsumeCheckpoint_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string))
	})
	return _c
}

func (_c *MockStreamingNodeCataLog_GetConsumeCheckpoint_Call) Return(_a0 *streamingpb.WALCheckpoint, _a1 error) *MockStreamingNodeCataLog_GetConsumeCheckpoint_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockStreamingNodeCataLog_GetConsumeCheckpoint_Call) RunAndReturn(run func(context.Context, string) (*streamingpb.WALCheckpoint, error)) *MockStreamingNodeCataLog_GetConsumeCheckpoint_Call {
	_c.Call.Return(run)
	return _c
}

// ListSegmentAssignment provides a mock function with given fields: ctx, pChannelName
func (_m *MockStreamingNodeCataLog) ListSegmentAssignment(ctx context.Context, pChannelName string) ([]*streamingpb.SegmentAssignmentMeta, error) {
	ret := _m.Called(ctx, pChannelName)

	if len(ret) == 0 {
		panic("no return value specified for ListSegmentAssignment")
	}

	var r0 []*streamingpb.SegmentAssignmentMeta
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) ([]*streamingpb.SegmentAssignmentMeta, error)); ok {
		return rf(ctx, pChannelName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) []*streamingpb.SegmentAssignmentMeta); ok {
		r0 = rf(ctx, pChannelName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*streamingpb.SegmentAssignmentMeta)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, pChannelName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockStreamingNodeCataLog_ListSegmentAssignment_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListSegmentAssignment'
type MockStreamingNodeCataLog_ListSegmentAssignment_Call struct {
	*mock.Call
}

// ListSegmentAssignment is a helper method to define mock.On call
//   - ctx context.Context
//   - pChannelName string
func (_e *MockStreamingNodeCataLog_Expecter) ListSegmentAssignment(ctx interface{}, pChannelName interface{}) *MockStreamingNodeCataLog_ListSegmentAssignment_Call {
	return &MockStreamingNodeCataLog_ListSegmentAssignment_Call{Call: _e.mock.On("ListSegmentAssignment", ctx, pChannelName)}
}

func (_c *MockStreamingNodeCataLog_ListSegmentAssignment_Call) Run(run func(ctx context.Context, pChannelName string)) *MockStreamingNodeCataLog_ListSegmentAssignment_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string))
	})
	return _c
}

func (_c *MockStreamingNodeCataLog_ListSegmentAssignment_Call) Return(_a0 []*streamingpb.SegmentAssignmentMeta, _a1 error) *MockStreamingNodeCataLog_ListSegmentAssignment_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockStreamingNodeCataLog_ListSegmentAssignment_Call) RunAndReturn(run func(context.Context, string) ([]*streamingpb.SegmentAssignmentMeta, error)) *MockStreamingNodeCataLog_ListSegmentAssignment_Call {
	_c.Call.Return(run)
	return _c
}

// SaveConsumeCheckpoint provides a mock function with given fields: ctx, pChannelName, checkpoint
func (_m *MockStreamingNodeCataLog) SaveConsumeCheckpoint(ctx context.Context, pChannelName string, checkpoint *streamingpb.WALCheckpoint) error {
	ret := _m.Called(ctx, pChannelName, checkpoint)

	if len(ret) == 0 {
		panic("no return value specified for SaveConsumeCheckpoint")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, *streamingpb.WALCheckpoint) error); ok {
		r0 = rf(ctx, pChannelName, checkpoint)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockStreamingNodeCataLog_SaveConsumeCheckpoint_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SaveConsumeCheckpoint'
type MockStreamingNodeCataLog_SaveConsumeCheckpoint_Call struct {
	*mock.Call
}

// SaveConsumeCheckpoint is a helper method to define mock.On call
//   - ctx context.Context
//   - pChannelName string
//   - checkpoint *streamingpb.WALCheckpoint
func (_e *MockStreamingNodeCataLog_Expecter) SaveConsumeCheckpoint(ctx interface{}, pChannelName interface{}, checkpoint interface{}) *MockStreamingNodeCataLog_SaveConsumeCheckpoint_Call {
	return &MockStreamingNodeCataLog_SaveConsumeCheckpoint_Call{Call: _e.mock.On("SaveConsumeCheckpoint", ctx, pChannelName, checkpoint)}
}

func (_c *MockStreamingNodeCataLog_SaveConsumeCheckpoint_Call) Run(run func(ctx context.Context, pChannelName string, checkpoint *streamingpb.WALCheckpoint)) *MockStreamingNodeCataLog_SaveConsumeCheckpoint_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].(*streamingpb.WALCheckpoint))
	})
	return _c
}

func (_c *MockStreamingNodeCataLog_SaveConsumeCheckpoint_Call) Return(_a0 error) *MockStreamingNodeCataLog_SaveConsumeCheckpoint_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockStreamingNodeCataLog_SaveConsumeCheckpoint_Call) RunAndReturn(run func(context.Context, string, *streamingpb.WALCheckpoint) error) *MockStreamingNodeCataLog_SaveConsumeCheckpoint_Call {
	_c.Call.Return(run)
	return _c
}

// SaveSegmentAssignments provides a mock function with given fields: ctx, pChannelName, infos
func (_m *MockStreamingNodeCataLog) SaveSegmentAssignments(ctx context.Context, pChannelName string, infos []*streamingpb.SegmentAssignmentMeta) error {
	ret := _m.Called(ctx, pChannelName, infos)

	if len(ret) == 0 {
		panic("no return value specified for SaveSegmentAssignments")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, []*streamingpb.SegmentAssignmentMeta) error); ok {
		r0 = rf(ctx, pChannelName, infos)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockStreamingNodeCataLog_SaveSegmentAssignments_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'SaveSegmentAssignments'
type MockStreamingNodeCataLog_SaveSegmentAssignments_Call struct {
	*mock.Call
}

// SaveSegmentAssignments is a helper method to define mock.On call
//   - ctx context.Context
//   - pChannelName string
//   - infos []*streamingpb.SegmentAssignmentMeta
func (_e *MockStreamingNodeCataLog_Expecter) SaveSegmentAssignments(ctx interface{}, pChannelName interface{}, infos interface{}) *MockStreamingNodeCataLog_SaveSegmentAssignments_Call {
	return &MockStreamingNodeCataLog_SaveSegmentAssignments_Call{Call: _e.mock.On("SaveSegmentAssignments", ctx, pChannelName, infos)}
}

func (_c *MockStreamingNodeCataLog_SaveSegmentAssignments_Call) Run(run func(ctx context.Context, pChannelName string, infos []*streamingpb.SegmentAssignmentMeta)) *MockStreamingNodeCataLog_SaveSegmentAssignments_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string), args[2].([]*streamingpb.SegmentAssignmentMeta))
	})
	return _c
}

func (_c *MockStreamingNodeCataLog_SaveSegmentAssignments_Call) Return(_a0 error) *MockStreamingNodeCataLog_SaveSegmentAssignments_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockStreamingNodeCataLog_SaveSegmentAssignments_Call) RunAndReturn(run func(context.Context, string, []*streamingpb.SegmentAssignmentMeta) error) *MockStreamingNodeCataLog_SaveSegmentAssignments_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockStreamingNodeCataLog creates a new instance of MockStreamingNodeCataLog. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockStreamingNodeCataLog(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockStreamingNodeCataLog {
	mock := &MockStreamingNodeCataLog{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
