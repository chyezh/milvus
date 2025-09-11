package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util"
)

func (c *Core) broadcastOperatePrivilege(ctx context.Context, in *milvuspb.OperatePrivilegeRequest) error {
	if err := c.operatePrivilegeCommonCheck(ctx, in); err != nil {
		return err
	}

	privName := in.Entity.Grantor.Privilege.Name
	switch in.Version {
	case "v2":
		if err := c.isValidPrivilegeV2(ctx, privName); err != nil {
			return err
		}
		if err := c.validatePrivilegeGroupParams(ctx, privName, in.Entity.DbName, in.Entity.ObjectName); err != nil {
			return err
		}
		// set up object type for metastore, to be compatible with v1 version
		in.Entity.Object.Name = util.GetObjectType(privName)
	default:
		if err := c.isValidPrivilege(ctx, privName, in.Entity.Object.Name); err != nil {
			return err
		}
		// set up object name if it is global object type and not built in privilege group
		if in.Entity.Object.Name == commonpb.ObjectType_Global.String() && !util.IsBuiltinPrivilegeGroup(in.Entity.Grantor.Privilege.Name) {
			in.Entity.ObjectName = util.AnyWord
		}
	}
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusivePrivilegeResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	var msg message.BroadcastMutableMessage
	switch in.Type {
	case milvuspb.OperatePrivilegeType_Grant:
		msg = message.NewGrantPrivilegeMessageBuilderV2().
			WithHeader(&message.GrantPrivilegeMessageHeader{
				Entity: in.Entity,
			}).
			WithBody(&message.GrantPrivilegeMessageBody{}).
			WithBroadcast([]string{streaming.WAL().ControlChannel()}).
			MustBuildBroadcast()
	case milvuspb.OperatePrivilegeType_Revoke:
		msg = message.NewRevokePrivilegeMessageBuilderV2().
			WithHeader(&message.RevokePrivilegeMessageHeader{
				Entity: in.Entity,
			}).
			WithBody(&message.RevokePrivilegeMessageBody{}).
			WithBroadcast([]string{streaming.WAL().ControlChannel()}).
			MustBuildBroadcast()
	default:
		return errors.New("invalid operate privilege type")
	}

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) grantPrivilegeV2AckCallback(ctx context.Context, result message.BroadcastResultGrantPrivilegeMessageV2) error {
	return executeOperatePrivilegeTaskSteps(ctx, c.Core, result.Message.Header().Entity, milvuspb.OperatePrivilegeType_Grant)
}

func (c *DDLCallback) revokePrivilegeV2AckCallback(ctx context.Context, result message.BroadcastResultRevokePrivilegeMessageV2) error {
	return executeOperatePrivilegeTaskSteps(ctx, c.Core, result.Message.Header().Entity, milvuspb.OperatePrivilegeType_Revoke)
}

func (c *Core) broadcastCreatePrivilegeGroup(ctx context.Context, in *milvuspb.CreatePrivilegeGroupRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusivePrivilegeResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	msg := message.NewPutPrivilegeGroupMessageBuilderV2().
		WithHeader(&message.PutPrivilegeGroupMessageHeader{
			PrivilegeGroupInfo: &milvuspb.PrivilegeGroupInfo{
				GroupName: in.GroupName,
			},
		}).
		WithBody(&message.PutPrivilegeGroupMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *Core) broadcastOperatePrivilegeGroup(ctx context.Context, in *milvuspb.OperatePrivilegeGroupRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusivePrivilegeResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	var msg message.BroadcastMutableMessage
	switch in.Type {
	case milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup:
		msg = message.NewPutPrivilegeGroupMessageBuilderV2().
			WithHeader(&message.PutPrivilegeGroupMessageHeader{
				PrivilegeGroupInfo: &milvuspb.PrivilegeGroupInfo{
					GroupName:  in.GroupName,
					Privileges: in.Privileges,
				},
			}).
			WithBody(&message.PutPrivilegeGroupMessageBody{}).
			WithBroadcast([]string{streaming.WAL().ControlChannel()}).
			MustBuildBroadcast()
	case milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup:
		msg = message.NewDropPrivilegeGroupMessageBuilderV2().
			WithHeader(&message.DropPrivilegeGroupMessageHeader{
				PrivilegeGroupInfo: &milvuspb.PrivilegeGroupInfo{
					GroupName:  in.GroupName,
					Privileges: in.Privileges,
				},
			}).
			WithBody(&message.DropPrivilegeGroupMessageBody{}).
			WithBroadcast([]string{streaming.WAL().ControlChannel()}).
			MustBuildBroadcast()
	default:
		return errors.New("invalid operate privilege group type")
	}

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) putPrivilegeGroupV2AckCallback(ctx context.Context, result message.BroadcastResultPutPrivilegeGroupMessageV2) error {
	if len(result.Message.Header().PrivilegeGroupInfo.Privileges) == 0 {
		return c.meta.CreatePrivilegeGroup(ctx, result.Message.Header().PrivilegeGroupInfo.GroupName)
	}
	return executeOperatePrivilegeGroupTaskSteps(ctx, c.Core, result.Message.Header().PrivilegeGroupInfo, milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup)
}

func (c *Core) broadcastDropPrivilegeGroup(ctx context.Context, in *milvuspb.DropPrivilegeGroupRequest) error {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusivePrivilegeResourceKey())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	msg := message.NewDropPrivilegeGroupMessageBuilderV2().
		WithHeader(&message.DropPrivilegeGroupMessageHeader{
			PrivilegeGroupInfo: &milvuspb.PrivilegeGroupInfo{
				GroupName: in.GroupName,
			},
		}).
		WithBody(&message.DropPrivilegeGroupMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}).
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (c *DDLCallback) dropPrivilegeGroupV2AckCallback(ctx context.Context, result message.BroadcastResultDropPrivilegeGroupMessageV2) error {
	if len(result.Message.Header().PrivilegeGroupInfo.Privileges) == 0 {
		return c.meta.DropPrivilegeGroup(ctx, result.Message.Header().PrivilegeGroupInfo.GroupName)
	}
	return executeOperatePrivilegeGroupTaskSteps(ctx, c.Core, result.Message.Header().PrivilegeGroupInfo, milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup)
}
