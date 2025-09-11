package rootcoord

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *DDLCallback) registerRBACCallbacks() {
	registry.RegisterPutUserV2AckCallback(c.putUserV2AckCallback)
	registry.RegisterDropUserV2AckCallback(c.dropUserV2AckCallback)
	registry.RegisterPutRoleV2AckCallback(c.putRoleV2AckCallback)
	registry.RegisterDropRoleV2AckCallback(c.dropRoleV2AckCallback)
	registry.RegisterPutUserRoleV2AckCallback(c.putUserRoleV2AckCallback)
	registry.RegisterDropUserRoleV2AckCallback(c.dropUserRoleV2AckCallback)
	registry.RegisterGrantPrivilegeV2AckCallback(c.grantPrivilegeV2AckCallback)
	registry.RegisterRevokePrivilegeV2AckCallback(c.revokePrivilegeV2AckCallback)
	registry.RegisterPutPrivilegeGroupV2AckCallback(c.putPrivilegeGroupV2AckCallback)
	registry.RegisterDropPrivilegeGroupV2AckCallback(c.dropPrivilegeGroupV2AckCallback)
}

// putUserV2AckCallback is the ack callback function for the PutUserMessageV2 message.
func (c *DDLCallback) putUserV2AckCallback(ctx context.Context, result message.BroadcastResultPutUserMessageV2) error {
	// insert to db
	if err := c.meta.AlterCredential(ctx, result); err != nil {
		return err
	}
	// update proxy's local cache
	if err := c.UpdateCredCache(ctx, result.Message.MustBody().CredentialInfo); err != nil {
		return err
	}
	return nil
}

// dropUserV2AckCallback is the ack callback function for the DeleteCredential message
func (c *DDLCallback) dropUserV2AckCallback(ctx context.Context, result message.BroadcastResultDropUserMessageV2) error {
	// There should always be only one message in the msgs slice.
	if err := c.meta.DeleteCredential(ctx, result); err != nil {
		return err
	}
	if err := c.ExpireCredCache(ctx, result.Message.Header().UserName); err != nil {
		return err
	}
	if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
		OpType: int32(typeutil.CacheDeleteUser),
		OpKey:  result.Message.Header().UserName,
	}); err != nil {
		return err
	}
	return nil
}

// putRoleV2AckCallback is the ack callback function for the PutRoleMessageV2 message.
func (c *DDLCallback) putRoleV2AckCallback(ctx context.Context, result message.BroadcastResultPutRoleMessageV2) error {
	// There should always be only one message in the msgs slice.
	return c.meta.CreateRole(ctx, util.DefaultTenant, result.Message.Header().RoleEntity)
}

// dropRoleV2AckCallback is the ack callback function for the DropRoleMessageV2 message.
func (c *DDLCallback) dropRoleV2AckCallback(ctx context.Context, result message.BroadcastResultDropRoleMessageV2) error {
	// There should always be only one message in the msgs slice.
	msg := result.Message
	err := c.meta.DropRole(ctx, util.DefaultTenant, msg.Header().RoleName)
	if err != nil {
		log.Ctx(ctx).Warn("drop role mata data failed", zap.String("role_name", msg.Header().RoleName), zap.Error(err))
		return err
	}
	if err := c.meta.DropGrant(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: msg.Header().RoleName}); err != nil {
		log.Ctx(ctx).Warn("drop the privilege list failed for the role", zap.String("role_name", msg.Header().RoleName), zap.Error(err))
		return err
	}
	if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
		OpType: int32(typeutil.CacheDropRole),
		OpKey:  msg.Header().RoleName,
	}); err != nil {
		log.Ctx(ctx).Warn("delete user role cache failed for the role", zap.String("role_name", msg.Header().RoleName), zap.Error(err))
		return err
	}
	return nil
}

func (c *DDLCallback) putUserRoleV2AckCallback(ctx context.Context, result message.BroadcastResultPutUserRoleMessageV2) error {
	username := result.Message.Header().RoleBinding.UserEntity.Name
	roleName := result.Message.Header().RoleBinding.RoleEntity.Name
	return executeOperateUserRoleTaskSteps(ctx, c.Core, username, roleName, milvuspb.OperateUserRoleType_AddUserToRole)
}

func (c *DDLCallback) dropUserRoleV2AckCallback(ctx context.Context, result message.BroadcastResultDropUserRoleMessageV2) error {
	username := result.Message.Header().RoleBinding.UserEntity.Name
	roleName := result.Message.Header().RoleBinding.RoleEntity.Name
	return executeOperateUserRoleTaskSteps(ctx, c.Core, username, roleName, milvuspb.OperateUserRoleType_RemoveUserFromRole)
}

func (c *DDLCallback) grantPrivilegeV2AckCallback(ctx context.Context, result message.BroadcastResultGrantPrivilegeMessageV2) error {
	return executeOperatePrivilegeTaskSteps(ctx, c.Core, result.Message.Header().Entity, milvuspb.OperatePrivilegeType_Grant)
}

func (c *DDLCallback) revokePrivilegeV2AckCallback(ctx context.Context, result message.BroadcastResultRevokePrivilegeMessageV2) error {
	return executeOperatePrivilegeTaskSteps(ctx, c.Core, result.Message.Header().Entity, milvuspb.OperatePrivilegeType_Revoke)
}

func (c *DDLCallback) putPrivilegeGroupV2AckCallback(ctx context.Context, result message.BroadcastResultPutPrivilegeGroupMessageV2) error {
	if len(result.Message.Header().PrivilegeGroupInfo.Privileges) == 0 {
		return c.meta.CreatePrivilegeGroup(ctx, result.Message.Header().PrivilegeGroupInfo.GroupName)
	}
	return executeOperatePrivilegeGroupTaskSteps(ctx, c.Core, result.Message.Header().PrivilegeGroupInfo, milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup)
}

func (c *DDLCallback) dropPrivilegeGroupV2AckCallback(ctx context.Context, result message.BroadcastResultDropPrivilegeGroupMessageV2) error {
	if len(result.Message.Header().PrivilegeGroupInfo.Privileges) == 0 {
		return c.meta.DropPrivilegeGroup(ctx, result.Message.Header().PrivilegeGroupInfo.GroupName)
	}
	return executeOperatePrivilegeGroupTaskSteps(ctx, c.Core, result.Message.Header().PrivilegeGroupInfo, milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup)
}
