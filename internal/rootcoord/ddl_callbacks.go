package rootcoord

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"go.uber.org/zap"
)

// registerBroadcastDDLCallbacks registers the broadcast DDL callbacks for the RBAC.
func (c *Core) registerBroadcastDDLCallbacks() {
	// RBAC
	registry.RegisterPutUserV2CheckCallback(c.putUserV2CheckCallback)
	registry.RegisterPutUserV2AckCallback(c.putUserV2AckCallback)
	registry.RegisterDropUserV2CheckCallback(c.dropUserMessageV2CheckCallback)
	registry.RegisterDropUserV2AckCallback(c.dropUserMessageV2AckCallback)
	registry.RegisterPutRoleV2CheckCallback(c.putRoleV2CheckCallback)
	registry.RegisterPutRoleV2AckCallback(c.putRoleV2AckCallback)
	registry.RegisterDropRoleV2CheckCallback(c.dropRoleV2CheckCallback)
	registry.RegisterDropRoleV2AckCallback(c.dropRoleV2AckCallback)
}

// putUserV2CheckCallback is the check callback function for the PutUserMessageV2 message.
func (c *Core) putUserV2CheckCallback(ctx context.Context, msg message.BroadcastPutUserMessageV2) error {
	if msg.Header().UserEntity.Name == "" {
		return errors.New("username is empty")
	}

	if msg.Header().IsUpdate {
		// update operation, no need to check if the user exists and limits.
		// TODO: It's wired that there's no check for user existence.
		return nil
	}

	// check if the number of users has reached the limit.
	resp, err := c.meta.ListCredentialUsernames(ctx)
	if err != nil {
		return err
	}
	if len(resp.Usernames) >= Params.ProxyCfg.MaxUserNum.GetAsInt() {
		errMsg := "unable to add user because the number of users has reached the limit"
		log.Ctx(ctx).Error(errMsg, zap.Int("max_user_num", Params.ProxyCfg.MaxUserNum.GetAsInt()))
		return errors.New(errMsg)
	}

	// check if the username already exists.
	for _, username := range resp.Usernames {
		if username == msg.Header().UserEntity.Name {
			return fmt.Errorf("user already exists: %s", msg.Header().UserEntity.Name)
		}
	}
	return nil
}

// putUserV2AckCallback is the ack callback function for the PutUserMessageV2 message.
func (c *Core) putUserV2AckCallback(ctx context.Context, msgs ...message.ImmutablePutUserMessageV2) error {
	msg := msgs[0]
	// There should always be only one message in the msgs slice.
	body := msg.MustBody()
	// insert to db
	if err := c.meta.AlterCredential(ctx, msg); err != nil {
		return err
	}
	// update proxy's local cache
	if err := c.UpdateCredCache(ctx, body.CredentialInfo); err != nil {
		return err
	}
	return nil
}

// dropUserMessageV2CheckCallback is the check callback function for the DeleteCredential message.
func (c *Core) dropUserMessageV2CheckCallback(ctx context.Context, msg message.BroadcastDropUserMessageV2) error {
	if msg.Header().UserName == "" {
		return errors.New("username is empty")
	}
	return nil
}

// dropUserMessageV2AckCallback is the ack callback function for the DeleteCredential message.
func (c *Core) dropUserMessageV2AckCallback(ctx context.Context, msgs ...message.ImmutableDropUserMessageV2) error {
	// There should always be only one message in the msgs slice.
	msg := msgs[0]
	if err := c.meta.DeleteCredential(ctx, msg); err != nil {
		return err
	}
	if err := c.ExpireCredCache(ctx, msg.Header().UserName); err != nil {
		return err
	}
	if err := c.proxyClientManager.RefreshPolicyInfoCache(ctx, &proxypb.RefreshPolicyInfoCacheRequest{
		OpType: int32(typeutil.CacheDeleteUser),
		OpKey:  msg.Header().UserName,
	}); err != nil {
		return err
	}
	return nil
}

// putRoleV2CheckCallback is the check callback function for the PutRoleMessageV2 message.
func (c *Core) putRoleV2CheckCallback(ctx context.Context, msg message.BroadcastPutRoleMessageV2) error {
	if msg.Header().RoleEntity.Name == "" {
		return errors.New("role name is empty")
	}

	results, err := c.meta.SelectRole(ctx, util.DefaultTenant, nil, false)
	if err != nil {
		log.Ctx(ctx).Warn("fail to list roles", zap.Error(err))
		return err
	}
	if len(results) >= Params.ProxyCfg.MaxRoleNum.GetAsInt() {
		errMsg := "unable to create role because the number of roles has reached the limit"
		log.Ctx(ctx).Warn(errMsg, zap.Int("max_role_num", Params.ProxyCfg.MaxRoleNum.GetAsInt()))
		return errors.New(errMsg)
	}
	for _, result := range results {
		if result.GetRole().GetName() == msg.Header().RoleEntity.Name {
			log.Ctx(ctx).Info("role already exists", zap.String("role", msg.Header().RoleEntity.Name))
			return common.NewIgnorableError(errors.Newf("role [%s] already exists", msg.Header().RoleEntity.Name))
		}
	}
	return nil
}

// putRoleV2AckCallback is the ack callback function for the PutRoleMessageV2 message.
func (c *Core) putRoleV2AckCallback(ctx context.Context, msgs ...message.ImmutablePutRoleMessageV2) error {
	// There should always be only one message in the msgs slice.
	return c.meta.CreateRole(ctx, util.DefaultTenant, msgs[0].Header().RoleEntity)
}

// dropRoleV2CheckCallback is the check callback function for the DropRoleMessageV2 message.
func (c *Core) dropRoleV2CheckCallback(ctx context.Context, msg message.BroadcastDropRoleMessageV2) error {
	if util.IsBuiltinRole(msg.Header().RoleName) {
		return merr.WrapErrPrivilegeNotPermitted("the role[%s] is a builtin role, which can't be dropped", msg.Header().RoleName)
	}

	if msg.Header().RoleName == "" {
		return errors.New("role name is empty")
	}

	if _, err := c.meta.SelectRole(ctx, util.DefaultTenant, &milvuspb.RoleEntity{Name: msg.Header().RoleName}, false); err != nil {
		errMsg := "not found the role, maybe the role isn't existed or internal system error"
		return errors.New(errMsg)
	}
	if msg.Header().ForceDrop {
		return nil
	}

	grantEntities, err := c.meta.SelectGrant(ctx, util.DefaultTenant, &milvuspb.GrantEntity{
		Role:   &milvuspb.RoleEntity{Name: msg.Header().RoleName},
		DbName: "*",
	})
	if err != nil {
		return err
	}
	if len(grantEntities) != 0 {
		errMsg := "fail to drop the role that it has privileges. Use REVOKE API to revoke privileges"
		return errors.New(errMsg)
	}
	return nil
}

// dropRoleV2AckCallback is the ack callback function for the DropRoleMessageV2 message.
func (c *Core) dropRoleV2AckCallback(ctx context.Context, msgs ...message.ImmutableDropRoleMessageV2) error {
	// There should always be only one message in the msgs slice.
	msg := msgs[0]
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
