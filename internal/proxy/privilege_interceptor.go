package proxy

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/casbin/casbin/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v2/mlog"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

type PrivilegeFunc func(ctx context.Context, req interface{}) (context.Context, error)

var (
	enforcer                *casbin.SyncedEnforcer
	initOnce                sync.Once
	initPrivilegeGroupsOnce sync.Once
)

var roPrivileges, rwPrivileges, adminPrivileges map[string]struct{}

// UnaryServerInterceptor returns a new unary server interceptors that performs per-request privilege access.
func UnaryServerInterceptor(privilegeFunc PrivilegeFunc) grpc.UnaryServerInterceptor {
	privilege.InitPrivilegeGroups()
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		newCtx, err := privilegeFunc(ctx, req)
		if err != nil {
			return nil, err
		}
		return handler(newCtx, req)
	}
}

func PrivilegeInterceptor(ctx context.Context, req interface{}) (context.Context, error) {
	if !Params.CommonCfg.AuthorizationEnabled.GetAsBool() {
		return ctx, nil
	}
	mlog.RatedDebug(context.TODO(), mlog.RateDefault, "PrivilegeInterceptor", mlog.String("type", reflect.TypeOf(req).String()))
	privilegeExt, err := funcutil.GetPrivilegeExtObj(req)
	if err != nil {
		mlog.RatedInfo(context.TODO(), mlog.RateDefault, "GetPrivilegeExtObj err", mlog.Err(err))
		return ctx, nil
	}
	username, password, err := contextutil.GetAuthInfoFromContext(ctx)
	if err != nil {
		mlog.Warn(context.TODO(), "GetCurUserFromContext fail", mlog.Err(err))
		return ctx, err
	}
	if !Params.CommonCfg.RootShouldBindRole.GetAsBool() && username == util.UserRoot {
		return ctx, nil
	}
	roleNames, err := GetRole(username)
	if err != nil {
		mlog.Warn(context.TODO(), "GetRole fail", mlog.String("username", username), mlog.Err(err))
		return ctx, err
	}
	roleNames = append(roleNames, util.RolePublic)
	objectType := privilegeExt.ObjectType.String()
	objectNameIndex := privilegeExt.ObjectNameIndex
	objectName := funcutil.GetObjectName(req, objectNameIndex)
	if isCurUserObject(objectType, username, objectName) {
		return ctx, nil
	}

	if isSelectMyRoleGrants(req, roleNames) {
		return ctx, nil
	}

	objectNameIndexs := privilegeExt.ObjectNameIndexs
	objectNames := funcutil.GetObjectNames(req, objectNameIndexs)
	objectPrivilege := privilegeExt.ObjectPrivilege.String()
	dbName := GetCurDBNameFromContextOrDefault(ctx)

	e := privilege.GetEnforcer()
	for _, roleName := range roleNames {
		permitFunc := func(objectName string) (bool, error) {
			object := funcutil.PolicyForResource(dbName, objectType, objectName)
			isPermit, cached, version := privilege.GetResultCache(roleName, object, objectPrivilege)
			if cached {
				return isPermit, nil
			}
			isPermit, err := e.Enforce(roleName, object, objectPrivilege)
			if err != nil {
				return false, err
			}
			privilege.SetResultCache(roleName, object, objectPrivilege, isPermit, version)
			return isPermit, nil
		}

		if objectNameIndex != 0 {
			// handle the api which refers one resource
			permitObject, err := permitFunc(objectName)
			if err != nil {
				mlog.Warn(context.TODO(), "fail to execute permit func", mlog.String("name", objectName), mlog.Err(err))
				return ctx, err
			}
			if permitObject {
				return ctx, nil
			}
		}

		if objectNameIndexs != 0 {
			// handle the api which refers many resources
			permitObjects := true
			for _, name := range objectNames {
				p, err := permitFunc(name)
				if err != nil {
					mlog.Warn(context.TODO(), "fail to execute permit func", mlog.String("name", name), mlog.Err(err))
					return ctx, err
				}
				if !p {
					permitObjects = false
					break
				}
			}
			if permitObjects && len(objectNames) != 0 {
				return ctx, nil
			}
		}
	}

	mlog.Info(context.TODO(), "permission deny", mlog.Strings("roles", roleNames))

	if password == util.PasswordHolder {
		username = "apikey user"
	}

	return ctx, status.Error(codes.PermissionDenied,
		fmt.Sprintf("%s: permission deny to %s in the `%s` database", objectPrivilege, username, dbName))
}

// isCurUserObject Determine whether it is an Object of type User that operates on its own user information,
// like updating password or viewing your own role information.
// make users operate their own user information when the related privileges are not granted.
func isCurUserObject(objectType string, curUser string, object string) bool {
	if objectType != commonpb.ObjectType_User.String() {
		return false
	}
	return curUser == object
}

func isSelectMyRoleGrants(req interface{}, roleNames []string) bool {
	selectGrantReq, ok := req.(*milvuspb.SelectGrantRequest)
	if !ok {
		return false
	}
	filterGrantEntity := selectGrantReq.GetEntity()
	roleName := filterGrantEntity.GetRole().GetName()
	return funcutil.SliceContain(roleNames, roleName)
}
