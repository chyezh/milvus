package meta

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type (
	Timestamp = typeutil.Timestamp
	UniqueID  = typeutil.UniqueID
)

//go:generate mockery --name=IMetaTable --structname=MockIMetaTable --output=./  --filename=mock_meta_table.go --with-expecter --inpackage
type IMetaTable interface {
	GetDatabaseByID(ctx context.Context, dbID int64, ts Timestamp) (*model.Database, error)
	GetDatabaseByName(ctx context.Context, dbName string, ts Timestamp) (*model.Database, error)
	CreateDatabase(ctx context.Context, db *model.Database, ts typeutil.Timestamp) error
	DropDatabase(ctx context.Context, dbName string, ts typeutil.Timestamp) error
	ListDatabases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Database, error)
	AlterDatabase(ctx context.Context, oldDB *model.Database, newDB *model.Database, ts typeutil.Timestamp) error

	AddCollection(ctx context.Context, coll *model.Collection) error
	DropCollection(ctx context.Context, collectionID UniqueID, ts Timestamp) error
	RemoveCollection(ctx context.Context, collectionID UniqueID, ts Timestamp) error
	// GetCollectionID retrieves the corresponding collectionID based on the collectionName.
	// If the collection does not exist, it will return InvalidCollectionID.
	// Please use the function with caution.
	GetCollectionID(ctx context.Context, dbName string, collectionName string) UniqueID
	GetCollectionByName(ctx context.Context, dbName string, collectionName string, ts Timestamp) (*model.Collection, error)
	GetCollectionByID(ctx context.Context, dbName string, collectionID UniqueID, ts Timestamp, allowUnavailable bool) (*model.Collection, error)
	GetCollectionByIDWithMaxTs(ctx context.Context, collectionID UniqueID) (*model.Collection, error)
	ListCollections(ctx context.Context, dbName string, ts Timestamp, onlyAvail bool) ([]*model.Collection, error)
	ListAllAvailCollections(ctx context.Context) map[int64][]int64
	// ListAllAvailPartitions returns the partition ids of all available collections.
	// The key of the map is the database id, and the value is a map of collection id to partition ids.
	ListAllAvailPartitions(ctx context.Context) map[int64]map[int64][]int64
	ListCollectionPhysicalChannels(ctx context.Context) map[typeutil.UniqueID][]string
	GetCollectionVirtualChannels(ctx context.Context, colID int64) []string
	GetPChannelInfo(ctx context.Context, pchannel string) *rootcoordpb.GetPChannelInfoResponse
	AddPartition(ctx context.Context, partition *model.Partition) error
	ChangePartitionState(ctx context.Context, collectionID UniqueID, partitionID UniqueID, state pb.PartitionState, ts Timestamp) error
	RemovePartition(ctx context.Context, dbID int64, collectionID UniqueID, partitionID UniqueID, ts Timestamp) error
	CreateAlias(ctx context.Context, dbName string, alias string, collectionName string, ts Timestamp) error
	DropAlias(ctx context.Context, dbName string, alias string, ts Timestamp) error
	AlterAlias(ctx context.Context, dbName string, alias string, collectionName string, ts Timestamp) error
	DescribeAlias(ctx context.Context, dbName string, alias string, ts Timestamp) (string, error)
	ListAliases(ctx context.Context, dbName string, collectionName string, ts Timestamp) ([]string, error)
	AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts Timestamp, fieldModify bool) error
	RenameCollection(ctx context.Context, dbName string, oldName string, newDBName string, newName string, ts Timestamp) error
	GetGeneralCount(ctx context.Context) int

	// TODO: it'll be a big cost if we handle the time travel logic, since we should always list all aliases in catalog.
	IsAlias(ctx context.Context, db, name string) bool
	ListAliasesByID(ctx context.Context, collID UniqueID) []string

	AddCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) error
	GetCredential(ctx context.Context, username string) (*internalpb.CredentialInfo, error)
	DeleteCredential(ctx context.Context, username string) error
	AlterCredential(ctx context.Context, credInfo *internalpb.CredentialInfo) error
	ListCredentialUsernames(ctx context.Context) (*milvuspb.ListCredUsersResponse, error)

	CreateRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity) error
	DropRole(ctx context.Context, tenant string, roleName string) error
	OperateUserRole(ctx context.Context, tenant string, userEntity *milvuspb.UserEntity, roleEntity *milvuspb.RoleEntity, operateType milvuspb.OperateUserRoleType) error
	SelectRole(ctx context.Context, tenant string, entity *milvuspb.RoleEntity, includeUserInfo bool) ([]*milvuspb.RoleResult, error)
	SelectUser(ctx context.Context, tenant string, entity *milvuspb.UserEntity, includeRoleInfo bool) ([]*milvuspb.UserResult, error)
	OperatePrivilege(ctx context.Context, tenant string, entity *milvuspb.GrantEntity, operateType milvuspb.OperatePrivilegeType) error
	SelectGrant(ctx context.Context, tenant string, entity *milvuspb.GrantEntity) ([]*milvuspb.GrantEntity, error)
	DropGrant(ctx context.Context, tenant string, role *milvuspb.RoleEntity) error
	ListPolicy(ctx context.Context, tenant string) ([]*milvuspb.GrantEntity, error)
	ListUserRole(ctx context.Context, tenant string) ([]string, error)
	BackupRBAC(ctx context.Context, tenant string) (*milvuspb.RBACMeta, error)
	RestoreRBAC(ctx context.Context, tenant string, meta *milvuspb.RBACMeta) error
	IsCustomPrivilegeGroup(ctx context.Context, groupName string) (bool, error)
	CreatePrivilegeGroup(ctx context.Context, groupName string) error
	DropPrivilegeGroup(ctx context.Context, groupName string) error
	ListPrivilegeGroups(ctx context.Context) ([]*milvuspb.PrivilegeGroupInfo, error)
	OperatePrivilegeGroup(ctx context.Context, groupName string, privileges []*milvuspb.PrivilegeEntity, operateType milvuspb.OperatePrivilegeGroupType) error
	GetPrivilegeGroupRoles(ctx context.Context, groupName string) ([]*milvuspb.RoleEntity, error)
}
