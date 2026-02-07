package helper

import (
	"context"
	"flag"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/mlog"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
)

var (
	addr                = flag.String("addr", "http://localhost:19530", "server host and port")
	user                = flag.String("user", "root", "user")
	password            = flag.String("password", "Milvus", "password")
	logLevel            = flag.String("log.level", "info", "log level for test")
	teiEndpoint         = flag.String("tei_endpoint", "http://text-embeddings-service.milvus-ci.svc.cluster.local:80", "TEI service endpoint for text embedding tests")
	teiRerankerEndpoint = flag.String("tei_reranker_uri", "http://text-rerank-service.milvus-ci.svc.cluster.local:80", "TEI reranker service endpoint")
	teiModelDim         = flag.Int("tei_model_dim", 768, "Vector dimension for text embedding model")
	defaultClientConfig *client.ClientConfig
)

func setDefaultClientConfig(cfg *client.ClientConfig) {
	defaultClientConfig = cfg
}

func GetDefaultClientConfig() *client.ClientConfig {
	newCfg := *defaultClientConfig
	dialOptions := newCfg.DialOptions
	newDialOptions := make([]grpc.DialOption, len(dialOptions))
	copy(newDialOptions, dialOptions)
	newCfg.DialOptions = newDialOptions
	return &newCfg
}

func GetAddr() string {
	return *addr
}

func GetUser() string {
	return *user
}

func GetPassword() string {
	return *password
}

func GetTEIEndpoint() string {
	return *teiEndpoint
}

func GetTEIRerankerEndpoint() string {
	return *teiRerankerEndpoint
}

func GetTEIModelDim() int {
	return *teiModelDim
}

func parseLogConfig() {
	mlog.Info(context.TODO(), "Parser Log Level", mlog.String("logLevel", *logLevel))
	switch *logLevel {
	case "debug", "DEBUG", "Debug":
		mlog.SetLevel(zap.DebugLevel)
	case "info", "INFO", "Info":
		mlog.SetLevel(zap.InfoLevel)
	case "warn", "WARN", "Warn":
		mlog.SetLevel(zap.WarnLevel)
	case "error", "ERROR", "Error":
		mlog.SetLevel(zap.ErrorLevel)
	default:
		mlog.SetLevel(zap.InfoLevel)
	}
}

func setup() {
	mlog.Info(context.TODO(), "Start to setup all......")
	flag.Parse()
	parseLogConfig()
	mlog.Info(context.TODO(), "Parser Milvus address", mlog.String("address", *addr))

	// set default milvus client config
	setDefaultClientConfig(&client.ClientConfig{Address: *addr})
}

// Teardown teardown
func teardown() {
	mlog.Info(context.TODO(), "Start to tear down all.....")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*common.DefaultTimeout)
	defer cancel()
	mc, err := base.NewMilvusClient(ctx, &client.ClientConfig{Address: GetAddr(), Username: GetUser(), Password: GetPassword()})
	if err != nil {
		mlog.Error(context.TODO(), "teardown failed to connect milvus with error", mlog.Err(err))
	}
	defer mc.Close(ctx)

	// clear dbs
	dbs, _ := mc.ListDatabase(ctx, client.NewListDatabaseOption())
	for _, db := range dbs {
		if db != common.DefaultDb {
			_ = mc.UseDatabase(ctx, client.NewUseDatabaseOption(db))
			collections, _ := mc.ListCollections(ctx, client.NewListCollectionOption())
			for _, coll := range collections {
				_ = mc.DropCollection(ctx, client.NewDropCollectionOption(coll))
			}
			_ = mc.DropDatabase(ctx, client.NewDropDatabaseOption(db))
		}
	}
}

func RunTests(m *testing.M) int {
	setup()
	code := m.Run()
	if code != 0 {
		mlog.Error(context.TODO(), "Tests failed and exited", mlog.Int("code", code))
	}
	teardown()
	return code
}
