package util

import (
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

// Config is the Config of the client
type Config struct {
	ClusterPrefix        string
	WALName              string
	ETCDClient           *clientv3.Client
	RootPath             string
	DialTimeout          time.Duration
	ExtraGRPCDialOptions []grpc.DialOption
	GRPCRetryPolicy      map[string]interface{}
}

func (c *Config) Validate() error {
	if c.RootPath == "" {
		return errors.New("root path is required")
	}
	return nil
}
