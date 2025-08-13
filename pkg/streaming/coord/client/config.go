package client

import "github.com/milvus-io/milvus/pkg/v2/util/paramtable"

// OptClientRootPath is the option to set the root path of the client.
func OptClientRootPath(rootPath string) ClientOption {
	return configOptionFunc(func(c *config) {
		c.RootPath = rootPath
	})
}

// ClientOption is the option of the client.
type ClientOption interface {
	apply(c *config)
}

type configOptionFunc func(c *config)

func (f configOptionFunc) apply(c *config) {
	f(c)
}

// config is the config of the client
type config struct {
	RootPath string
}

// newConfig create a new config with the given options
func newConfig(opts ...ClientOption) *config {
	cfg := &config{
		RootPath: paramtable.Get().EtcdCfg.MetaRootPath.GetValue(), // default to the meta root path
	}
	for _, opt := range opts {
		opt.apply(cfg)
	}
	return cfg
}
