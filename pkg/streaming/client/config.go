package client

import (
	"github.com/cockroachdb/errors"
)

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

type config struct {
	RootPath string
}

func (cfg *config) Validate() error {
	if cfg.RootPath == "" {
		return errors.New("root path is empty")
	}
	return nil
}

// newConfig create a new config with the given options
func newConfig(opts ...ClientOption) (*config, error) {
	cfg := &config{}
	for _, opt := range opts {
		opt.apply(cfg)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}
