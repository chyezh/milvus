package main

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/spf13/viper"

	"github.com/milvus-io/milvus/pkg/v2/mlog"
)

func ShowYaml(filepath string) {
	reader := viper.New()
	reader.SetConfigFile(filepath)
	if err := reader.ReadInConfig(); err != nil {
		mlog.Warn(context.TODO(), "read config failed", mlog.Err(err))
		os.Exit(-3)
	}
	keys := reader.AllKeys()
	sort.Strings(keys)
	for _, key := range keys {
		v := reader.GetString(key)
		fmt.Fprintln(os.Stdout, key, "=", v)
	}
}
