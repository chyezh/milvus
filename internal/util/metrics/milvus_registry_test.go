/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package metrics

import (
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgmetrics "github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMilvusRegistryGather_NilGoRegistry(t *testing.T) {
	r := &MilvusRegistry{
		GoRegistry: nil,
		CRegistry:  nil,
	}
	res, err := r.Gather()
	assert.NoError(t, err)
	assert.Nil(t, res)
}

func TestMilvusRegistryGather_NilCRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	r := &MilvusRegistry{
		GoRegistry: reg,
		CRegistry:  nil,
	}
	res, err := r.Gather()
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Greater(t, len(res), 0)
}

func TestMilvusRegistryRegisterer_InjectsResourceGroupConstLabel(t *testing.T) {
	t.Setenv("MILVUS_SERVER_LABEL_RESOURCE_GROUP", "rg-default")
	t.Setenv("MILVUS_SERVER_LABEL_QN_RESOURCE_GROUP", "rg-querynode")
	paramtable.SetRole(typeutil.QueryNodeRole)
	t.Cleanup(func() {
		paramtable.SetRole("")
	})

	r := NewMilvusRegistry()
	r.CRegistry = nil
	r.InitResourceGroupRegisterer(paramtable.GetRole())

	plainGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "milvus_registry_test_const_label_metric",
		Help: "plain metric",
	})
	plainGauge.Set(1)
	r.Registerer().MustRegister(plainGauge)
	res, err := r.Gather()
	require.NoError(t, err)

	assertMetricHasResourceGroupLabel(t, res, "milvus_registry_test_const_label_metric", "rg-querynode")
}

func TestMilvusRegistryRegisterer_UsesRawRegistryWithoutResourceGroup(t *testing.T) {
	unsetEnv(t, "MILVUS_SERVER_LABEL_RESOURCE_GROUP")
	unsetEnv(t, "MILVUS_SERVER_LABEL_QN_RESOURCE_GROUP")
	paramtable.SetRole(typeutil.QueryNodeRole)
	t.Cleanup(func() {
		paramtable.SetRole("")
	})

	r := NewMilvusRegistry()
	r.CRegistry = nil
	r.InitResourceGroupRegisterer(paramtable.GetRole())

	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "milvus_registry_test_no_resource_group_metric",
		Help: "plain metric",
	})
	gauge.Set(1)
	r.Registerer().MustRegister(gauge)
	res, err := r.Gather()
	require.NoError(t, err)

	assertMetricResourceGroupLabelCount(t, res, "milvus_registry_test_no_resource_group_metric", 0)
}

func TestMilvusRegistryGather_DoesNotInjectResourceGroupLabel(t *testing.T) {
	t.Setenv("MILVUS_SERVER_LABEL_RESOURCE_GROUP", "rg-default")
	paramtable.SetRole(typeutil.QueryNodeRole)
	t.Cleanup(func() {
		paramtable.SetRole("")
	})

	r := NewMilvusRegistry()
	r.CRegistry = nil
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "milvus_registry_test_raw_metric",
		Help: "plain metric",
	})
	gauge.Set(1)
	r.GoRegistry.MustRegister(gauge)

	res, err := r.Gather()
	require.NoError(t, err)

	assertMetricResourceGroupLabelCount(t, res, "milvus_registry_test_raw_metric", 0)
}

func TestMilvusRegistryRegisterBaseCollectors_UsesResourceGroupRegisterer(t *testing.T) {
	t.Setenv("MILVUS_SERVER_LABEL_RESOURCE_GROUP", "rg-default")
	paramtable.SetRole(typeutil.ProxyRole)
	t.Cleanup(func() {
		paramtable.SetRole("")
	})

	r := NewMilvusRegistry()
	r.CRegistry = nil
	r.InitResourceGroupRegisterer(paramtable.GetRole())
	r.RegisterBaseCollectors()

	res, err := r.Gather()
	require.NoError(t, err)

	for _, mf := range res {
		if mf.GetName() == "go_goroutines" {
			assertMetricHasResourceGroupLabel(t, res, "go_goroutines", "rg-default")
			return
		}
	}
	t.Fatal("go_goroutines metric not found")
}

func assertMetricHasResourceGroupLabel(t *testing.T, metricFamilies []*dto.MetricFamily, metricName string, expected string) {
	t.Helper()

	metric := findMetric(t, metricFamilies, metricName)
	for _, label := range metric.GetLabel() {
		if label.GetName() == pkgmetrics.ResourceGroupLabelName {
			assert.Equal(t, expected, label.GetValue())
			return
		}
	}
	t.Fatalf("metric %s does not have %s label", metricName, pkgmetrics.ResourceGroupLabelName)
}

func assertMetricResourceGroupLabelCount(t *testing.T, metricFamilies []*dto.MetricFamily, metricName string, expected int) {
	t.Helper()

	metric := findMetric(t, metricFamilies, metricName)
	count := 0
	for _, label := range metric.GetLabel() {
		if label.GetName() == pkgmetrics.ResourceGroupLabelName {
			count++
		}
	}
	assert.Equal(t, expected, count)
}

func findMetric(t *testing.T, metricFamilies []*dto.MetricFamily, metricName string) *dto.Metric {
	t.Helper()

	for _, mf := range metricFamilies {
		if mf.GetName() != metricName {
			continue
		}
		require.NotEmpty(t, mf.GetMetric())
		return mf.GetMetric()[0]
	}
	t.Fatalf("metric %s not found", metricName)
	return nil
}

func unsetEnv(t *testing.T, key string) {
	t.Helper()

	oldValue, ok := os.LookupEnv(key)
	require.NoError(t, os.Unsetenv(key))
	t.Cleanup(func() {
		if ok {
			require.NoError(t, os.Setenv(key, oldValue))
			return
		}
		require.NoError(t, os.Unsetenv(key))
	})
}
