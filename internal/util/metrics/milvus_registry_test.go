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
	"google.golang.org/protobuf/proto"

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

func TestMilvusRegistryGather_InjectsResourceGroupLabel(t *testing.T) {
	t.Setenv("MILVUS_SERVER_LABEL_RESOURCE_GROUP", "rg-default")
	t.Setenv("MILVUS_SERVER_LABEL_QN_RESOURCE_GROUP", "rg-querynode")
	paramtable.SetRole(typeutil.QueryNodeRole)
	t.Cleanup(func() {
		paramtable.SetRole("")
	})

	reg := prometheus.NewRegistry()
	plainGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "milvus_registry_test_plain_metric",
		Help: "plain metric",
	})
	existingRGGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "milvus_registry_test_existing_rg_metric",
		Help: "metric with semantic rg label",
	}, []string{pkgmetrics.ResourceGroupLabelName})
	plainGauge.Set(1)
	existingRGGauge.WithLabelValues("semantic-rg").Set(2)
	reg.MustRegister(plainGauge, existingRGGauge)

	r := &MilvusRegistry{
		GoRegistry: reg,
		CRegistry:  nil,
	}
	res, err := r.Gather()
	require.NoError(t, err)

	assertMetricHasResourceGroupLabel(t, res, "milvus_registry_test_plain_metric", "rg-querynode")
	assertMetricHasResourceGroupLabel(t, res, "milvus_registry_test_existing_rg_metric", "semantic-rg")
	assertMetricResourceGroupLabelCount(t, res, "milvus_registry_test_existing_rg_metric", 1)
}

func TestMilvusRegistryGather_LeavesMetricsUnchangedWithoutResourceGroup(t *testing.T) {
	unsetEnv(t, "MILVUS_SERVER_LABEL_RESOURCE_GROUP")
	unsetEnv(t, "MILVUS_SERVER_LABEL_QN_RESOURCE_GROUP")
	paramtable.SetRole(typeutil.QueryNodeRole)
	t.Cleanup(func() {
		paramtable.SetRole("")
	})

	reg := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "milvus_registry_test_no_resource_group_metric",
		Help: "plain metric",
	})
	gauge.Set(1)
	reg.MustRegister(gauge)

	r := &MilvusRegistry{
		GoRegistry: reg,
		CRegistry:  nil,
	}
	res, err := r.Gather()
	require.NoError(t, err)

	assertMetricResourceGroupLabelCount(t, res, "milvus_registry_test_no_resource_group_metric", 0)
}

func TestInjectResourceGroupLabel_AddsLabelToMetricFamilies(t *testing.T) {
	metricType := dto.MetricType_GAUGE
	metricWithoutRG := &dto.MetricFamily{
		Name: proto.String("c_metric_without_rg"),
		Help: proto.String("c metric without rg"),
		Type: &metricType,
		Metric: []*dto.Metric{
			{
				Gauge: &dto.Gauge{Value: proto.Float64(1)},
			},
		},
	}
	metricFamilies := []*dto.MetricFamily{
		metricWithoutRG,
		{
			Name: proto.String("c_metric_with_rg"),
			Help: proto.String("c metric with rg"),
			Type: &metricType,
			Metric: []*dto.Metric{
				{
					Label: []*dto.LabelPair{
						{Name: proto.String(pkgmetrics.ResourceGroupLabelName), Value: proto.String("semantic-rg")},
					},
					Gauge: &dto.Gauge{Value: proto.Float64(2)},
				},
			},
		},
	}

	res := injectResourceGroupLabel(metricFamilies, "rg-process")

	assertMetricHasResourceGroupLabel(t, res, "c_metric_without_rg", "rg-process")
	assertMetricHasResourceGroupLabel(t, res, "c_metric_with_rg", "semantic-rg")
	assertMetricResourceGroupLabelCount(t, res, "c_metric_with_rg", 1)
	assertMetricResourceGroupLabelCount(t, []*dto.MetricFamily{metricWithoutRG}, "c_metric_without_rg", 0)
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
