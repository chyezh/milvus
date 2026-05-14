/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	pkgmetrics "github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func NewMilvusRegistry() *MilvusRegistry {
	r := &MilvusRegistry{
		GoRegistry: prometheus.NewRegistry(),
		CRegistry:  NewCRegistry(),
	}

	r.GoRegistry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	r.GoRegistry.MustRegister(prometheus.NewGoCollector())

	return r
}

// re-write the implementation of Gather()
type MilvusRegistry struct {
	GoRegistry *prometheus.Registry
	CRegistry  *CRegistry
}

// Gather implements Gatherer.
func (r *MilvusRegistry) Gather() ([]*dto.MetricFamily, error) {
	if r.GoRegistry == nil {
		return nil, nil
	}
	resGo, err := r.GoRegistry.Gather()
	if err != nil {
		return nil, err
	}
	resourceGroup := resourceGroupLabelValue()
	if r.CRegistry == nil {
		return injectResourceGroupLabel(resGo, resourceGroup), nil
	}
	resC, err := r.CRegistry.Gather()
	if err != nil {
		// if gather c metrics fail, ignore the error and return go metrics
		return injectResourceGroupLabel(resGo, resourceGroup), nil
	}
	return injectResourceGroupLabel(append(resGo, resC...), resourceGroup), nil
}

func resourceGroupLabelValue() string {
	return sessionutil.GetResourceGroupNameFromEnv(paramtable.GetRole())
}

func injectResourceGroupLabel(metricFamilies []*dto.MetricFamily, resourceGroup string) []*dto.MetricFamily {
	if resourceGroup == "" {
		return metricFamilies
	}

	for i, mf := range metricFamilies {
		if mf == nil {
			continue
		}

		var cloned *dto.MetricFamily
		for j, metric := range mf.GetMetric() {
			if metric == nil || hasResourceGroupLabel(metric) {
				continue
			}
			if cloned == nil {
				cloned = proto.Clone(mf).(*dto.MetricFamily)
			}
			cloned.Metric[j].Label = append(cloned.Metric[j].Label, &dto.LabelPair{
				Name:  proto.String(pkgmetrics.ResourceGroupLabelName),
				Value: proto.String(resourceGroup),
			})
		}

		if cloned != nil {
			metricFamilies[i] = cloned
		}
	}
	return metricFamilies
}

func hasResourceGroupLabel(metric *dto.Metric) bool {
	for _, label := range metric.GetLabel() {
		if label.GetName() == pkgmetrics.ResourceGroupLabelName {
			return true
		}
	}
	return false
}
