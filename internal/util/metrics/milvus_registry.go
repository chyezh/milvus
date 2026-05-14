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
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	pkgmetrics "github.com/milvus-io/milvus/pkg/v3/metrics"
)

func NewMilvusRegistry() *MilvusRegistry {
	r := &MilvusRegistry{
		GoRegistry:   prometheus.NewRegistry(),
		CRegistry:    NewCRegistry(),
		goRegisterer: nil,
	}
	r.goRegisterer = r.GoRegistry
	return r
}

// re-write the implementation of Gather()
type MilvusRegistry struct {
	GoRegistry *prometheus.Registry
	CRegistry  *CRegistry

	goRegisterer          prometheus.Registerer
	registerBaseCollector sync.Once
}

func (r *MilvusRegistry) InitResourceGroupRegisterer(role string) {
	if r.GoRegistry == nil {
		r.goRegisterer = nil
		return
	}

	resourceGroup := sessionutil.GetResourceGroupNameFromEnv(role)
	if resourceGroup == "" {
		r.goRegisterer = r.GoRegistry
		return
	}
	r.goRegisterer = prometheus.WrapRegistererWith(
		prometheus.Labels{pkgmetrics.ResourceGroupLabelName: resourceGroup},
		r.GoRegistry,
	)
}

func (r *MilvusRegistry) Registerer() prometheus.Registerer {
	if r.goRegisterer == nil {
		return r.GoRegistry
	}
	return r.goRegisterer
}

func (r *MilvusRegistry) RegisterBaseCollectors() {
	r.registerBaseCollector.Do(func() {
		r.Registerer().MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
		r.Registerer().MustRegister(prometheus.NewGoCollector())
	})
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
	if r.CRegistry == nil {
		return resGo, nil
	}
	resC, err := r.CRegistry.Gather()
	if err != nil {
		// if gather c metrics fail, ignore the error and return go metrics
		return resGo, nil
	}
	return append(resGo, resC...), nil
}
