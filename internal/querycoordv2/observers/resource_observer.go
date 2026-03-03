// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package observers

import (
	"context"
	"sync"
	"time"


	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/v2/mlog"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// ResourceObserver is used to observe resource group status.
// Recover resource group into expected configuration.
type ResourceObserver struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
	meta   *meta.Meta

	startOnce sync.Once
	stopOnce  sync.Once
}

func NewResourceObserver(meta *meta.Meta) *ResourceObserver {
	return &ResourceObserver{
		meta: meta,
	}
}

func (ob *ResourceObserver) Start() {
	ob.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		ob.cancel = cancel

		ob.wg.Add(1)
		go ob.schedule(ctx)
	})
}

func (ob *ResourceObserver) Stop() {
	ob.stopOnce.Do(func() {
		if ob.cancel != nil {
			ob.cancel()
		}
		ob.wg.Wait()
	})
}

func (ob *ResourceObserver) schedule(ctx context.Context) {
	defer ob.wg.Done()
	mlog.Info(context.TODO(), "Start check resource group loop")

	listener := ob.meta.ResourceManager.ListenResourceGroupChanged(ctx)
	for {
		ob.waitRGChangedOrTimeout(ctx, listener)
		// stop if the context is canceled.
		if ctx.Err() != nil {
			mlog.Info(context.TODO(), "Close resource group observer")
			return
		}

		// do check once.
		ob.checkAndRecoverResourceGroup(ctx)
	}
}

func (ob *ResourceObserver) waitRGChangedOrTimeout(ctx context.Context, listener *syncutil.VersionedListener) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, params.Params.QueryCoordCfg.CheckResourceGroupInterval.GetAsDuration(time.Second))
	defer cancel()
	listener.Wait(ctxWithTimeout)
}

func (ob *ResourceObserver) checkAndRecoverResourceGroup(ctx context.Context) {
	manager := ob.meta.ResourceManager
	rgNames := manager.ListResourceGroups(ctx)
	enableRGAutoRecover := params.Params.QueryCoordCfg.EnableRGAutoRecover.GetAsBool()
	mlog.Debug(context.TODO(), "start to check resource group", mlog.Bool("enableRGAutoRecover", enableRGAutoRecover), mlog.Int("resourceGroupNum", len(rgNames)))

	// Check if there is any incoming node.
	if manager.CheckIncomingNodeNum(ctx) > 0 {
		mlog.Info(context.TODO(), "new incoming node is ready to be assigned...", mlog.Int("incomingNodeNum", manager.CheckIncomingNodeNum(ctx)))
		manager.AssignPendingIncomingNode(ctx)
	}

	mlog.Debug(context.TODO(), "recover resource groups...")
	// Recover all resource group into expected configuration.
	for _, rgName := range rgNames {
		if err := manager.MeetRequirement(ctx, rgName); err != nil {
			mlog.Info(context.TODO(), "found resource group need to be recovered",
				mlog.String("rgName", rgName),
				mlog.String("reason", err.Error()),
			)

			if enableRGAutoRecover {
				err := manager.AutoRecoverResourceGroup(ctx, rgName)
				if err != nil {
					mlog.Warn(context.TODO(), "failed to recover resource group",
						mlog.String("rgName", rgName),
						mlog.Err(err),
					)
				}
			}
		}
	}
	mlog.Debug(context.TODO(), "check resource group done", mlog.Bool("enableRGAutoRecover", enableRGAutoRecover), mlog.Int("resourceGroupNum", len(rgNames)))
}
