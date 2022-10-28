/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package flowcontrol

import (
	"net/http"
	"sync"

	"k8s.io/apiserver/pkg/endpoints/request"
)

type KcpWatchTracker interface {
	GetInterestedWatchCount(cluster string, requestInfo *request.RequestInfo) int
	RegisterWatch(r *http.Request) ForgetWatchFunc
}

type kcpWatchTracker struct {
	lock      sync.RWMutex
	delegates map[string]WatchTracker
}

func NewKcpWatchTracker() KcpWatchTracker {
	return &kcpWatchTracker{
		delegates: make(map[string]WatchTracker),
	}
}

func (k *kcpWatchTracker) getOrCreateDelegate(clusterName string) WatchTracker {
	k.lock.RLock()
	delegate := k.delegates[clusterName]
	k.lock.RUnlock()

	if delegate != nil {
		return delegate
	}

	k.lock.Lock()
	defer k.lock.Unlock()

	delegate = k.delegates[clusterName]
	if delegate != nil {
		return delegate
	}

	delegate = NewWatchTracker()
	k.delegates[clusterName] = delegate
	return delegate
}

// GetInterestedWatchCount extracts the cluster name from requestInfo and calls
// cluster-specific WatchTracker
func (t *kcpWatchTracker) GetInterestedWatchCount(cluster string, requestInfo *request.RequestInfo) int {
	tracker := t.getOrCreateDelegate(cluster)
	// TODO(cyang49): tracker might be deleted before the count is fetched?
	return tracker.GetInterestedWatchCount(requestInfo)
}

// RegisterWatch implements WatchTracker
func (t *kcpWatchTracker) RegisterWatch(r *http.Request) ForgetWatchFunc {
	cluster, err := request.ValidClusterFrom(r.Context())
	if err != nil {
		return nil
	}
	tracker := t.getOrCreateDelegate(cluster.Name.String())
	// TODO(cyang49): tracker might be deleted before the count is fetched?
	return tracker.RegisterWatch(r)
}
