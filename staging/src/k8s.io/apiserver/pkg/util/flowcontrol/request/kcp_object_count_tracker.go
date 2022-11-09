package request

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/klog/v2"
)

// KcpStorageObjectCountTracker is cluster aware and
// maintains a dynamic list of cluster specific trackers (reusing k8s trackers)
type KcpStorageObjectCountTracker interface {
	// GetObjectCount returns the count of objects for a given cluster and object type
	// It is used by work estimator in APF
	GetObjectCount(cluster string, resource string) (int64, error)

	// CreateClusterSpecificTracker is used by SOCT controller to create tracker of a new cluster
	CreateClusterSpecificTracker(ctx context.Context, cluster string)
	// DeleteClusterSpecificTracker is used by SOCT controller to delete tracker when a cluster is deleted
	DeleteClusterSpecificTracker(cluster string)

	EnsureObserving(apiChangedCtx context.Context, cluster string, key string, getterFunc func() int64)
	// StopObserving(cluster string, key string)
}

type kcpStorageObjectCountTracker struct {
	lock sync.RWMutex
	// csTrackers maps from a cluster name string to a k8s storage object count tracker
	csTrackers map[string]*clusterSpecificStorageObjectCountTracker

	// This stop channel that will stop all trackers across all logical clusters
	globalStopCh <-chan struct{}
}

var _ KcpStorageObjectCountTracker = &kcpStorageObjectCountTracker{}

func NewKcpStorageObjectCountTracker(globalStopCh <-chan struct{}) *kcpStorageObjectCountTracker {
	return &kcpStorageObjectCountTracker{
		lock:         sync.RWMutex{},
		csTrackers:   map[string]*clusterSpecificStorageObjectCountTracker{},
		globalStopCh: globalStopCh,
	}
}

// GetObjectCount gets the object count of a specific resource object type in a specific cluster
func (c *kcpStorageObjectCountTracker) GetObjectCount(cluster string, resource string) (int64, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	tracker, ok := c.csTrackers[cluster]
	if !ok {
		return -1, fmt.Errorf("tracker does not exist for cluster %s", cluster)
	}
	return tracker.Get(resource)
}

// CreateTracker creates a cluster specific storage object count tracker
// A goroutine is created to monitor incoming stop signals from different sources.
func (c *kcpStorageObjectCountTracker) CreateClusterSpecificTracker(ctx context.Context, cluster string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.csTrackers[cluster]; ok {
		return // should return error?
	}

	// Create a cluster-specific cancellable context
	csCtx, cancelFunc := context.WithCancel(ctx)
	go func() {
		select {
		case <-c.globalStopCh:
			cancelFunc()
		case <-csCtx.Done(): // for a cluster specific stop
		}
	}()

	c.csTrackers[cluster] = newClusterSpecificStorageObjectCountTracker(csCtx.Done(), cancelFunc)
	klog.Infof("StorageObjectCountTracker created: cluster %s", cluster)
}

// DeleteTracker delete the tracker of a specific cluster
func (c *kcpStorageObjectCountTracker) DeleteClusterSpecificTracker(cluster string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	t, ok := c.csTrackers[cluster]
	if !ok {
		return // should return error?
	}

	// Send stop signal to the cluster specific tracker
	t.Stop()
	delete(c.csTrackers, cluster)
	klog.Infof("StorageObjectCountTracker deleted: cluster %s", cluster)
}

// StartObserving starts an observer for a specific resource object type on a specific cluster
func (c *kcpStorageObjectCountTracker) EnsureObserving(apiChangedCtx context.Context, cluster string, resource string, getterFunc func() int64) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	t, ok := c.csTrackers[cluster]
	if !ok {
		return // should return error?
	}
	klog.Infof("EnsureObserving %s - %s", cluster, resource)
	t.EnsureObserving(apiChangedCtx, resource, getterFunc)
}
