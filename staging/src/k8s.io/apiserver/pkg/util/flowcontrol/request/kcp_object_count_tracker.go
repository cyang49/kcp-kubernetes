package request

import (
	"context"
	"fmt"
	"sync"

	"k8s.io/klog/v2"
)

// KcpStorageObjectCountTracker is an interface for the operations
// on the tracker. The underlying implementation is cluster aware and
// maintains a dynamic list of cluster specific trackers (reusing k8s trackers)
type KcpStorageObjectCountTracker interface {
	// GetObjectCount returns the count of objects for a given cluster and object type
	// It is used by work estimator in APF
	GetObjectCount(cluster string, resource string) (int64, error)
	// SetObjectCount sets the count of objects for a given cluster and object type
	// It is used by observer threads managed by KCP SOCT controller
	// SetObjectCount(cluster string, resource string, count int64)

	// CreateTracker is used by SOCT controller to create tracker of a new cluster
	CreateTracker(ctx context.Context, cluster string)
	// DeleteTracker is used by SOCT controller to delete tracker when a cluster is deleted
	DeleteTracker(cluster string)

	StartObserving(cluster string, key string, getterFunc func() int64)
	StopObserving(cluster string, key string)
}

type kcpStorageObjectCountTracker struct {
	lock sync.RWMutex
	// trackers maps from a cluster name string to a k8s storage object count tracker
	// suppose there are N logical clusters, there should be N trackers
	trackers map[string]*stoppableStorageObjectCountTracker

	// This stop channel that will stop all trackers across all logical clusters
	globalStopCh <-chan struct{}
}

var _ KcpStorageObjectCountTracker = &kcpStorageObjectCountTracker{}

func NewKcpObjectCountTracker(globalStopCh <-chan struct{}) *kcpStorageObjectCountTracker {
	return &kcpStorageObjectCountTracker{
		lock:         sync.RWMutex{},
		trackers:     map[string]*stoppableStorageObjectCountTracker{},
		globalStopCh: globalStopCh,
	}
}

// GetObjectCount looks for the cluster name in an http request to get the
// object count of a specific object type in a specific cluster
func (c *kcpStorageObjectCountTracker) GetObjectCount(cluster string, resource string) (int64, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	tracker, ok := c.trackers[cluster]
	if !ok {
		return -1, fmt.Errorf("tracker does not exist for cluster %s", cluster)
	}
	return tracker.Get(resource)
}

// CreateTracker implements KcpObjectCountTracker
func (c *kcpStorageObjectCountTracker) CreateTracker(ctx context.Context, cluster string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.trackers[cluster]; ok {
		return // should return error?
	}

	// create cluster specific stopCh that will be closed either when the call context is canceled
	// or when the global stop signal is received
	stopCh := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			close(stopCh)
		case <-c.globalStopCh:
			close(stopCh)
		case <-stopCh: // this is needed (?) for a cluster specific stop caused by a DeleteTracker call
			return
		}
	}()

	c.trackers[cluster] = newStoppableStorageObjectCountTracker(stopCh)
}

// DeleteTracker delete the tracker of a specific cluster
func (c *kcpStorageObjectCountTracker) DeleteTracker(cluster string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	t, ok := c.trackers[cluster]
	if !ok {
		return // should return error?
	}

	// Send stop signal to the cluster specific tracker
	t.Stop()
	delete(c.trackers, cluster)
}

func (c *kcpStorageObjectCountTracker) StartObserving(cluster string, resource string, getterFunc func() int64) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	tracker, ok := c.trackers[cluster]
	if !ok {
		return // should return error?
	}
	klog.Infof("StartObserving %s - %s", cluster, resource)
	tracker.StartObserving(resource, getterFunc)
}

func (c *kcpStorageObjectCountTracker) StopObserving(cluster string, resource string) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	tracker, ok := c.trackers[cluster]
	if !ok {
		return // should return error?
	}
	tracker.StopObserving(resource)
}
