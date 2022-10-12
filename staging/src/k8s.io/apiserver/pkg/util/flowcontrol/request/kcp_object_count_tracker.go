package request

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

// KcpStorageObjectCountTracker is an interface for the operations
// on the tracker. The underlying implementation is cluster aware and
// maintains a dynamic list of cluster specific trackers (reusing k8s trackers)
type KcpStorageObjectCountTracker interface {
	// GetObjectCount returns the count of objects for a given cluster and object type
	// It is used by work estimator in APF
	GetObjectCount(cluster string, key string) (int64, error)
	// SetObjectCount sets the count of objects for a given cluster and object type
	// It is used by observer threads managed by KCP SOCT controller
	SetObjectCount(cluster string, key string, count int64)

	// CreateTracker is used by SOCT controller to create tracker of a new cluster
	CreateTracker(cluster string)
	// DeleteTracker is used by SOCT controller to delete tracker when a cluster is deleted
	DeleteTracker(cluster string)

	StartObserving(cluster string, key string, getterFunc func() int64)
	StopObserving(cluster string, key string)
}

type kcpStorageObjectCountTracker struct {
	lock sync.RWMutex
	// trackers maps from a cluster name string to a k8s storage object count tracker
	// suppose there are N logical clusters, there should be N trackers
	trackers map[string]StorageObjectCountTracker
	// There is one stop channel that will stop all trackers
	stopCh <-chan struct{}
	// Suppose there are M resource types per logical cluster, there should be
	// N*M observers and thus N*M stop channels
	observerStops map[string]chan struct{}
}

var _ KcpStorageObjectCountTracker = &kcpStorageObjectCountTracker{}

func NewKcpObjectCountTracker(stopCh <-chan struct{}) *kcpStorageObjectCountTracker {
	return &kcpStorageObjectCountTracker{
		lock:          sync.RWMutex{},
		trackers:      map[string]StorageObjectCountTracker{},
		stopCh:        stopCh,
		observerStops: map[string]chan struct{}{},
	}
}

// GetObjectCount looks for the cluster name in an http request to get the
// object count of a specific object type in a specific cluster
func (c *kcpStorageObjectCountTracker) GetObjectCount(cluster string, key string) (int64, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	tracker, ok := c.trackers[cluster]
	if !ok {
		return -1, fmt.Errorf("tracker does not exist for cluster %s", cluster)
	}
	return tracker.Get(key)
}

// SetObjectCount implements KcpObjectCountTracker
func (c *kcpStorageObjectCountTracker) SetObjectCount(cluster string, key string, count int64) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	tracker, ok := c.trackers[cluster]
	if !ok {
		// TODO: should return error here?
		return
	}
	tracker.Set(key, count)
}

// CreateTracker implements KcpObjectCountTracker
func (c *kcpStorageObjectCountTracker) CreateTracker(cluster string) {
	if _, ok := c.trackers[cluster]; ok {
		// This should not happen
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	// this is for when tracker is removed while acquiring lock
	if _, ok := c.trackers[cluster]; ok {
		return
	}

	c.trackers[cluster] = NewStorageObjectCountTracker(c.stopCh)
}

// DeleteTracker implements KcpObjectCountTracker
func (c *kcpStorageObjectCountTracker) DeleteTracker(cluster string) {
	// TODO: Need to figure out stopCh logic

	if _, ok := c.trackers[cluster]; !ok {
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.trackers[cluster]; !ok {
		return
	}
	delete(c.trackers, cluster)
}

const observePeriod = 5 * time.Second

type storageObjectCountObserver struct {
	getterFunc func() int64
	setterFunc func(int64)
}

func newStoppableObserver(getterFunc func() int64, setterFunc func(int64)) *storageObjectCountObserver {
	return &storageObjectCountObserver{
		getterFunc: getterFunc,
		setterFunc: setterFunc,
	}
}

func (o *storageObjectCountObserver) start(stop <-chan struct{}) {
	go wait.Until(func() { o.setterFunc(o.getterFunc()) }, observePeriod, stop)
}

func (c *kcpStorageObjectCountTracker) StartObserving(cluster string, key string, getterFunc func() int64) {
	if _, ok := c.trackers[cluster]; !ok {
		panic("tracker is accessed before it's created")
	}

	observer := newStoppableObserver(
		getterFunc,
		func(count int64) { c.SetObjectCount(cluster, key, count) },
	)
	stop := make(chan struct{})
	observer.start(stop)

	// forward tracker stop signal to observer stop channel
	go func() {
		for {
			select {
			case <-c.stopCh: // forwards signal and return
				close(stop)
				return
			case <-stop: // no need to forward
				return
			}
		}
	}()

	// TODO: use a different lock?
	c.lock.Lock()
	defer c.lock.Unlock()
	c.observerStops[cluster+":"+key] = stop // TODO: this is a hack because the clust:key string may not be unique. Should fix
}

func (c *kcpStorageObjectCountTracker) StopObserving(cluster string, key string) {
	// TODO: use a different lock?
	c.lock.Lock()
	defer c.lock.Unlock()
	stop, ok := c.observerStops[cluster+":"+key]
	if !ok {
		return
	}
	close(stop)
	delete(c.observerStops, cluster+":"+key)
}
