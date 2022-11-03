package request

import (
	"context"
	"sync"
)

// clusterSpecificStorageObjectCountTracker is a cluster scoped
// storage object count tracker
type clusterSpecificStorageObjectCountTracker struct {
	tracker StorageObjectCountTracker
	// csStopCh is a cluster specific stop channel that
	// will be used to broadcast stop signals to all
	// observers of this tracker
	csStopCh   <-chan struct{}
	cancelFunc context.CancelFunc

	lock      sync.RWMutex
	observers map[string]*storageObjectCountObserver
}

var _ StorageObjectCountTracker = &clusterSpecificStorageObjectCountTracker{}

// newClusterSpecificStorageObjectCountTracker creates a k8s StorageObjectCountTracker
// note that NewStorageObjectCountTracker requires a stopCh because it starts a prune goroutine
func newClusterSpecificStorageObjectCountTracker(csStopCh <-chan struct{}, cancelFunc context.CancelFunc) *clusterSpecificStorageObjectCountTracker {
	return &clusterSpecificStorageObjectCountTracker{
		tracker:    NewStorageObjectCountTracker(csStopCh),
		observers:  map[string]*storageObjectCountObserver{},
		csStopCh:   csStopCh,
		cancelFunc: cancelFunc,
	}
}

// Get exposes the resource object count stored in the tracker
func (t *clusterSpecificStorageObjectCountTracker) Get(resource string) (int64, error) {
	// TODO(cyang49): should this function block till the object count is set at least once?
	return t.tracker.Get(resource)
}

// Set grants write access to the resource object count
func (t *clusterSpecificStorageObjectCountTracker) Set(resource string, count int64) {
	t.tracker.Set(resource, count)
}

// Stop stops the tracker and also all observers
func (t *clusterSpecificStorageObjectCountTracker) Stop() {
	t.cancelFunc()
}

// EnsureObserving starts an observer of a specific resource type
// getterFunc is supplied by the caller for accessing the storage layer
func (t *clusterSpecificStorageObjectCountTracker) EnsureObserving(apiChangedCtx context.Context, resource string, getterFunc func() int64) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.observers[resource]; ok {
		return
	}

	observer := newStorageObjectCountObserver(
		getterFunc,
		func(count int64) { t.Set(resource, count) },
	)

	// Use a goroutine to monitor and join stop signals from different sources
	obsStopCh := make(chan struct{})
	observer.start(obsStopCh)
	go func() {
		select {
		case <-apiChangedCtx.Done():
		case <-t.csStopCh:
		}
		close(obsStopCh)
		t.removeObserver(resource)
	}()

	t.observers[resource] = observer
}

// removeObserver stops the observer of the specified resource type
func (t *clusterSpecificStorageObjectCountTracker) removeObserver(resource string) {
	t.lock.Lock()
	defer t.lock.Unlock()

	_, ok := t.observers[resource]
	if !ok {
		return
	}
	delete(t.observers, resource)
}
