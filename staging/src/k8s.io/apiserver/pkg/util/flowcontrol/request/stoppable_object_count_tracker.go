package request

import (
	"context"
	"sync"
)

// stoppableStorageObjectCountTracker is a cluster scoped
// storage object count tracker
type stoppableStorageObjectCountTracker struct {
	tracker StorageObjectCountTracker
	// stopCh is a cluster specific stop channel that
	// will be used to broadcast stop signals to all
	// observers of this tracker
	stopCh    chan struct{}
	lock      sync.RWMutex
	observers map[string]*storageObjectCountObserver
}

var _ StorageObjectCountTracker = &stoppableStorageObjectCountTracker{}

// newStoppableStorageObjectCountTracker creates a k8s StorageObjectCountTracker
// note that it also starts a goroutine for forwarding upstream stop signal
func newStoppableStorageObjectCountTracker(stopSignal <-chan struct{}) *stoppableStorageObjectCountTracker {
	stopCh := make(chan struct{})
	// forward upstream stop signal to tracker stop channel
	go func() {
		select {
		case <-stopSignal:
			close(stopCh)
		case <-stopCh:
		}
	}()

	return &stoppableStorageObjectCountTracker{
		tracker:   NewStorageObjectCountTracker(stopCh),
		observers: map[string]*storageObjectCountObserver{},
		stopCh:    stopCh,
	}
}

// Get function exposes the resource object count stored in the tracker
func (t *stoppableStorageObjectCountTracker) Get(resource string) (int64, error) {
	// TODO: should this function block till the object count is set at least once?
	return t.tracker.Get(resource)
}

// Set function grants write access to the resource object count
func (t *stoppableStorageObjectCountTracker) Set(resource string, count int64) {
	t.tracker.Set(resource, count)
}

// Stop function stops the tracker and also all observers
func (t *stoppableStorageObjectCountTracker) Stop() {
	close(t.stopCh)
}

// StartObserving starts an observer for a specific resource type
// getterFunc is supplied by the caller for accessing the storage layer
func (t *stoppableStorageObjectCountTracker) StartObserving(ctx context.Context, resource string, getterFunc func() int64) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if _, ok := t.observers[resource]; ok {
		return
	}

	observer := newStoppableStorageObjectCountObserver(
		getterFunc,
		func(count int64) { t.Set(resource, count) },
	)

	// Use a goroutine to monitor stop signals from different sources
	stopCh := observer.start() // This stopCh will be closed if observer.stop() is called
	go func() {
		select {
		case <-ctx.Done():
			t.StopObserving(resource)
		case <-t.stopCh:
			t.StopObserving(resource)
		case <-stopCh:
		}
	}()

	t.observers[resource] = observer
}

// StopObserving stops the observer of the specified resource type
func (t *stoppableStorageObjectCountTracker) StopObserving(resource string) {
	t.lock.Lock()
	defer t.lock.Unlock()

	observer, ok := t.observers[resource]
	if !ok {
		return
	}

	observer.stop()
	delete(t.observers, resource)
}
