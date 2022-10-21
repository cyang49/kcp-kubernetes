package request

// stoppableStorageObjectCountTracker is a cluster scoped
// storage object count tracker
type stoppableStorageObjectCountTracker struct {
	tracker StorageObjectCountTracker
	// stopCh is a cluster local stop channel that
	// will be used to broadcast stop signals to all
	// observers of this tracker
	stopCh    chan struct{}
	observers map[string]*storageObjectCountObserver
}

var _ StorageObjectCountTracker = &stoppableStorageObjectCountTracker{}

// !!FIXME!!: Operations are not thread safe
func newStoppableStorageObjectCountTracker(stopSignal <-chan struct{}) *stoppableStorageObjectCountTracker {
	stopCh := make(chan struct{})

	// forward upstream stop signal to tracker stop channel
	go func() {
		select {
		case <-stopSignal: // forwards signal and return
			close(stopCh)
		case <-stopCh: // no need to forward
		}
	}()

	return &stoppableStorageObjectCountTracker{
		tracker:   NewStorageObjectCountTracker(stopCh),
		observers: map[string]*storageObjectCountObserver{},
		stopCh:    stopCh,
	}
}

// Get implements StorageObjectCountTracker
func (t *stoppableStorageObjectCountTracker) Get(resource string) (int64, error) {
	return t.tracker.Get(resource)
}

// Set implements StorageObjectCountTracker
func (t *stoppableStorageObjectCountTracker) Set(resource string, count int64) {
	t.tracker.Set(resource, count)
}

func (t *stoppableStorageObjectCountTracker) Stop() {
	close(t.stopCh)
}

func (t *stoppableStorageObjectCountTracker) StartObserving(resource string, getterFunc func() int64) {
	if _, ok := t.observers[resource]; ok {
		return
	}

	observer := newStoppableObserver(
		getterFunc,
		func(count int64) { t.Set(resource, count) },
		t.stopCh,
	)
	observer.start()
	t.observers[resource] = observer
}

func (t *stoppableStorageObjectCountTracker) StopObserving(resource string) {
	observer, ok := t.observers[resource]

	if !ok {
		return
	}

	observer.stop()
	delete(t.observers, resource)
}
