package request

import (
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

const observePeriod = 5 * time.Second

type storageObjectCountObserver struct {
	getterFunc func() int64
	setterFunc func(int64)
	stopCh     chan struct{}
}

func newStoppableStorageObjectCountObserver(getterFunc func() int64, setterFunc func(int64)) *storageObjectCountObserver {
	return &storageObjectCountObserver{
		getterFunc: getterFunc,
		setterFunc: setterFunc,
		stopCh:     make(chan struct{}),
	}
}

// start function creates two goroutines
// One for forwarding the upstream stop signal to local observer stopCh
// The other updates the object count by using the getter and setter functions
func (o *storageObjectCountObserver) start(stopSignal <-chan struct{}) {
	// For forwarding upstream stop signal to observer stop channel
	go func() {
		select {
		case <-stopSignal:
			close(o.stopCh)
		case <-o.stopCh:
		}
	}()

	// Observer goroutine
	go wait.Until(func() { o.setterFunc(o.getterFunc()) }, observePeriod, o.stopCh)
}

// stop function will stop the observer goroutines
func (o *storageObjectCountObserver) stop() {
	close(o.stopCh)
}
