package request

import (
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

const observePeriod = 1 * time.Second

type storageObjectCountObserver struct {
	getterFunc func() int64
	setterFunc func(int64)
	stopCh     chan struct{}
}

func newStoppableStorageObjectCountObserver(getterFunc func() int64, setterFunc func(int64)) *storageObjectCountObserver {
	return &storageObjectCountObserver{
		getterFunc: getterFunc,
		setterFunc: setterFunc,
	}
}

// start function creates a goroutine that periodically updates the object count by using the getter and setter functions
func (o *storageObjectCountObserver) start() <-chan struct{} {
	o.stopCh = make(chan struct{})
	go wait.Until(func() { o.setterFunc(o.getterFunc()) }, observePeriod, o.stopCh)
	return o.stopCh
}

// stop function will stop the observer goroutine
func (o *storageObjectCountObserver) stop() {
	close(o.stopCh)
}
