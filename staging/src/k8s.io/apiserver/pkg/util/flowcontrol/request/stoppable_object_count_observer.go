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

func newStoppableObserver(getterFunc func() int64, setterFunc func(int64), stopSignal <-chan struct{}) *storageObjectCountObserver {
	stopCh := make(chan struct{})

	// forward tracker stop signal to observer stop channel
	go func() {
		select {
		case <-stopSignal: // forwards signal and return
			close(stopCh)
		case <-stopCh: // no need to forward
		}
	}()

	return &storageObjectCountObserver{
		getterFunc: getterFunc,
		setterFunc: setterFunc,
		stopCh:     stopCh,
	}
}

func (o *storageObjectCountObserver) start() {
	go wait.Until(func() { o.setterFunc(o.getterFunc()) }, observePeriod, o.stopCh)
}

func (o *storageObjectCountObserver) stop() {
	close(o.stopCh)
}
