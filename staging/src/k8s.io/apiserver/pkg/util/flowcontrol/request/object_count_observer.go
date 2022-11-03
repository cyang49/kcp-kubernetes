package request

import (
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

const observePeriod = 1 * time.Second

type storageObjectCountObserver struct {
	getterFunc func() int64
	setterFunc func(int64)
}

func newStorageObjectCountObserver(getterFunc func() int64, setterFunc func(int64)) *storageObjectCountObserver {
	return &storageObjectCountObserver{
		getterFunc: getterFunc,
		setterFunc: setterFunc,
	}
}

// start function creates a goroutine that periodically updates the object count by using the getter and setter functions
func (o *storageObjectCountObserver) start(stopCh <-chan struct{}) {
	go wait.Until(func() { o.setterFunc(o.getterFunc()) }, observePeriod, stopCh)
}
