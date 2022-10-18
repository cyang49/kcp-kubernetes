package request

import (
	"sync"

	"github.com/kcp-dev/logicalcluster/v2"
)

type StorageObjectCountGetterFunc func(clusterName logicalcluster.Name) int64

type StorageObjectCountGetterRegistry interface {
	Register(resource string, getter StorageObjectCountGetterFunc)
	Deregister(resource string)
	GetObjectCount(clusterName logicalcluster.Name, resource string) int64
}

func NewStorageObjectCountGetterRegistry() StorageObjectCountGetterRegistry {
	return &storageObjectCountGetterRegistry{
		lock:     sync.RWMutex{},
		registry: map[string]StorageObjectCountGetterFunc{},
	}
}

type storageObjectCountGetterRegistry struct {
	lock     sync.RWMutex
	registry map[string]StorageObjectCountGetterFunc
}

func (r *storageObjectCountGetterRegistry) Register(resource string, getter StorageObjectCountGetterFunc) {
	if _, ok := r.registry[resource]; ok {
		return
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	if _, ok := r.registry[resource]; ok {
		return
	}

	r.registry[resource] = getter
}

func (r *storageObjectCountGetterRegistry) Deregister(resource string) {
	if _, ok := r.registry[resource]; !ok {
		return
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	if _, ok := r.registry[resource]; !ok {
		return
	}

	delete(r.registry, resource)
}

func (r *storageObjectCountGetterRegistry) GetObjectCount(clusterName logicalcluster.Name, resource string) int64 {
	r.lock.RLock()
	defer r.lock.RUnlock()

	f, ok := r.registry[resource]
	if !ok {
		return -1
	}

	return f(clusterName)
}
