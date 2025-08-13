package kvstore

import "sync"

type KVStore struct {
	mu    sync.RWMutex
	store map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{store: make(map[string]string)}
}

func (k *KVStore) Put(key, value string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.store[key] = value
}

func (k *KVStore) Get(key string) (string, bool) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	v, ok := k.store[key]
	return v, ok
}

func (k *KVStore) Delete(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.store, key)
}
