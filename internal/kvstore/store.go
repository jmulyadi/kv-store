package kvstore

import (
	"sync"
)

// KVStore is a simple in-memory key-value store
type KVStore struct {
	data map[string]string
	mu   sync.RWMutex
}

// NewKVStore creates a new KVStore
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Put stores a key-value pair
func (s *KVStore) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get retrieves the value for a key
func (s *KVStore) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	return value, ok
}

// Delete removes a key-value pair
func (s *KVStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}
