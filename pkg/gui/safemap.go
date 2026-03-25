package gui

import "sync"

// SafeMap is a generic thread-safe map with string keys.
type SafeMap[T any] struct {
	mu sync.RWMutex
	m  map[string]T
}

// NewSafeMap creates a new empty SafeMap.
func NewSafeMap[T any]() *SafeMap[T] {
	return &SafeMap[T]{m: make(map[string]T)}
}

// Get returns the value for the key and whether it was found.
func (sm *SafeMap[T]) Get(key string) (T, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	v, ok := sm.m[key]
	return v, ok
}

// Set stores a value for the key.
func (sm *SafeMap[T]) Set(key string, value T) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.m[key] = value
}

// Has returns true if the key exists.
func (sm *SafeMap[T]) Has(key string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	_, ok := sm.m[key]
	return ok
}

// Delete removes a key from the map.
func (sm *SafeMap[T]) Delete(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.m, key)
}

// Invalidate replaces the map contents. Pass nil to clear.
func (sm *SafeMap[T]) Invalidate(newItems map[string]T) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if newItems != nil {
		sm.m = newItems
	} else {
		sm.m = make(map[string]T)
	}
}
