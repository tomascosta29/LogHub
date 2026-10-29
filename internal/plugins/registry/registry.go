// Package registry provides the concurrency-safe name → factory map
// shared by the source, destination and processor plugin packages.
package registry

import "sync"

// Registry maps plugin type names to factories of type F and records
// which types are built in. It is safe for concurrent use, so plugins
// can be registered at runtime while pipelines are being built.
type Registry[F any] struct {
	mu        sync.RWMutex
	factories map[string]F
	builtIn   map[string]struct{}
}

// New returns an empty Registry.
func New[F any]() *Registry[F] {
	return &Registry[F]{
		factories: make(map[string]F),
		builtIn:   make(map[string]struct{}),
	}
}

// Register adds a factory under name. A later Register for the same
// name overwrites the previous entry.
func (r *Registry[F]) Register(name string, fn F) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[name] = fn
}

// RegisterBuiltIn marks name as a built-in plugin type. The config
// loader validates built-in types strictly and allows custom keys for
// the others.
func (r *Registry[F]) RegisterBuiltIn(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.builtIn[name] = struct{}{}
}

// IsBuiltIn reports whether name was marked built in.
func (r *Registry[F]) IsBuiltIn(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.builtIn[name]
	return ok
}

// Lookup returns the factory registered under name.
func (r *Registry[F]) Lookup(name string) (F, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.factories[name]
	return fn, ok
}
