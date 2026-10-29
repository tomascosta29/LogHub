package destination

import (
	"fmt"

	"loghub/internal/core"
	"loghub/internal/plugins/registry"
)

// Factory creates a Destination from configuration properties.
type Factory func(props map[string]interface{}) (core.Destination, error)

var plugins = registry.New[Factory]()

// Register adds a factory for the given type name. Built-in plugins
// register themselves via init(); user code can also call Register at
// runtime to add custom types without recompiling. A later Register
// for the same name overwrites the previous entry.
func Register(name string, fn Factory) {
	plugins.Register(name, fn)
}

// RegisterBuiltIn marks a type as built-in, so the config loader
// validates its properties strictly.
func RegisterBuiltIn(name string) {
	plugins.RegisterBuiltIn(name)
}

// IsBuiltIn returns true if the destination type is a built-in plugin.
func IsBuiltIn(name string) bool {
	return plugins.IsBuiltIn(name)
}

// IsRegistered reports whether a factory is registered for the destination type.
func IsRegistered(name string) bool {
	_, ok := plugins.Lookup(name)
	return ok
}

// Create instantiates a destination by type name.
func Create(name string, props map[string]interface{}) (core.Destination, error) {
	fn, ok := plugins.Lookup(name)
	if !ok {
		return nil, fmt.Errorf("unknown destination type: %s", name)
	}
	return fn(props)
}

// CreateWithHTTPPool creates a destination with a shared HTTP idle pool limit.
// Zero selects the default (100); non-HTTP plugins are unaffected.
func CreateWithHTTPPool(name string, props map[string]interface{}, maxIdle int) (core.Destination, error) {
	if maxIdle < 0 {
		return nil, fmt.Errorf("http_max_idle_conns_per_host cannot be negative")
	}
	dest, err := Create(name, props)
	if err != nil || maxIdle == 0 || maxIdle == 100 {
		return dest, err
	}
	var h *HTTP
	switch d := dest.(type) {
	case *HTTP:
		h = d
	case *sentinelDestination:
		h = d.httpDest
	}
	if h != nil {
		h.releaseTransport()
		h.client.Transport, h.releaseTransport = acquireTransport(maxIdle)
	}
	return dest, nil
}
