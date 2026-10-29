package processor

import (
	"fmt"

	"loghub/internal/core"
	"loghub/internal/plugins/registry"
)

// Factory creates a Processor from configuration properties.
type Factory func(props map[string]interface{}) (core.Processor, error)

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

// IsBuiltIn returns true if the processor type is a built-in plugin.
func IsBuiltIn(name string) bool {
	return plugins.IsBuiltIn(name)
}

// Create instantiates a processor by type name.
func Create(name string, props map[string]interface{}) (core.Processor, error) {
	fn, ok := plugins.Lookup(name)
	if !ok {
		return nil, fmt.Errorf("unknown processor type: %s", name)
	}
	return fn(props)
}
