package source

import (
	"context"
	"fmt"

	"loghub/internal/core"
	"loghub/internal/plugins/registry"
)

// SecretResolver resolves secret values. It is typically satisfied by
// *secrets.SecretHolder without importing the secrets package.
type SecretResolver interface {
	Resolve(ctx context.Context, value string) (string, error)
}

// Factory creates a Source from configuration properties.
type Factory func(ctx context.Context, props map[string]interface{}, sh SecretResolver) (core.Source, error)

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

// IsBuiltIn returns true if the source type is a built-in plugin.
func IsBuiltIn(name string) bool {
	return plugins.IsBuiltIn(name)
}

// IsRegistered reports whether a factory is registered for the source type.
func IsRegistered(name string) bool {
	_, ok := plugins.Lookup(name)
	return ok
}

// Create instantiates a source by type name.
func Create(name string, ctx context.Context, props map[string]interface{}, sh SecretResolver) (core.Source, error) {
	fn, ok := plugins.Lookup(name)
	if !ok {
		return nil, fmt.Errorf("unknown source type: %s", name)
	}
	return fn(ctx, props, sh)
}
