package connector

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// SourceFactory constructs one custom source instance.
type SourceFactory func() Source

// DestinationFactory constructs one custom destination instance.
type DestinationFactory func() Destination

// Registry owns process-local custom connector registrations. First-party
// connector names are permanently reserved and are not stored in this registry.
type Registry struct {
	mu           sync.RWMutex
	sources      map[EndpointType]SourceFactory
	destinations map[EndpointType]DestinationFactory
}

// NewRegistry returns an empty custom connector registry.
func NewRegistry() *Registry {
	return &Registry{sources: make(map[EndpointType]SourceFactory), destinations: make(map[EndpointType]DestinationFactory)}
}

// DefaultRegistry is shared by the default API and worker construction paths.
var DefaultRegistry = NewRegistry()

// RegisterSource registers a custom source constructor.
func (r *Registry) RegisterSource(endpointType string, factory SourceFactory) error {
	name, err := validateCustomRegistration(endpointType)
	if err != nil {
		return err
	}
	if factory == nil {
		return errors.New("custom source factory is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.sources[name]; exists {
		return fmt.Errorf("custom source %q is already registered", name)
	}
	if _, collision := r.destinations[name]; collision {
		return fmt.Errorf("custom connector %q is already registered as a destination", name)
	}
	r.sources[name] = factory
	return nil
}

// RegisterDestination registers a custom destination constructor.
func (r *Registry) RegisterDestination(endpointType string, factory DestinationFactory) error {
	name, err := validateCustomRegistration(endpointType)
	if err != nil {
		return err
	}
	if factory == nil {
		return errors.New("custom destination factory is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.destinations[name]; exists {
		return fmt.Errorf("custom destination %q is already registered", name)
	}
	if _, collision := r.sources[name]; collision {
		return fmt.Errorf("custom connector %q is already registered as a source", name)
	}
	r.destinations[name] = factory
	return nil
}

// HasSource reports whether a custom source type is registered.
func (r *Registry) HasSource(endpointType EndpointType) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.sources[endpointType]
	return ok
}

// HasDestination reports whether a custom destination type is registered.
func (r *Registry) HasDestination(endpointType EndpointType) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.destinations[endpointType]
	return ok
}

// NewSource constructs a registered custom source.
func (r *Registry) NewSource(endpointType EndpointType) (Source, error) {
	if r == nil {
		return nil, fmt.Errorf("custom source %q is not registered", endpointType)
	}
	r.mu.RLock()
	factory := r.sources[endpointType]
	r.mu.RUnlock()
	if factory == nil {
		return nil, fmt.Errorf("custom source %q is not registered", endpointType)
	}
	result := factory()
	if result == nil {
		return nil, fmt.Errorf("custom source %q constructor returned nil", endpointType)
	}
	return result, nil
}

// NewDestination constructs a registered custom destination.
func (r *Registry) NewDestination(endpointType EndpointType) (Destination, error) {
	if r == nil {
		return nil, fmt.Errorf("custom destination %q is not registered", endpointType)
	}
	r.mu.RLock()
	factory := r.destinations[endpointType]
	r.mu.RUnlock()
	if factory == nil {
		return nil, fmt.Errorf("custom destination %q is not registered", endpointType)
	}
	result := factory()
	if result == nil {
		return nil, fmt.Errorf("custom destination %q constructor returned nil", endpointType)
	}
	return result, nil
}

func validateCustomRegistration(value string) (EndpointType, error) {
	if value != strings.TrimSpace(value) || value == "" {
		return "", errors.New("custom connector type must be a nonempty canonical string without surrounding whitespace")
	}
	name := EndpointType(value)
	if IsBuiltinEndpointType(name) {
		return "", fmt.Errorf("custom connector type %q collides with a built-in connector", name)
	}
	return name, nil
}
