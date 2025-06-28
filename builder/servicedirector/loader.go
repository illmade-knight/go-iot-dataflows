package servicedirector

import (
	"context"
	"fmt"
	"os"

	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"gopkg.in/yaml.v3"
)

// ServicesDefinitionLoader defines the interface for loading service configurations.
// This abstraction allows for different sources (e.g., YAML file, Firestore)
// to provide the service definitions. It returns an interface from the servicemanager
// library, ensuring type compatibility.
type ServicesDefinitionLoader interface {
	Load(ctx context.Context) (servicemanager.ServicesDefinition, error)
}

// yamlServicesDefinition is an internal implementation that holds configuration loaded from a YAML file.
// It implements the servicemanager.ServicesDefinition interface.
type yamlServicesDefinition struct {
	config *servicemanager.TopLevelConfig
}

// NewServicesDefinitionFromYAML parses a YAML file and returns a concrete type that
// satisfies the servicemanager.ServicesDefinition interface.
func NewServicesDefinitionFromYAML(configPath string) (servicemanager.ServicesDefinition, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file '%s': %w", configPath, err)
	}

	var config servicemanager.TopLevelConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config YAML from '%s': %w", configPath, err)
	}
	// Return a concrete struct that fulfills the interface contract.
	return &yamlServicesDefinition{config: &config}, nil
}

// --- Interface Method Implementations (for servicemanager.ServicesDefinition) ---

// GetTopLevelConfig returns the entire parsed TopLevelConfig.
func (s *yamlServicesDefinition) GetTopLevelConfig() (*servicemanager.TopLevelConfig, error) {
	if s.config == nil {
		return nil, fmt.Errorf("top-level config is not loaded")
	}
	return s.config, nil
}

// GetDataflow retrieves a specific dataflow by name from the configuration.
func (s *yamlServicesDefinition) GetDataflow(name string) (*servicemanager.ResourceGroup, error) {
	if s.config == nil {
		return nil, fmt.Errorf("config not loaded")
	}
	for _, df := range s.config.Dataflows {
		if df.Name == name {
			dfCopy := df // Return a pointer to a copy to prevent modification.
			return &dfCopy, nil
		}
	}
	return nil, fmt.Errorf("dataflow '%s' not found in configuration", name)
}

// GetService retrieves a specific service by name from the configuration.
func (s *yamlServicesDefinition) GetService(name string) (*servicemanager.ServiceSpec, error) {
	if s.config == nil {
		return nil, fmt.Errorf("config not loaded")
	}
	for _, svc := range s.config.Services {
		if svc.Name == name {
			svcCopy := svc // Return a pointer to a copy.
			return &svcCopy, nil
		}
	}
	return nil, fmt.Errorf("service '%s' not found in configuration", name)
}

// GetProjectID retrieves the project ID for a given environment.
func (s *yamlServicesDefinition) GetProjectID(environment string) (string, error) {
	if s.config == nil {
		return "", fmt.Errorf("config not loaded")
	}

	if envSpec, ok := s.config.Environments[environment]; ok && envSpec.ProjectID != "" {
		return envSpec.ProjectID, nil
	}
	if s.config.DefaultProjectID != "" {
		return s.config.DefaultProjectID, nil
	}
	return "", fmt.Errorf("project ID not found for environment '%s' and no default project ID is set", environment)
}

// --- Loader Implementation ---

// YAMLServicesDefinitionLoader implements ServicesDefinitionLoader for YAML files.
type YAMLServicesDefinitionLoader struct {
	ConfigPath string
}

// NewYAMLServicesDefinitionLoader creates a new loader for YAML files.
func NewYAMLServicesDefinitionLoader(configPath string) *YAMLServicesDefinitionLoader {
	return &YAMLServicesDefinitionLoader{ConfigPath: configPath}
}

// Load uses the local YAML parsing logic to provide the configuration.
func (l *YAMLServicesDefinitionLoader) Load(ctx context.Context) (servicemanager.ServicesDefinition, error) {
	return NewServicesDefinitionFromYAML(l.ConfigPath)
}
