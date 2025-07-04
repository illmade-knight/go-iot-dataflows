package config

import (
	"fmt"
	"github.com/illmade-knight/go-iot-dataflows/builder/servicedirector"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

func LoadServicesConfig(logger zerolog.Logger, v *viper.Viper) (*servicemanager.TopLevelConfig, error) {
	configPath := v.GetString("services")
	// 4. Load services definition using the 'servicedirector' to avoid code duplication
	servicesDefinition, err := servicedirector.NewServicesDefinitionFromYAML(configPath) // Correct call as specified by user
	if err != nil {
		return nil, err
	}

	projectID := v.GetString("project_id")
	topLevelConfig, err := servicesDefinition.GetTopLevelConfig()
	if err != nil {
		return nil, err
	}
	if topLevelConfig.DefaultProjectID == "" {
		topLevelConfig.DefaultProjectID = projectID
	}
	return topLevelConfig, nil
}

// LoadDeploymentConfig loads the deployer configuration.
func LoadDeploymentConfig(logger zerolog.Logger, v *viper.Viper) (*DeployerConfig, error) {
	configPath := v.GetString("deploy")

	cfg := &DeployerConfig{}
	projectID := v.GetString("project-id")

	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Warn().Str("config_path", configPath).Msg("Deployer config file not found, initializing with defaults.")
		} else {
			return nil, fmt.Errorf("failed to read deployer config file %s: %w", configPath, err)
		}
	} else {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal deployer config from %s: %w", configPath, err)
		}
		logger.Info().Str("config_path", configPath).Msg("Deployer config file loaded.")
	}

	if cfg.ProjectID == "" {
		cfg.ProjectID = projectID
	}
	// --- 4. Dynamic adjustment for DefaultDockerRegistry ---
	if cfg.DefaultDockerRegistry == "" {
		cfg.DefaultDockerRegistry = fmt.Sprintf("gcr.io/%s", cfg.ProjectID)
		logger.Info().Str("project-id", cfg.ProjectID).Str("default_docker_registry", cfg.DefaultDockerRegistry).Msg("Default Docker Registry dynamically set based on Project ID.")
	}

	return cfg, nil
}

// Duration is a custom type that wraps time.Duration to implement yaml.Unmarshaler.
type Duration time.Duration

// UnmarshalYAML implements the yaml.Unmarshaler interface, allowing "15s" to be parsed directly to a duration.
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}
