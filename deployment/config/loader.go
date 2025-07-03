// deployer/config/loader.go
package config

import (
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// LoadConfig loads the deployer configuration from various sources.
// It prioritizes command-line flags, then environment variables, then config files, then defaults.
func LoadConfig(logger zerolog.Logger) (*DeployerConfig, error) {
	v := viper.New()

	// --- 1. Set up command-line flags ---
	pflag.String("config", "", "Path to the deployer configuration YAML file")
	pflag.String("project-id", "", "Google Cloud Project ID for deployment")
	pflag.String("region", "", "Google Cloud region for deployments")
	pflag.String("services-definition-path", "services.yaml", "Path to the services definition YAML file")
	pflag.String("default-docker-registry", "", "Default Docker registry for images (e.g., gcr.io/your-project-id)")
	pflag.String("director-service-url", "", "URL of the Service Director application")

	pflag.Parse()
	_ = v.BindPFlags(pflag.CommandLine) // Bind AFTER parsing

	// --- 2. Set up Viper to read from file ---
	configFile := v.GetString("config")
	if configFile != "" {
		v.SetConfigFile(configFile)
		v.SetConfigType("yaml")
		if err := v.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				logger.Warn().Err(err).Str("config_file", configFile).Msg("Failed to read deployer config file, using defaults/flags/environment variables.")
			}
		}
	}

	// --- 3. Set up environment variable support ---
	v.SetEnvPrefix("DEPLOYER") // Using "DEPLOYER" prefix for orchestrator config
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	// --- 4. Set default values ---
	v.SetDefault("project_id", "")
	v.SetDefault("region", "us-central1") // Default top-level region
	v.SetDefault("services_definition_path", "services.yaml")
	v.SetDefault("default_docker_registry", "")
	v.SetDefault("director_service_url", "")

	// Cloud Run Defaults
	v.SetDefault("cloud_run_defaults.cpu", "1000m") // 1 vCPU
	v.SetDefault("cloud_run_defaults.memory", "512Mi")
	v.SetDefault("cloud_run_defaults.concurrency", 80)
	v.SetDefault("cloud_run_defaults.min_instances", 0)
	v.SetDefault("cloud_run_defaults.max_instances", 10)
	v.SetDefault("cloud_run_defaults.timeout_seconds", 300)
	v.SetDefault("cloud_run_defaults.service_account", "") // Default is Cloud Run default SA

	// Cloud Run Default Probes
	v.SetDefault("cloud_run_defaults.startup_probe.path", "/health/startup")
	v.SetDefault("cloud_run_defaults.startup_probe.port", 8080)
	v.SetDefault("cloud_run_defaults.startup_probe.initial_delay_seconds", 1) // Changed from "1s" to 1
	v.SetDefault("cloud_run_defaults.startup_probe.period_seconds", 5)        // Changed from "5s" to 5
	v.SetDefault("cloud_run_defaults.startup_probe.timeout_seconds", 2)       // Changed from "2s" to 2
	v.SetDefault("cloud_run_defaults.startup_probe.failure_threshold", 5)

	v.SetDefault("cloud_run_defaults.liveness_probe.path", "/health/liveness")
	v.SetDefault("cloud_run_defaults.liveness_probe.port", 8080)
	v.SetDefault("cloud_run_defaults.liveness_probe.initial_delay_seconds", 0) // Changed from "0s" to 0
	v.SetDefault("cloud_run_defaults.liveness_probe.period_seconds", 30)       // Changed from "30s" to 30
	v.SetDefault("cloud_run_defaults.liveness_probe.timeout_seconds", 5)       // Changed from "5s" to 5
	v.SetDefault("cloud_run_defaults.liveness_probe.failure_threshold", 1)

	// No defaults for `cloud_run_services` map; it's explicitly defined by users.

	// --- 5. Unmarshal config into our struct ---
	var cfg DeployerConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal deployer configuration: %w", err)
	}

	return &cfg, nil
}
