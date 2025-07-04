package servicedirector

import (
	"fmt"
	"os"
	"strings"

	"github.com/illmade-knight/go-iot-dataflows/builder" // Import the builder for BaseConfig
	"github.com/rs/zerolog/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Config holds all configuration for the ServiceDirector itself.
type Config struct {
	builder.BaseConfig `mapstructure:",squash"` // Embed BaseConfig for common fields

	// ServicesDefSourceType indicates where the service definitions are loaded from (e.g., "yaml", "firestore").
	// This makes the ServiceDirector itself adaptable to different configuration backends.
	ServicesDefSourceType string `mapstructure:"services_def_source_type"`
	// ServicesDefPath is used if ServicesDefSourceType is "yaml".
	ServicesDefPath string `mapstructure:"services_def_path"`
	// Environment is the operational environment (e.g., "dev", "prod") for resource management.
	Environment string `mapstructure:"environment"`

	// FirestoreConfig for when we transition to Firestore for service definitions.
	// This will hold details like collection paths, project IDs if different from BaseConfig.ProjectID, etc.
	Firestore struct {
		CollectionPath string `mapstructure:"collection_path"`
		// Add other Firestore-specific config fields here, like credentials or project ID overrides
	} `mapstructure:"firestore"`
}

// LoadConfig initializes and loads the application configuration for the ServiceDirector.
// It follows a hierarchy of configuration sources: defaults -> config file -> environment variables -> command-line flags.
func LoadConfig() (*Config, error) {
	v := viper.New()

	// --- 1. Set Defaults ---
	v.SetDefault("log_level", "info")
	v.SetDefault("http_port", ":8080") // Default HTTP port for ServiceDirector
	v.SetDefault("project_id", "default-gcp-project")
	v.SetDefault("credentials_file", "") // Default to ADC for general GCP operations

	v.SetDefault("services_def_source_type", "yaml")   // Default to loading from YAML
	v.SetDefault("services_def_path", "services.yaml") // Default path for the services definition file
	v.SetDefault("environment", "dev")                 // Default operational environment

	v.SetDefault("firestore.collection_path", "service-definitions") // Default Firestore collection path

	// --- 2. Set up pflag for command-line overrides ---
	pflag.String("config", "", "ServiceSourcePath to ServiceDirector config file (e.g., director-config.yaml)")
	pflag.String("log-level", "", "Log level (debug, info, warn, error)")
	pflag.String("http-port", "", "HTTP health check port for ServiceDirector")
	pflag.String("project-id", "", "GCP Project ID for ServiceDirector's own operations")
	pflag.String("credentials-file", "", "ServiceSourcePath to GCP credentials JSON file for ServiceDirector's own GCP clients")
	pflag.String("services-def-source-type", "", "Source type for service definitions (yaml or firestore)")
	pflag.String("services-def-path", "", "ServiceSourcePath to services definition YAML file")
	pflag.String("environment", "", "Operational environment (e.g., dev, prod)")
	pflag.String("firestore-collection-path", "", "Firestore collection path for service definitions")

	pflag.Parse()
	_ = v.BindPFlags(pflag.CommandLine)

	// --- 3. Set up Viper to read from file ---
	configFile := v.GetString("config")
	if configFile != "" {
		v.SetConfigFile(configFile)
		v.SetConfigType("yaml")
		if err := v.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				log.Warn().Err(err).Str("config_file", configFile).Msg("Failed to read ServiceDirector config file, using defaults/flags/environment variables.")
			}
		}
	}

	// --- 4. Set up environment variable support ---
	v.SetEnvPrefix("SD") // Service Director prefix, e.g., SD_HTTP_PORT, SD_SERVICES_DEF_SOURCE_TYPE
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	// --- 5. Unmarshal config into our struct ---
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ServiceDirector configuration: %w", err)
	}

	// --- 6. Explicitly check for Cloud Run PORT environment variable ---
	// The `PORT` environment variable is automatically set by Cloud Run and takes precedence.
	if port := os.Getenv("PORT"); port != "" {
		log.Info().Str("old_http_port", cfg.HTTPPort).Str("new_http_port", ":"+port).Msg("Overriding ServiceDirector HTTP port with Cloud Run PORT environment variable.")
		cfg.HTTPPort = ":" + port
	}

	return &cfg, nil
}
