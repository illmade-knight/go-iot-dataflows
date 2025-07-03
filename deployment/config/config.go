// deployer/config/config.go
package config

import (
	"time"
)

// DeployerConfig holds the configuration for the deployment orchestrator.
// This defines global settings for where and how services are deployed.
type DeployerConfig struct {
	ProjectID                  string           `mapstructure:"project_id"`
	Region                     string           `mapstructure:"region"`
	ServicesDefinitionPath     string           `mapstructure:"services_definition_path"`
	DefaultDockerRegistry      string           `mapstructure:"default_docker_registry"`
	ServicesSourceBasePath     string           `mapstructure:"services_source_base_path"` // Base path for service source code
	ServiceDirectorServiceName string           `mapstructure:"service_director_service_name"`
	CloudRunDefaults           CloudRunDefaults `mapstructure:"cloud_run_defaults"`
	// REMOVED: ServiceAccountMappings is now handled per-service under cloud_run_spec
	DirectorServiceURL string `mapstructure:"director_service_url"` // Can be overridden for testing/existing deployments

	Services map[string]ServiceDeploymentConfig `mapstructure:"services,omitempty"` // Service-specific deployment overrides
}

// CloudRunDefaults holds default configurations for Cloud Run services,
// and can also be used for per-service overrides.
type CloudRunDefaults struct {
	CPU            string            `mapstructure:"cpu,omitempty"`             // e.g., "1000m" (1 vCPU)
	Memory         string            `mapstructure:"memory,omitempty"`          // e.g., "512Mi", "1Gi"
	Concurrency    int               `mapstructure:"concurrency,omitempty"`     // e.g., 80
	MinInstances   int               `mapstructure:"min_instances,omitempty"`   // e.g., 0
	MaxInstances   int               `mapstructure:"max_instances,omitempty"`   // e.g., 10
	TimeoutSeconds int               `mapstructure:"timeout_seconds,omitempty"` // e.g., 300 (5 minutes)
	ServiceAccount string            `mapstructure:"service_account,omitempty"` // Custom service account email
	EnvVars        map[string]string `mapstructure:"env_vars,omitempty"`        // Environment variables for the service
	StartupProbe   *ProbeConfig      `mapstructure:"startup_probe,omitempty"`
	LivenessProbe  *ProbeConfig      `mapstructure:"liveness_probe,omitempty"`
}

// ServiceDeploymentConfig holds deployment-specific configuration for a single service.
type ServiceDeploymentConfig struct {
	// Path to the main Go file relative to the service's base directory (e.g., "cmd/main.go").
	// Used by the builder to locate the entry point.
	MainPath string `mapstructure:"main_path,omitempty"`
	// Path to the Dockerfile relative to the service's base directory (e.g., "Dockerfile").
	// Used by the builder.
	DockerfilePath string `mapstructure:"dockerfile_path,omitempty"`

	// CloudRunSpec allows overriding default Cloud Run settings for this specific service.
	// Any field left empty/zero will fall back to the global CloudRunDefaults.
	CloudRunSpec *CloudRunDefaults `mapstructure:"cloud_run_spec,omitempty"`
}

// ProbeConfig defines parameters for HTTP probes (startup or liveness).
type ProbeConfig struct {
	Path             string        `mapstructure:"path"`
	Port             int           `mapstructure:"port"`
	InitialDelay     time.Duration `mapstructure:"initial_delay,omitempty"`
	Period           time.Duration `mapstructure:"period,omitempty"`
	Timeout          time.Duration `mapstructure:"timeout,omitempty"`
	FailureThreshold int           `mapstructure:"failure_threshold,omitempty"`
}
