// deployer/config/config.go
package config

import (
	"time"
)

// DeployerConfig holds the configuration for the deployment orchestrator.
// This defines global settings for where and how services are deployed.
type DeployerConfig struct {
	Region                     string `yaml:"region,omitempty"`
	ServiceSourcePath          string `yaml:"service_source_path,omitempty"`
	ServiceDirectorServiceName string `yaml:"service_director_service_name,omitempty"`

	ProjectID string `yaml:"project_id,omitempty"`
	// we usually leave this to be specified by ProjectID
	DefaultDockerRegistry string `yaml:"default_docker_registry,omitempty"`
	// this is filled in once the service is deployed
	DirectorServiceURL string `yaml:"director_service_url"` // Can be overridden for testing/existing deployments

	CloudRunDefaults CloudRunSetup                      `yaml:"cloud_run_defaults"`
	Services         map[string]ServiceDeploymentConfig `mapstructure:"services,omitempty"` // Service-specific deployment overrides
}

// CloudRunSetup holds default configurations for Cloud Run services,
// and can also be used for per-service overrides.
type CloudRunSetup struct {
	CPU            string            `yaml:"cpu,omitempty"`             // e.g., "1000m" (1 vCPU)
	Memory         string            `yaml:"memory,omitempty"`          // e.g., "512Mi", "1Gi"
	Concurrency    int               `yaml:"concurrency,omitempty"`     // e.g., 80
	MinInstances   int               `yaml:"min_instances,omitempty"`   // e.g., 0
	MaxInstances   int               `yaml:"max_instances,omitempty"`   // e.g., 10
	TimeoutSeconds int               `yaml:"timeout_seconds,omitempty"` // e.g., 300 (5 minutes)
	ServiceAccount string            `yaml:"service_account,omitempty"` // Custom service account email
	EnvVars        map[string]string `yaml:"env_vars,omitempty"`        // Environment variables for the service
	StartupProbe   *ProbeConfig      `yaml:"startup_probe,omitempty"`
	LivenessProbe  *ProbeConfig      `yaml:"liveness_probe,omitempty"`
}

// Merge applies non-zero/non-empty values from a source CloudRunSetup onto the receiver CloudRunSetup.
// For EnvVars, it performs a merge (add/update). ProbeConfigs are replaced if provided in src.
func (c *CloudRunSetup) Merge(src *CloudRunSetup) { // Corrected parameter type to *CloudRunSetup
	if src == nil {
		return
	}

	if src.ServiceAccount != "" {
		c.ServiceAccount = src.ServiceAccount
	}
	if src.CPU != "" {
		c.CPU = src.CPU
	}
	if src.Memory != "" {
		c.Memory = src.Memory
	}
	if src.Concurrency != 0 {
		c.Concurrency = src.Concurrency
	}
	if src.TimeoutSeconds != 0 {
		c.TimeoutSeconds = src.TimeoutSeconds
	}
	if src.MaxInstances != 0 {
		c.MaxInstances = src.MaxInstances
	}
	if src.MinInstances != 0 {
		c.MinInstances = src.MinInstances
	}

	// Merge EnvVars: add or update entries from src.EnvVars into c.EnvVars
	if len(src.EnvVars) > 0 {
		if c.EnvVars == nil {
			c.EnvVars = make(map[string]string)
		}
		for k, v := range src.EnvVars {
			c.EnvVars[k] = v
		}
	}

	// ProbeConfigs: Replace the entire probe if src has one.
	if src.StartupProbe != nil {
		c.StartupProbe = src.StartupProbe
	}
	if src.LivenessProbe != nil {
		c.LivenessProbe = src.LivenessProbe
	}
}

// ServiceDeploymentConfig holds deployment-specific configuration for a single service.
type ServiceDeploymentConfig struct {
	// ServiceSourcePath to the main Go file relative to the service's base directory (e.g., "cmd/main.go").
	// Used by the builder to locate the entry point.
	MainPath string `yaml:"main_path,omitempty"`
	// ServiceSourcePath to the Dockerfile relative to the service's base directory (e.g., "Dockerfile").
	// Used by the builder.
	DockerfilePath string `yaml:"dockerfile_path,omitempty"`

	// CloudRunSpec allows overriding default Cloud Run settings for this specific service.
	// Any field left empty/zero will fall back to the global CloudRunSetup.
	CloudRunSpec *CloudRunSetup `yaml:"cloud_run_spec,omitempty"`
}

// ProbeConfig defines parameters for HTTP probes (startup or liveness).
type ProbeConfig struct {
	Path             string        `yaml:"path"`
	Port             int           `yaml:"port"`
	InitialDelay     time.Duration `yaml:"initial_delay,omitempty"`
	Period           time.Duration `yaml:"period,omitempty"`
	Timeout          time.Duration `yaml:"timeout,omitempty"`
	FailureThreshold int           `yaml:"failure_threshold,omitempty"`
}
