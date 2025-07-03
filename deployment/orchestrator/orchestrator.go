// deployer/orchestrator/orchestrator.go
package orchestrator

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/illmade-knight/go-iot-dataflows/deployment/cloudrun"
	"github.com/illmade-knight/go-iot-dataflows/deployment/config"
	"github.com/illmade-knight/go-iot-dataflows/deployment/docker"
	"github.com/illmade-knight/go-iot/pkg/servicemanager" // Assuming this contains ServiceSpec and related types
	"github.com/rs/zerolog"
)

// Deployer orchestrates the end-to-end deployment process of services.
type Deployer struct {
	config       *config.DeployerConfig
	servicesDef  servicemanager.TopLevelConfig // This is the parsed services.yaml (TopLevelConfig)
	cloudClient  *cloudrun.Client
	imageBuilder *docker.ImageBuilder
	logger       zerolog.Logger
}

// NewDeployer creates a new Deployer instance.
func NewDeployer(
	cfg *config.DeployerConfig,
	servicesDef servicemanager.TopLevelConfig,
	cloudRunMgr *cloudrun.Client,
	imageBuilder *docker.ImageBuilder,
	logger zerolog.Logger,
) *Deployer {
	return &Deployer{
		config:       cfg,
		servicesDef:  servicesDef,
		cloudClient:  cloudRunMgr,
		imageBuilder: imageBuilder,
		logger:       logger.With().Str("component", "Deployer").Logger(),
	}
}

// Run executes the deployment orchestration.
func (d *Deployer) Run(ctx context.Context) error {
	d.logger.Info().Msg("Starting deployment orchestration.")

	// --- Phase 1: Build and Deploy Service Director (if defined) ---
	directorServiceName := d.config.ServiceDirectorServiceName
	if directorServiceName != "" {
		d.logger.Info().Str("service_name", directorServiceName).Msg("Processing Service Director.")
		// Pass project ID from deployer config
		directorURL, err := d.deployService(ctx, directorServiceName, d.config.DefaultDockerRegistry) // Removed projectID as it's in d.config already
		if err != nil {
			return fmt.Errorf("failed to deploy Service Director '%s': %w", directorServiceName, err)
		}
		// Update the DeployerConfig with the director's URL for other services to use
		d.config.DirectorServiceURL = directorURL
		d.logger.Info().Str("service_name", directorServiceName).Str("url", directorURL).Msg("Service Director deployed and URL set.")
	} else {
		d.logger.Info().Msg("No Service Director specified. Skipping deployment.")
	}

	// --- Phase 2: Deploy other services ---
	// Collect all service names from all dataflows in services.yaml
	// Assuming servicesDef (servicemanager.ServicesDefinition) is the TopLevelConfig struct
	// and has a Dataflows field, each with ServiceNames.
	allServiceNames := make(map[string]struct{})       // Use a map to handle duplicates if a service is in multiple dataflows
	for _, dataflow := range d.servicesDef.Dataflows { // Access Dataflows directly from servicesDef
		for _, serviceName := range dataflow.ServiceNames {
			allServiceNames[serviceName] = struct{}{}
		}
	}

	for serviceName := range allServiceNames {
		if serviceName == directorServiceName {
			continue // Already handled
		}
		d.logger.Info().Str("service_name", serviceName).Msg("Processing service.")
		// Pass project ID from deployer config
		_, err := d.deployService(ctx, serviceName, d.config.DefaultDockerRegistry) // Removed projectID
		if err != nil {
			return fmt.Errorf("failed to deploy service '%s': %w", serviceName, err)
		}
	}

	d.logger.Info().Msg("Deployment orchestration completed successfully.")
	return nil
}

// deployService handles the build and deployment of a single service.
// It returns the deployed service's URL.
// Removed projectID as a parameter since it's available via d.config.ProjectID
func (d *Deployer) deployService(ctx context.Context, serviceName, dockerRegistry string) (string, error) {
	// 1. Get service-specific deployment configuration
	// Check if there are overrides for this service in deployer_config.yaml.
	serviceCfg, hasServiceSpecificConfig := d.config.Services[serviceName]

	// 2. Determine source paths for building
	serviceSourceDir := filepath.Join(d.config.ServicesSourceBasePath, serviceName)

	//mainPath := filepath.Join(serviceSourceDir, "cmd", "main.go") // Default Go main path
	//if hasServiceSpecificConfig && serviceCfg.MainPath != "" {
	//	mainPath = filepath.Join(serviceSourceDir, serviceCfg.MainPath) // Override if specified
	//}
	dockerfilePath := filepath.Join(serviceSourceDir, "Dockerfile") // Default Dockerfile path
	if hasServiceSpecificConfig && serviceCfg.DockerfilePath != "" {
		dockerfilePath = filepath.Join(serviceSourceDir, serviceCfg.DockerfilePath) // Override if specified
	}

	// 3. Build Docker Image
	imageTag := fmt.Sprintf("%s/%s:latest", dockerRegistry, serviceName)
	d.logger.Info().Str("service_name", serviceName).Str("image_tag", imageTag).Msg("Building Docker image.")

	if err := d.imageBuilder.BuildImage(ctx, dockerfilePath, serviceSourceDir, imageTag); err != nil {
		return "", fmt.Errorf("failed to build image for service '%s': %w", serviceName, err)
	}
	d.logger.Info().Str("service_name", serviceName).Str("image_tag", imageTag).Msg("Docker image built successfully.")

	// 4. Push Docker Image
	d.logger.Info().Str("service_name", serviceName).Str("image_tag", imageTag).Msg("Pushing Docker image.")
	if err := d.imageBuilder.PushImage(ctx, imageTag); err != nil {
		return "", fmt.Errorf("failed to push image for service '%s': %w", serviceName, err)
	}
	d.logger.Info().Str("service_name", serviceName).Str("image_tag", imageTag).Msg("Docker image pushed successfully.")

	// 5. Determine Cloud Run Deployment Configuration
	// Start with global defaults from deployer_config.yaml
	cloudRunCfg := d.config.CloudRunDefaults

	// Apply service-specific overrides from deployer_config.yaml
	if hasServiceSpecificConfig && serviceCfg.CloudRunSpec != nil {
		// Manual merge logic: copy non-zero/non-empty values from serviceCfg.CloudRunSpec
		// to override the cloudRunCfg (which holds the global defaults).
		if serviceCfg.CloudRunSpec.CPU != "" {
			cloudRunCfg.CPU = serviceCfg.CloudRunSpec.CPU
		}
		if serviceCfg.CloudRunSpec.Memory != "" {
			cloudRunCfg.Memory = serviceCfg.CloudRunSpec.Memory
		}
		if serviceCfg.CloudRunSpec.Concurrency != 0 {
			cloudRunCfg.Concurrency = serviceCfg.CloudRunSpec.Concurrency
		}
		if serviceCfg.CloudRunSpec.MinInstances != 0 {
			cloudRunCfg.MinInstances = serviceCfg.CloudRunSpec.MinInstances
		}
		if serviceCfg.CloudRunSpec.MaxInstances != 0 {
			cloudRunCfg.MaxInstances = serviceCfg.CloudRunSpec.MaxInstances
		}
		if serviceCfg.CloudRunSpec.TimeoutSeconds != 0 {
			cloudRunCfg.TimeoutSeconds = serviceCfg.CloudRunSpec.TimeoutSeconds
		}
		if serviceCfg.CloudRunSpec.ServiceAccount != "" {
			cloudRunCfg.ServiceAccount = serviceCfg.CloudRunSpec.ServiceAccount
		}

		// Merge EnvVars: service-specific overrides global EnvVars.
		if len(serviceCfg.CloudRunSpec.EnvVars) > 0 {
			if cloudRunCfg.EnvVars == nil {
				cloudRunCfg.EnvVars = make(map[string]string)
			}
			for k, v := range serviceCfg.CloudRunSpec.EnvVars {
				cloudRunCfg.EnvVars[k] = v
			}
		}

		// Merge Probes: If a service specifies a probe, it completely replaces the default probe.
		if serviceCfg.CloudRunSpec.StartupProbe != nil {
			cloudRunCfg.StartupProbe = serviceCfg.CloudRunSpec.StartupProbe
		}
		if serviceCfg.CloudRunSpec.LivenessProbe != nil {
			cloudRunCfg.LivenessProbe = serviceCfg.CloudRunSpec.LivenessProbe
		}
	}

	// 6. Deploy/Update Cloud Run Service
	d.logger.Info().Str("service_name", serviceName).Str("image_tag", imageTag).Msg("Deploying Cloud Run service.")
	// Pass the merged cloudRunCfg and imageTag to the Cloud Run client
	service, err := d.cloudClient.DeployService(ctx, serviceName, imageTag, cloudRunCfg) // Corrected function call and arguments
	if err != nil {
		return "", fmt.Errorf("failed to deploy Cloud Run service '%s': %w", serviceName, err)
	}
	mainServiceURL := service.Urls[0]
	d.logger.Info().Str("service_name", serviceName).Str("url", mainServiceURL).Msg("Cloud Run service deployed successfully.")

	return mainServiceURL, nil
}

// Teardown orchestrates the tearing down of ephemeral services and resources.
func (d *Deployer) Teardown(ctx context.Context) error {
	d.logger.Info().Msg("Starting teardown orchestration.")

	// Iterate through dataflows to find ephemeral ones
	// Access Dataflows directly from servicesDef
	for _, dataflow := range d.servicesDef.Dataflows {
		if dataflow.Lifecycle != nil && dataflow.Lifecycle.Strategy == servicemanager.LifecycleStrategyEphemeral {
			d.logger.Info().Str("dataflow", dataflow.Name).Msg("Tearing down ephemeral dataflow resources and services.")
			for _, serviceName := range dataflow.ServiceNames {
				d.logger.Info().Str("service_name", serviceName).Msg("Deleting ephemeral Cloud Run service.")
				err := d.cloudClient.DeleteService(ctx, serviceName)
				if err != nil {
					d.logger.Error().Err(err).Str("service_name", serviceName).Msg("Failed to delete Cloud Run service during teardown. Continuing...")
				} else {
					d.logger.Info().Str("service_name", serviceName).Msg("Ephemeral Cloud Run service deleted.")
				}
				// TODO: Add logic here to tear down other resource types (Pub/Sub, BigQuery, GCS)
				// by calling their respective managers, similar to Cloud Run.
				// This would also involve removing IAM role bindings for service accounts.
			}
		}
	}

	d.logger.Info().Msg("Teardown orchestration completed.")
	return nil
}
