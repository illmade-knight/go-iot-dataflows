// deployer/orchestrator/orchestrator.go
package orchestrator

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/illmade-knight/go-iot-dataflows/deployment/cloudrun"
	"github.com/illmade-knight/go-iot-dataflows/deployment/config"
	"github.com/illmade-knight/go-iot-dataflows/deployment/docker"
	"github.com/illmade-knight/go-iot-dataflows/deployment/iam"
	"github.com/illmade-knight/go-iot/pkg/servicemanager"
	"github.com/rs/zerolog"
)

// Deployer orchestrates the end-to-end deployment process of services.
type Deployer struct {
	deployerConfig     *config.DeployerConfig
	servicesConfig     *servicemanager.TopLevelConfig // This is the parsed services.yaml (TopLevelConfig)
	cloudClient        *cloudrun.Client
	iamClient          *iam.Client
	imageBuilder       *docker.ImageBuilder
	serviceDirectorURL string                                    // Stores the Service Director's URL after deployment
	deployConfigs      map[string]config.ServiceDeploymentConfig // Maps service names to their deployment configs
	logger             zerolog.Logger
}

// NewDeployer creates a new Deployer instance.
func NewDeployer(
	cfg *config.DeployerConfig,
	servicesDef *servicemanager.TopLevelConfig, // Correctly takes TopLevelConfig
	cloudRunClient *cloudrun.Client,
	iamClient *iam.Client,
	imageBuilder *docker.ImageBuilder,
	logger zerolog.Logger,
) *Deployer {
	return &Deployer{
		deployerConfig: cfg,
		servicesConfig: servicesDef, // Assign directly as it's the TopLevelConfig
		cloudClient:    cloudRunClient,
		iamClient:      iamClient,
		imageBuilder:   imageBuilder,
		logger:         logger,
	}
}

// Deploy orchestrates the end-to-end deployment of services and resources.
// Removed servicesConfig from parameters as it's already in the struct.
func (d *Deployer) Deploy(ctx context.Context, defaultDockerRegistry string, deployConfigs map[string]config.ServiceDeploymentConfig) error {
	d.logger.Info().Msg("Starting deployment orchestration.")

	// Store for later use in teardown and deployment steps
	d.deployConfigs = deployConfigs

	// 1. Provision IAM Service Accounts and Bindings for Dataflow Resources
	d.logger.Info().Msg("Provisioning IAM service accounts and roles for dataflow resources.")
	// TODO create service names if not specified - generate from projectID and serviceName
	if err := d.ProvisionIAM(ctx); err != nil {
		return fmt.Errorf("failed to provision IAM for dataflow resources: %w", err)
	}

	// 2. Build and Push Docker Images
	d.logger.Info().Msg("Building and pushing Docker images for services.")
	if err := d.BuildAndPushImages(ctx, defaultDockerRegistry); err != nil {
		return fmt.Errorf("failed to build and push images: %w", err)
	}

	// 3. Deploy Service Director (if configured) to get its URL first
	serviceDirectorConfig, directorFound := deployConfigs[d.deployerConfig.ServiceDirectorServiceName]
	if directorFound && serviceDirectorConfig.CloudRunSpec != nil {
		d.logger.Info().Str("service_name", d.deployerConfig.ServiceDirectorServiceName).Msg("Deploying Service Director.")
		// Pass the specific service deployment deployerConfig to DeployService
		serviceURL, err := d.DeployService(ctx, d.deployerConfig.ServiceDirectorServiceName, defaultDockerRegistry, serviceDirectorConfig)
		if err != nil {
			return fmt.Errorf("failed to deploy Service Director: %w", err)
		}
		d.serviceDirectorURL = serviceURL
		d.logger.Info().Str("service_name", d.deployerConfig.ServiceDirectorServiceName).Str("url", d.serviceDirectorURL).Msg("Service Director deployed.")
	} else {
		d.logger.Warn().Msg("Service Director not configured or found in deployment configs. Other services might not get its URL.")
	}

	// 4. Deploy all other Cloud Run Services
	d.logger.Info().Msg("Deploying Cloud Run services.")
	for _, serviceName := range d.servicesConfig.ServiceNames {
		if serviceName == d.deployerConfig.ServiceDirectorServiceName {
			d.logger.Info().Str("service_name", serviceName).Msg("Service Director already handled, skipping.")
			continue
		}

		svcConfig, ok := d.deployConfigs[serviceName]
		if !ok || svcConfig.CloudRunSpec == nil {
			d.logger.Info().Str("service_name", serviceName).Msg("No Cloud Run deployment deployerConfig found for service. Skipping Cloud Run deployment.")
			continue
		}

		d.logger.Info().Str("service_name", serviceName).Msg("Deploying Cloud Run service.")
		// Pass the specific service deployment deployerConfig to DeployService
		serviceURL, err := d.DeployService(ctx, serviceName, defaultDockerRegistry, svcConfig)
		if err != nil {
			return fmt.Errorf("failed to deploy Cloud Run service '%s': %w", serviceName, err)
		}
		d.logger.Info().Str("service_name", serviceName).Str("service_url", serviceURL).Msg("Cloud Run service deployed successfully.")
	}

	d.logger.Info().Msg("Deployment orchestration completed successfully.")
	return nil
}

// BuildAndPushImages builds and pushes Docker images for all defined services.
func (d *Deployer) BuildAndPushImages(ctx context.Context, defaultDockerRegistry string) error {
	for _, serviceName := range d.servicesConfig.ServiceNames {
		d.logger.Info().Str("service_name", serviceName).Msg("Processing image for service.")

		svcConfig, ok := d.deployConfigs[serviceName]
		if !ok {
			d.logger.Warn().Str("service_name", serviceName).Msg("No specific deployment deployerConfig found for service. Skipping image build/push.")
			continue
		}

		dockerfilePath := filepath.Join(d.deployerConfig.ServiceSourcePath, svcConfig.DockerfilePath)
		// We need the path to where the service main files are held. This assumes the services source base path
		// is the context for all docker builds for services.
		// Reverted to d.deployerConfig.ServicesSourceBasePath for contextPath as per previous correction.
		contextPath := d.deployerConfig.ServiceSourcePath
		imageName := fmt.Sprintf("%s/%s", defaultDockerRegistry, serviceName)

		d.logger.Info().Str("service_name", serviceName).Str("image", imageName).Msg("Building Docker image.")
		if err := d.imageBuilder.BuildImage(ctx, dockerfilePath, contextPath, imageName); err != nil {
			return fmt.Errorf("failed to build image for service '%s': %w", serviceName, err)
		}
		d.logger.Info().Str("service_name", serviceName).Str("image", imageName).Msg("Docker image built.")

		d.logger.Info().Str("service_name", serviceName).Str("image", imageName).Msg("Pushing Docker image.")
		if err := d.imageBuilder.PushImage(ctx, imageName); err != nil {
			return fmt.Errorf("failed to push image for service '%s': %w", serviceName, err)
		}
		d.logger.Info().Str("service_name", serviceName).Str("image", imageName).Msg("Docker image pushed.")
	}
	return nil
}

// DeployService deploys a single Cloud Run service.
// This function should NOT include Docker build/push logic as that is handled by BuildAndPushImages.
func (d *Deployer) DeployService(ctx context.Context, serviceName, dockerRegistry string, svcConfig config.ServiceDeploymentConfig) (string, error) {
	imageName := fmt.Sprintf("%s/%s", dockerRegistry, serviceName)
	// Use the CloudRunSpec directly from the passed svcConfig and merge with defaults.
	cloudRunConfigForDeployment := d.deployerConfig.CloudRunDefaults // Start with defaults
	if svcConfig.CloudRunSpec != nil {
		// Merge service-specific overrides onto the defaults
		cloudRunConfigForDeployment.Merge(svcConfig.CloudRunSpec)
	}

	// Inject Service Director URL as an environment variable if available
	if d.serviceDirectorURL != "" {
		if cloudRunConfigForDeployment.EnvVars == nil {
			cloudRunConfigForDeployment.EnvVars = make(map[string]string)
		}
		cloudRunConfigForDeployment.EnvVars["SERVICE_DIRECTOR_URL"] = d.serviceDirectorURL
		d.logger.Info().Str("service_name", serviceName).Str("director_url", d.serviceDirectorURL).Msg("Injected Service Director URL into service environment.")
	}

	if cloudRunConfigForDeployment.ServiceAccount != "" {
		d.logger.Info().Str("service_name", serviceName).Str("service_account", cloudRunConfigForDeployment.ServiceAccount).Msg("Ensuring service account for Cloud Run service exists.")
		saEmail, err := d.iamClient.EnsureServiceAccountExists(ctx, d.deployerConfig.ProjectID, cloudRunConfigForDeployment.ServiceAccount)
		if err != nil {
			return "", fmt.Errorf("failed to ensure service account %s for service %s: %w", cloudRunConfigForDeployment.ServiceAccount, serviceName, err)
		}
		cloudRunConfigForDeployment.ServiceAccount = saEmail // Update with full email for deployment
		d.logger.Info().Str("service_name", serviceName).Str("service_account_email", saEmail).Msg("Service account ensured for Cloud Run service.")
	}

	d.logger.Info().Str("service_name", serviceName).Str("image", imageName).Msg("Calling Cloud Run client to deploy service.")
	deployedService, err := d.cloudClient.DeployService(ctx,
		serviceName,
		imageName,
		cloudRunConfigForDeployment, // Pass the merged CloudRunSpec
	)
	if err != nil {
		return "", fmt.Errorf("failed to deploy Cloud Run service '%s': %w", serviceName, err)
	}

	//deployedService.Uri is a string not a pointer
	if deployedService.Uri == "" {
		return "", fmt.Errorf("deployed Cloud Run service URI is empty for service '%s'", serviceName)
	}
	//deployedService.Uri is a string not a pointer
	mainServiceURL := deployedService.Uri
	d.logger.Info().Str("service_name", serviceName).Str("url", mainServiceURL).Msg("Cloud Run service deployed successfully.")

	return mainServiceURL, nil
}

// ProvisionIAM provisions necessary IAM service accounts and bindings based on the services definition.
func (d *Deployer) ProvisionIAM(ctx context.Context) error {
	d.logger.Info().Msg("Starting IAM provisioning for dataflow resources.")

	projectID := d.deployerConfig.ProjectID

	// Iterate through dataflows to provision IAM for their resources
	for _, dataflow := range d.servicesConfig.Dataflows { // dataflow is of type servicemanager.ResourceGroup
		d.logger.Info().Str("dataflow", dataflow.Name).Msg("Processing IAM for dataflow resources.")

		// dataflow.Resources is not a pointer (maybe it should be but atm its not)
		//if dataflow.Resources == nil {
		//	d.logger.Debug().Str("dataflow", dataflow.Name).Msg("No resources defined for dataflow, skipping IAM provisioning.")
		//	continue
		//}

		// Pub/Sub Topics
		for _, topic := range dataflow.Resources.Topics { // Correctly using .Topics as per types.go
			if topic.IAMAccessPolicy.IAMAccess == nil {
				d.logger.Debug().Str("topic", topic.Name).Msg("No IAM access policies defined for topic, skipping.")
				continue
			}
			for _, iamPolicy := range topic.IAMAccessPolicy.IAMAccess {
				// Assuming iamPolicy.Name directly provides the service account ID/name
				// Corrected: use iamPolicy.Name directly as the service account.
				serviceAccount := iamPolicy.Name
				serviceAccountEmail, err := d.iamClient.EnsureServiceAccountExists(ctx, projectID, serviceAccount)
				if err != nil {
					return fmt.Errorf("failed to ensure service account %s for Pub/Sub topic %s: %w", serviceAccount, topic.Name, err)
				}
				if err := d.iamClient.AddPubSubTopicIAMBinding(ctx, topic.Name, iamPolicy.Role, serviceAccountEmail); err != nil {
					return fmt.Errorf("failed to add IAM binding for Pub/Sub topic %s: %w", topic.Name, err)
				}
				d.logger.Info().Str("topic", topic.Name).Str("role", iamPolicy.Role).Msg("Applied IAM binding for Pub/Sub topic.")
			}
		}

		// GCS Buckets
		for _, bucket := range dataflow.Resources.GCSBuckets {
			if bucket.IAMAccessPolicy.IAMAccess == nil {
				d.logger.Debug().Str("bucket", bucket.Name).Msg("No IAM access policies defined for bucket, skipping.")
				continue
			}
			for _, iamPolicy := range bucket.IAMAccessPolicy.IAMAccess {
				// Assuming iamPolicy.Name directly provides the service account ID/name
				serviceAccount := iamPolicy.Name
				serviceAccountEmail, err := d.iamClient.EnsureServiceAccountExists(ctx, projectID, serviceAccount)
				if err != nil {
					return fmt.Errorf("failed to ensure service account %s for GCS bucket %s: %w", serviceAccount, bucket.Name, err)
				}
				if err := d.iamClient.AddGCSBucketIAMBinding(ctx, bucket.Name, iamPolicy.Role, serviceAccountEmail); err != nil {
					return fmt.Errorf("failed to add IAM binding for GCS bucket %s: %w", bucket.Name, err)
				}
				d.logger.Info().Str("bucket", bucket.Name).Str("role", iamPolicy.Role).Msg("Applied IAM binding for GCS bucket.")
			}
		}

		// BigQuery Datasets
		for _, dataset := range dataflow.Resources.BigQueryDatasets {
			if dataset.IAMAccessPolicy.IAMAccess == nil {
				d.logger.Debug().Str("dataset", dataset.Name).Msg("No IAM access policies defined for dataset, skipping.")
				continue
			}
			for _, iamPolicy := range dataset.IAMAccessPolicy.IAMAccess {
				// Assuming iamPolicy.Name directly provides the service account ID/name
				serviceAccount := iamPolicy.Name
				serviceAccountEmail, err := d.iamClient.EnsureServiceAccountExists(ctx, projectID, serviceAccount)
				if err != nil {
					return fmt.Errorf("failed to ensure service account %s for BigQuery dataset %s: %w", serviceAccount, dataset.Name, err)
				}
				// Note: AddBigQueryDatasetIAMBinding usually needs project ID for dataset context
				if err := d.iamClient.AddBigQueryDatasetIAMBinding(ctx, projectID, dataset.Name, iamPolicy.Role, serviceAccountEmail); err != nil {
					return fmt.Errorf("failed to add IAM binding for BigQuery dataset %s: %w", dataset.Name, err)
				}
				d.logger.Info().Str("dataset", dataset.Name).Str("role", iamPolicy.Role).Msg("Applied IAM binding for BigQuery dataset.")
			}
		}
	}

	d.logger.Info().Msg("IAM provisioning completed successfully.")
	return nil
}

// Teardown orchestrates the tearing down of ephemeral services and resources.
func (d *Deployer) Teardown(ctx context.Context) error {
	d.logger.Info().Msg("Starting teardown orchestration.")

	// Iterate through dataflows to find ephemeral ones
	for _, dataflow := range d.servicesConfig.Dataflows { // dataflow is of type servicemanager.ResourceGroup
		if dataflow.Lifecycle != nil && dataflow.Lifecycle.Strategy == servicemanager.LifecycleStrategyEphemeral {
			d.logger.Info().Str("dataflow", dataflow.Name).Msg("Tearing down ephemeral dataflow resources and services.")

			// Teardown Services
			for _, serviceName := range dataflow.ServiceNames {
				d.logger.Info().Str("service_name", serviceName).Msg("Initiating teardown for Cloud Run service.")
				if err := d.TeardownService(ctx, serviceName); err != nil {
					d.logger.Error().Err(err).Str("service_name", serviceName).Msg("Failed to teardown Cloud Run service. Continuing...")
				} else {
					d.logger.Info().Str("service_name", serviceName).Msg("Cloud Run service torn down.")
				}
			}

			// Teardown IAM for resources associated with this ephemeral dataflow
			d.logger.Info().Str("dataflow", dataflow.Name).Msg("Initiating IAM teardown for dataflow resources.")
			// Re-added TeardownIAM call with correct type
			if err := d.TeardownIAM(ctx, dataflow); err != nil {
				d.logger.Error().Err(err).Str("dataflow", dataflow.Name).Msg("Failed to teardown IAM for dataflow. Continuing...")
			} else {
				d.logger.Info().Str("dataflow", dataflow.Name).Msg("IAM for dataflow resources torn down.")
			}

			// TODO: Add teardown logic for other resource types (Pub/Sub, BigQuery, GCS)
			// This would involve calling respective client methods to delete resources.
		}
	}

	d.logger.Info().Msg("Teardown orchestration completed.")
	return nil
}

// TeardownService tears down a single Cloud Run service.
func (d *Deployer) TeardownService(ctx context.Context, serviceName string) error {
	d.logger.Info().Str("service_name", serviceName).Msg("Deleting Cloud Run service.")
	err := d.cloudClient.DeleteService(ctx, serviceName)
	if err != nil {
		return fmt.Errorf("failed to delete Cloud Run service '%s': %w", serviceName, err)
	}
	return nil
}

// TeardownIAM removes IAM bindings associated with ephemeral dataflow resources.
// It accepts a specific ResourceGroup (which is what dataflow is in servicesConfig.Dataflows)
func (d *Deployer) TeardownIAM(ctx context.Context, dataflow servicemanager.ResourceGroup) error {
	d.logger.Info().Str("dataflow", dataflow.Name).Msg("Starting IAM teardown for dataflow resources.")

	// dataflow.Resources is not a pointer (maybe it should be but atm its not)
	//if dataflow.Resources == nil {
	//	d.logger.Debug().Str("dataflow", dataflow.Name).Msg("No resources defined for dataflow, skipping IAM provisioning.")
	//	continue
	//}

	projectID := d.deployerConfig.ProjectID

	// Mirror the provisioning logic to remove bindings
	// Pub/Sub Topics
	for _, topic := range dataflow.Resources.Topics { // Correctly using .Topics as per types.go
		if topic.IAMAccessPolicy.IAMAccess == nil {
			d.logger.Debug().Str("topic", topic.Name).Msg("No IAM access policies defined for topic, skipping teardown.")
			continue
		}
		for _, iamPolicy := range topic.IAMAccessPolicy.IAMAccess {
			// Assuming iamPolicy.Name directly provides the service account email or ID
			serviceAccount := iamPolicy.Name
			// Ensure we have the full email for removal, even if the service account might have been deleted already.
			// This call might fail if the SA is gone, but we proceed to attempt binding removal.
			serviceAccountEmail, err := d.iamClient.EnsureServiceAccountExists(ctx, projectID, serviceAccount)
			if err != nil {
				d.logger.Warn().Err(err).Str("service_account", serviceAccount).Msg("Could not resolve service account email for IAM teardown. Attempting removal with provided SA name.")
				serviceAccountEmail = fmt.Sprintf("%s@%s.iam.gserviceaccount.com", serviceAccount, projectID) // Fallback to assumed email format
			}

			d.logger.Info().Str("topic", topic.Name).Str("role", iamPolicy.Role).Str("service_account_email", serviceAccountEmail).Msg("Attempting to remove IAM binding for Pub/Sub topic.")
			// This assumes an `iamClient.RemovePubSubTopicIAMBinding` exists.
			// If not, this is a logical placeholder that needs implementation in the IAM client.
			if err := d.iamClient.RemovePubSubTopicIAMBinding(ctx, topic.Name, iamPolicy.Role, serviceAccountEmail); err != nil {
				d.logger.Error().Err(err).Msg("Failed to remove IAM binding for Pub/Sub topic. Continuing...")
			}
		}
	}

	// GCS Buckets
	for _, bucket := range dataflow.Resources.GCSBuckets {
		if bucket.IAMAccessPolicy.IAMAccess == nil {
			d.logger.Debug().Str("bucket", bucket.Name).Msg("No IAM access policies defined for bucket, skipping teardown.")
			continue
		}
		for _, iamPolicy := range bucket.IAMAccessPolicy.IAMAccess {
			serviceAccount := iamPolicy.Name
			serviceAccountEmail, err := d.iamClient.EnsureServiceAccountExists(ctx, projectID, serviceAccount)
			if err != nil {
				d.logger.Warn().Err(err).Str("service_account", serviceAccount).Msg("Could not resolve service account email for IAM teardown. Attempting removal with provided SA name.")
				serviceAccountEmail = fmt.Sprintf("%s@%s.iam.gserviceaccount.com", serviceAccount, projectID) // Fallback to assumed email format
			}
			d.logger.Info().Str("bucket", bucket.Name).Str("role", iamPolicy.Role).Str("service_account_email", serviceAccountEmail).Msg("Attempting to remove IAM binding for GCS bucket.")
			if err := d.iamClient.RemoveGCSBucketIAMBinding(ctx, bucket.Name, iamPolicy.Role, serviceAccountEmail); err != nil {
				d.logger.Error().Err(err).Msg("Failed to remove IAM binding for GCS bucket. Continuing...")
			}
		}
	}

	// BigQuery Datasets
	for _, dataset := range dataflow.Resources.BigQueryDatasets {
		if dataset.IAMAccessPolicy.IAMAccess == nil {
			d.logger.Debug().Str("dataset", dataset.Name).Msg("No IAM access policies defined for dataset, skipping teardown.")
			continue
		}
		for _, iamPolicy := range dataset.IAMAccessPolicy.IAMAccess {
			serviceAccount := iamPolicy.Name
			serviceAccountEmail, err := d.iamClient.EnsureServiceAccountExists(ctx, projectID, serviceAccount)
			if err != nil {
				d.logger.Warn().Err(err).Str("service_account", serviceAccount).Msg("Could not resolve service account email for IAM teardown. Attempting removal with provided SA name.")
				serviceAccountEmail = fmt.Sprintf("%s@%s.iam.gserviceaccount.com", serviceAccount, projectID) // Fallback to assumed email format
			}
			d.logger.Info().Str("dataset", dataset.Name).Str("role", iamPolicy.Role).Str("service_account_email", serviceAccountEmail).Msg("Attempting to remove IAM binding for BigQuery dataset.")
			if err := d.iamClient.RemoveBigQueryDatasetIAMBinding(ctx, projectID, dataset.Name, iamPolicy.Role, serviceAccountEmail); err != nil {
				d.logger.Error().Err(err).Msg("Failed to remove IAM binding for BigQuery dataset. Continuing...")
			}
		}
	}

	// Important Note on Service Account Deletion:
	// Deleting service accounts is highly sensitive and should only be done if you are absolutely
	// sure the service account is no longer used by any other resource or service.
	// This would require robust tracking or a specific user instruction (e.g., delete SA if created by orchestrator AND no longer used).
	// For this reason, direct service account deletion is NOT implemented here.

	d.logger.Info().Str("dataflow", dataflow.Name).Msg("IAM teardown for dataflow resources completed (bindings removed if IAM client supports).")
	return nil
}
