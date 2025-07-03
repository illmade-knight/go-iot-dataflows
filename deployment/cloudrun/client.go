// deployer/cloudrun/client.go
package cloudrun

import (
	"context"
	"fmt"
	"strings"
	"time" // Import time for duration conversions

	run "cloud.google.com/go/run/apiv2"
	runpb "cloud.google.com/go/run/apiv2/runpb"
	"github.com/illmade-knight/go-iot-dataflows/deployment/config"
	"github.com/rs/zerolog"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	durationpb "google.golang.org/protobuf/types/known/durationpb" // Import for durationpb
)

// Client provides methods for interacting with Google Cloud Run.
type Client struct {
	runClient *run.ServicesClient
	config    *config.DeployerConfig
	logger    zerolog.Logger
}

// NewClient creates a new Cloud Run client.
func NewClient(ctx context.Context, cfg *config.DeployerConfig, logger zerolog.Logger, opts ...option.ClientOption) (*Client, error) {
	runClient, err := run.NewServicesClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloud Run services client: %w", err)
	}
	return &Client{
		runClient: runClient,
		config:    cfg,
		logger:    logger.With().Str("component", "CloudRunClient").Logger(),
	}, nil
}

// Close closes the Cloud Run client.
func (c *Client) Close() error {
	return c.runClient.Close()
}

// DeployService deploys or updates a Cloud Run service based on the provided configuration.
// It creates the service if it doesn't exist, or updates it if it does.
// It now accepts config.CloudRunDefaults and the image tag directly.
func (c *Client) DeployService(
	ctx context.Context,
	serviceName string,
	imageTag string, // Add imageTag here
	cloudRunCfg config.CloudRunDefaults, // Accept custom CloudRunDefaults struct
) (*runpb.Service, error) {
	parent := fmt.Sprintf("projects/%s/locations/%s", c.config.ProjectID, c.config.Region)
	servicePath := fmt.Sprintf("%s/services/%s", parent, serviceName)

	// Construct the runpb.RevisionTemplate from cloudRunCfg and imageTag
	revisionTemplate := &runpb.RevisionTemplate{
		// Revision:       // Optional, omitted as per user's template
		Labels:         cloudRunCfg.EnvVars, // Map EnvVars to Labels if that's the intent or add a separate Labels field in config.CloudRunDefaults
		ServiceAccount: cloudRunCfg.ServiceAccount,
		Containers: []*runpb.Container{
			{
				Image: imageTag,
				Resources: &runpb.ResourceRequirements{
					Limits: map[string]string{
						"cpu":    cloudRunCfg.CPU,
						"memory": cloudRunCfg.Memory,
					},
				},
				Ports: []*runpb.ContainerPort{
					{ContainerPort: 8080}, // Assuming default container port is 8080
				},
				Env: []*runpb.EnvVar{}, // Initialize Env
			},
		},
		MaxInstanceRequestConcurrency: int32(cloudRunCfg.Concurrency),
	}

	// Add environment variables
	for k, v := range cloudRunCfg.EnvVars {
		revisionTemplate.Containers[0].Env = append(revisionTemplate.Containers[0].Env, &runpb.EnvVar{
			Name:   k,
			Values: &runpb.EnvVar_Value{Value: v}, // Corrected: use EnvVar_Value
		})
	}

	// Set optional fields if they are provided
	if cloudRunCfg.TimeoutSeconds != 0 {
		revisionTemplate.Timeout = durationpb.New(time.Duration(cloudRunCfg.TimeoutSeconds) * time.Second)
	}

	// Add scaling settings
	if cloudRunCfg.MinInstances != 0 || cloudRunCfg.MaxInstances != 0 {
		revisionTemplate.Scaling = &runpb.RevisionScaling{
			MinInstanceCount: int32(cloudRunCfg.MinInstances),
			MaxInstanceCount: int32(cloudRunCfg.MaxInstances),
		}
	}

	// Add probes if configured
	if cloudRunCfg.StartupProbe != nil {
		revisionTemplate.Containers[0].StartupProbe = &runpb.Probe{
			InitialDelaySeconds: int32(cloudRunCfg.StartupProbe.InitialDelay.Seconds()),
			PeriodSeconds:       int32(cloudRunCfg.StartupProbe.Period.Seconds()),
			TimeoutSeconds:      int32(cloudRunCfg.StartupProbe.Timeout.Seconds()),
			FailureThreshold:    int32(cloudRunCfg.StartupProbe.FailureThreshold),
			ProbeType: &runpb.Probe_HttpGet{
				HttpGet: &runpb.HTTPGetAction{
					Path: cloudRunCfg.StartupProbe.Path,
					Port: int32(cloudRunCfg.StartupProbe.Port),
				},
			},
		}
	}
	if cloudRunCfg.LivenessProbe != nil {
		revisionTemplate.Containers[0].LivenessProbe = &runpb.Probe{
			InitialDelaySeconds: int32(cloudRunCfg.LivenessProbe.InitialDelay.Seconds()),
			PeriodSeconds:       int32(cloudRunCfg.LivenessProbe.Period.Seconds()),
			TimeoutSeconds:      int32(cloudRunCfg.LivenessProbe.Timeout.Seconds()),
			FailureThreshold:    int32(cloudRunCfg.LivenessProbe.FailureThreshold),
			ProbeType: &runpb.Probe_HttpGet{
				HttpGet: &runpb.HTTPGetAction{
					Path: cloudRunCfg.LivenessProbe.Path,
					Port: int32(cloudRunCfg.LivenessProbe.Port),
				},
			},
		}
	}

	// Existing logic for Create/Update service
	_, err := c.GetService(ctx, serviceName)
	if err != nil {
		if IsNotFound(err) {
			c.logger.Info().Str("service_name", serviceName).Msg("Cloud Run service does not exist, creating new service.")
			req := &runpb.CreateServiceRequest{
				Parent:    parent,
				ServiceId: serviceName,
				Service: &runpb.Service{
					Template: revisionTemplate,
				},
			}
			op, err := c.runClient.CreateService(ctx, req)
			if err != nil {
				return nil, fmt.Errorf("failed to create Cloud Run service '%s': %w", serviceName, err)
			}
			resp, err := op.Wait(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to wait for Cloud Run service '%s' creation: %w", serviceName, err)
			}
			c.logger.Info().Str("service_name", serviceName).Str("service_url", resp.Uri).Msg("Cloud Run service created successfully.")
			return resp, nil
		}
		return nil, fmt.Errorf("failed to check for existing Cloud Run service '%s': %w", serviceName, err)
	}

	c.logger.Info().Str("service_name", serviceName).Msg("Cloud Run service exists, updating service.")
	req := &runpb.UpdateServiceRequest{
		Service: &runpb.Service{
			Name:     servicePath,
			Template: revisionTemplate,
		},
	}
	op, err := c.runClient.UpdateService(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to update Cloud Run service '%s': %w", serviceName, err)
	}
	resp, err := op.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to wait for Cloud Run service '%s' update: %w", serviceName, err)
	}
	c.logger.Info().Str("service_name", serviceName).Str("service_url", resp.Uri).Msg("Cloud Run service updated successfully.")
	return resp, nil
}

// GetService retrieves a Cloud Run service by name.
func (c *Client) GetService(ctx context.Context, serviceName string) (*runpb.Service, error) {
	name := fmt.Sprintf("projects/%s/locations/%s/services/%s", c.config.ProjectID, c.config.Region, serviceName)
	req := &runpb.GetServiceRequest{
		Name: name,
	}
	resp, err := c.runClient.GetService(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get Cloud Run service '%s': %w", serviceName, err)
	}
	return resp, nil
}

// GetLatestServiceURL retrieves the current URL of a deployed Cloud Run service.
func (c *Client) GetLatestServiceURL(ctx context.Context, serviceName string) (string, error) {
	service, err := c.GetService(ctx, serviceName)
	if err != nil {
		return "", fmt.Errorf("failed to get service '%s' to retrieve URL: %w", serviceName, err)
	}
	if service.Uri == "" {
		return "", fmt.Errorf("service '%s' has no URI; service might not be ready or deployed correctly", serviceName)
	}
	c.logger.Info().Str("service_name", serviceName).Str("service_url", service.Uri).Msg("Retrieved Cloud Run service URL.")
	return service.Uri, nil
}

// DeleteService deletes a Cloud Run service by name.
func (c *Client) DeleteService(ctx context.Context, serviceName string) error {
	name := fmt.Sprintf("projects/%s/locations/%s/services/%s", c.config.ProjectID, c.config.Region, serviceName)
	req := &runpb.DeleteServiceRequest{
		Name: name,
	}
	op, err := c.runClient.DeleteService(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to delete Cloud Run service '%s': %w", serviceName, err)
	}
	_, err = op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for Cloud Run service '%s' deletion: %w", serviceName, err)
	}
	c.logger.Info().Str("service_name", serviceName).Msg("Cloud Run service deleted successfully.")
	return nil
}

// ListServices lists all Cloud Run services in the configured project and location.
func (c *Client) ListServices(ctx context.Context) ([]*runpb.Service, error) {
	parent := fmt.Sprintf("projects/%s/locations/%s", c.config.ProjectID, c.config.Region)
	req := &runpb.ListServicesRequest{
		Parent: parent,
	}
	it := c.runClient.ListServices(ctx, req)
	var services []*runpb.Service
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list Cloud Run services: %w", err)
		}
		services = append(services, resp)
	}
	return services, nil
}

// IsNotFound checks if an error indicates a "Not Found" condition (status code 404).
func IsNotFound(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404"))
}
