// deployer/cloudrun/builder.go (or service.go)
package cloudrun

import (
	"time"

	runpb "cloud.google.com/go/run/apiv2/runpb"                    // Cloud Run v2 Protobuf definitions
	"github.com/illmade-knight/go-iot-dataflows/deployment/config" // Your deployer config
	"google.golang.org/protobuf/types/known/durationpb"            // For protobuf Duration type
)

// toProtoDuration converts time.Duration to *durationpb.Duration.
// It returns nil for a zero duration to allow omitempty behavior in protobuf serialization,
// which is useful for optional fields.
func toProtoDuration(d time.Duration) *durationpb.Duration {
	if d == 0 {
		return nil
	}
	return durationpb.New(d)
}

// convertProbeConfig translates your internal ProbeConfig to the Cloud Run API's runpb.Probe.
// This function handles the conversion from time.Duration to int32 seconds.
func convertProbeConfig(pc *config.ProbeConfig) *runpb.Probe {
	if pc == nil {
		return nil
	}

	runProbe := &runpb.Probe{
		InitialDelaySeconds: int32(pc.InitialDelay.Seconds()),
		PeriodSeconds:       int32(pc.Period.Seconds()),
		TimeoutSeconds:      int32(pc.Timeout.Seconds()),
		FailureThreshold:    int32(pc.FailureThreshold),
	}

	// Cloud Run currently primarily supports HTTPGet probes for Startup/Liveness checks
	if pc.Path != "" && pc.Port != 0 {
		runProbe.ProbeType = &runpb.Probe_HttpGet{
			HttpGet: &runpb.HTTPGetAction{
				Path: pc.Path,
				Port: int32(pc.Port), // Ensure port is int32
			},
		}
	}
	// If other probe types (TCP, gRPC) were supported by config.ProbeConfig,
	// additional logic would be added here to set runpb.Probe_TcpSocket or runpb.Probe_Grpc.

	return runProbe
}

// buildRevisionTemplate constructs a runpb.RevisionTemplate using DeployerConfig defaults
// and service-specific information (like image and service name for mappings).
// This function strictly uses DeployerConfig for Cloud Run parameters and now
// accepts `serviceName` directly for lookups.
func BuildRevisionTemplate(
	deployerCfg *config.DeployerConfig,
	serviceName string, // Replaced *servicemanager.ServiceSpec with serviceName string
	imageTag string, // The fully qualified Docker image tag (e.g., gcr.io/project/service:tag)
) (*runpb.RevisionTemplate, error) {

	// Get Cloud Run default settings from the deployer configuration.
	// All Cloud Run specific parameters will come from these defaults.
	defaults := deployerCfg.CloudRunDefaults

	// Determine the service account for this specific revision.
	// Prioritize service-specific mappings defined in DeployerConfig, using the provided serviceName as a key.
	serviceAccount := deployerCfg.ServiceAccountMappings[serviceName]
	if serviceAccount == "" {
		// Fallback to the default service account specified in CloudRunDefaults.
		serviceAccount = defaults.ServiceAccount
	}
	// If `serviceAccount` is still an empty string, Cloud Run will implicitly
	// use the project's default compute service account.

	// Convert the default timeout (which is an int in seconds) to a time.Duration,
	// then to the protobuf Duration type (*durationpb.Duration) required by the API.
	revisionTimeout := toProtoDuration(time.Duration(defaults.TimeoutSeconds) * time.Second)

	// Construct the runpb.RevisionTemplate.
	template := &runpb.RevisionTemplate{
		// Labels and Annotations are not present in your current ServiceSpec or CloudRunDefaults
		// for per-service configuration. If these are needed, they would need to be added
		// to your configuration structs. For now, they will be initialized as empty maps.
		Labels:      make(map[string]string),
		Annotations: make(map[string]string),

		ServiceAccount: serviceAccount,
		Timeout:        revisionTimeout,
		// Concurrency from defaults
		MaxInstanceRequestConcurrency: int32(defaults.Concurrency),
	}

	// Populate Scaling settings using default min/max instances.
	template.Scaling = &runpb.RevisionScaling{
		MinInstanceCount: int32(defaults.MinInstances),
		MaxInstanceCount: int32(defaults.MaxInstances),
	}

	// Configure the primary container for the revision.
	container := &runpb.Container{
		Image: imageTag, // The Docker image to deploy for this service comes from the build process.
		Resources: &runpb.ResourceRequirements{
			Limits:  make(map[string]string), // Initialize the Limits map as per ResourceRequirements struct
			CpuIdle: true,                    // Default to true as per provided ResourceRequirements definition
			// StartupCpuBoost: false, // Will be zero value (false) by default, or set if configured
		},
	}

	// Apply default CPU and Memory limits to the 'Limits' map.
	if defaults.CPU != "" {
		container.Resources.Limits["cpu"] = defaults.CPU
	}
	if defaults.Memory != "" {
		container.Resources.Limits["memory"] = defaults.Memory
	}

	// --- Environment Variables ---
	// Environment variable processing from svcSpec was removed as `svcSpec.EnvironmentVars`
	// was reported as non-existent. To include environment variables via deployerCfg,
	// a new field/structure needs to be defined in `deployer/config/config.go`.

	// Apply Startup and Liveness Probes from CloudRunDefaults.
	if defaults.StartupProbe != nil {
		container.StartupProbe = convertProbeConfig(defaults.StartupProbe)
	}
	if defaults.LivenessProbe != nil {
		container.LivenessProbe = convertProbeConfig(defaults.LivenessProbe)
	}

	// A RevisionTemplate usually contains a single container definition.
	template.Containers = []*runpb.Container{container}

	return template, nil
}

// boolPtr is a helper function to return a pointer to a boolean value.
// NOTE: This helper is no longer strictly needed for `CpuIdle` as it's a direct `bool`
// in the provided `ResourceRequirements` struct. It's kept here if other fields
// might still use it in the future.
func boolPtr(b bool) *bool {
	return &b
}
