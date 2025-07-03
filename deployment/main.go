// deployer/cmd/orchestrator/main.go
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/illmade-knight/go-iot-dataflows/builder/servicedirector" // Import the director package for its loader
	"github.com/illmade-knight/go-iot-dataflows/deployment/config"
	"github.com/illmade-knight/go-iot-dataflows/deployment/docker"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// 1. Initialize Logging
	log.Logger = zerolog.New(os.Stdout).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.InfoLevel) // Default to Info, can be changed.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal. Initiating graceful shutdown...")
		cancel() // Signal context cancellation
	}()

	configLogger := log.Logger.With().Str("component", "config").Logger()

	// 2. Load Deployer Configuration
	cfg, err := config.LoadConfig(configLogger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load deployer configuration")
	}
	log.Info().Interface("config", cfg).Msg("Deployer configuration loaded successfully.")

	// 3. Check Docker Availability (if required by the orchestrator, e.g., for image building)
	log.Info().Msg("Running Docker availability check...")
	if err := docker.CheckDockerAvailable(ctx, log.Logger.With().Str("component", "docker-check").Logger()); err != nil {
		log.Fatal().Err(err).Msg("Docker is not available. Please ensure Docker daemon is running.")
	}
	// The success message is now logged by docker.CheckDockerAvailable itself.

	// --- NEW CODE FOR LOADING SERVICES.YAML ---

	// 4. Determine path to services.yaml
	// Assuming services.yaml is in the same directory as the executable or current working directory.
	// For production-grade, you might make this configurable via flags/environment variables (using your sdconfig).
	servicesFilePath := "services.yaml"

	// If you want to derive the path relative to the executable's directory more robustly:
	// exePath, err := os.Executable()
	// if err != nil {
	// 	log.Fatal().Err(err).Msg("Failed to get executable path")
	// }
	// servicesFilePath = filepath.Join(filepath.Dir(exePath), "services.yaml")

	// 5. Initialize Services Definition Loader for YAML
	// This uses the loader from the 'director' package, which abstracts the source.
	var servicesDefLoader servicedirector.ServicesDefinitionLoader
	servicesDefLoader = servicedirector.NewYAMLServicesDefinitionLoader(servicesFilePath)

	// 6. Load Service Definitions
	servicesDef, err := servicesDefLoader.Load(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load service definitions from services.yaml")
	}
	log.Info().Msg("Service definitions loaded successfully.")

	_ = servicesDef
	// --- END NEW CODE ---

	// 7. Initialize GCP Clients (Cloud Run, Pub/Sub, BigQuery, GCS, IAM, etc.)
	// These clients would be instantiated here using the loaded cfg.ProjectID and cfg.Region.
	// For example:
	// cloudRunClient, err := cloudrun.NewClient(ctx, cfg.ProjectID, cfg.Region)
	// if err != nil { ... }

	// 8. Initialize Service Manager (coordinates resource provisioning and service deployment)
	// This would take the loaded servicesDef and various GCP clients.
	// For example:
	// svcManager := servicemanager.NewServiceManager(
	//    messagingClient, storageClient, bigqueryClient, iamClient,
	//    servicesDef, schemaRegistry, log.Logger,
	// )

	// 9. Execute Core Orchestration/Deployment Logic
	// This is where you'd call methods on your 'orchestrator' or 'deployer' package
	// to actually build images, deploy services, provision resources, etc.
	// For example:
	// deployer := orchestrator.NewDeployer(cfg, imageBuilder, svcManager, cloudRunClient, log.Logger)
	// if err := deployer.Run(ctx); err != nil {
	// 	log.Fatal().Err(err).Msg("Deployment orchestration failed")
	// }

	log.Info().Msg("Orchestrator finished (or waiting for tasks).")

	// The main function might then block or run a server, depending on the orchestrator's role
	// For a CLI deployment tool, it would typically exit here after completion.
	<-ctx.Done() // Block until context is cancelled (e.g., by signal)
	log.Info().Msg("Main application context cancelled. Exiting.")
}
