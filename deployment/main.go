package main

import (
	"context"
	"fmt"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"

	"github.com/illmade-knight/go-iot-dataflows/deployment/cloudrun"
	"github.com/illmade-knight/go-iot-dataflows/deployment/config" // Import our loader for deployer config
	"github.com/illmade-knight/go-iot-dataflows/deployment/docker"
	"github.com/illmade-knight/go-iot-dataflows/deployment/iam"
	"github.com/illmade-knight/go-iot-dataflows/deployment/orchestrator"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// 1. Setup logging
	logger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = logger

	ctx := context.Background()

	// 2. Define ALL command-line flags using pflag
	var action string
	pflag.StringVar(&action, "action", "deploy", "Action to perform: 'deploy' or 'teardown'")

	var deployerCfgPath string
	pflag.StringVar(&deployerCfgPath, "deploy", "deployment.yaml", "ServiceSourcePath to the deployer configuration file")

	var servicesDefPath string
	pflag.StringVar(&servicesDefPath, "services", "services.yaml", "ServiceSourcePath to the services definition file")

	// Define other flags that can override config/env, e.g., project-id, region
	// These flags are for direct overrides and will be picked up by Viper
	pflag.String("project-id", "", "Google Cloud Project ID for deployment (overrides config/env)")

	pflag.Parse() // Parse all the defined flags

	// --- DEBUGGING: Print parsed pflag values ---
	log.Info().Msg("--- pflag parsed values ---")
	log.Info().Str("action", action).Msg("Flag value")
	log.Info().Str("deploy", deployerCfgPath).Msg("Flag value")
	log.Info().Str("services", servicesDefPath).Msg("Flag value")
	// --- END DEBUGGING ---

	// 3. Bind pflags to Viper
	// This makes Viper aware of command-line flags.
	v := viper.New() // Create a new Viper instance for flags

	v.BindPFlags(pflag.CommandLine) // Bind AFTER parsing

	if v.GetString("project-id") != pflag.Lookup("project-id").Value.String() {
		log.Error().Str("project-id", pflag.Lookup("project-id").Value.String()).Msg("Mismatched")
	}
	log.Info().Str("deploy", pflag.Lookup("deploy").Value.String()).Msg("deploy config")

	// 3. Load deployer configuration using its loader.
	// This assumes config.LoadConfig (in deployer/config/loader.go) handles its own flag parsing (with pflag) internally.
	deployerCfg, err := config.LoadDeploymentConfig(logger, v) // Calling without passing deployerCfgPath, as per user's main.go
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load deployer configuration")
	}

	servicesCfg, err := config.LoadServicesConfig(logger, v)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load deployer configuration")
	}

	// 5. Initialize clients
	cloudRunLogger := logger.With().Str("component", "cloudRunClient").Logger()
	// Ensure NewClient uses deployerCfg directly for project ID and region
	cloudRunClient, err := cloudrun.NewClient(ctx, deployerCfg, cloudRunLogger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create cloudRun client")
	}
	// Use deployerCfg.ProjectID for IAM client. Note: user's original main.go snippet used topLevelConfig.DefaultProjectID here.
	// Sticking to deployerCfg.ProjectID as it should be the primary config for the orchestrator.
	iamClient, err := iam.NewClient(ctx, deployerCfg.ProjectID)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create IAM client")
	}
	dockerLogger := logger.With().Str("component", "dockerBuilder").Logger()
	imageBuilder := docker.NewImageBuilder(dockerLogger)

	// 6. Create Deployer instance
	deployer := orchestrator.NewDeployer(
		deployerCfg,
		servicesCfg,
		cloudRunClient,
		iamClient,
		imageBuilder,
		log.Logger,
	)

	// 7. Execute action based on flag
	switch action {
	case "deploy":
		log.Info().Msg("Starting deployment process...")
		if err := deployer.Deploy(ctx, deployerCfg.DefaultDockerRegistry, deployerCfg.Services); err != nil {
			log.Fatal().Err(err).Msg("Deployment failed")
		}
		log.Info().Msg("Deployment completed successfully!")
	case "teardown":
		log.Info().Msg("Starting teardown process...")
		if err := deployer.Teardown(ctx); err != nil {
			log.Fatal().Err(err).Msg("Teardown failed")
		}
		log.Info().Msg("Teardown completed successfully!")
	default:
		log.Fatal().Msg(fmt.Sprintf("Invalid action: %s. Please use 'deploy' or 'teardown'.", action))
	}
}
