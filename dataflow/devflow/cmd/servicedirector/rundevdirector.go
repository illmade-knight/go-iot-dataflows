package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/illmade-knight/go-iot-dataflows/builder/servicedirector"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Use a console writer for pretty, human-readable logs.
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	// Load the ServiceDirector's own configuration. This will read flags like
	// --services-def-path to find the services.yaml file.
	cfg, err := servicedirector.LoadConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load servicedirector config")
	}

	// For local development, if the default services.yaml is not found,
	// try a path relative to the project root. This makes `go run` work
	// from the root directory without needing to pass a flag.
	if _, err := os.Stat(cfg.ServicesDefPath); os.IsNotExist(err) {
		log.Warn().
			Str("path", cfg.ServicesDefPath).
			Msg("Could not find services.yaml at the default path. Trying path relative to project root for local dev.")

		// This path is specific to the devflow dataflow structure.
		devflowPath := "dataflow/devflow/services.yaml"
		if _, err2 := os.Stat(devflowPath); err2 == nil {
			log.Info().Str("path", devflowPath).Msg("Found services.yaml at devflow path.")
			cfg.ServicesDefPath = devflowPath
		} else {
			// If neither the default/flag path nor the devflow path works, fail.
			log.Fatal().Err(err).Str("checked_paths", cfg.ServicesDefPath+", "+devflowPath).Msg("services.yaml definition file not found")
		}
	}

	// Create a loader that reads definitions from the specified YAML file.
	loader := servicedirector.NewYAMLServicesDefinitionLoader(cfg.ServicesDefPath)

	// Create the ServiceDirector instance.
	schemaRegistry := map[string]interface{}{}
	director, err := servicedirector.NewServiceDirector(context.Background(), cfg, loader, schemaRegistry, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create ServiceDirector")
	}

	// Start the service. This is non-blocking.
	if err := director.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start ServiceDirector")
	}
	log.Info().Str("port", director.GetHTTPPort()).Msg("ServiceDirector is running")

	// Wait for a shutdown signal.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info().Msg("Shutdown signal received, stopping ServiceDirector...")

	// Gracefully shut down the service.
	director.Shutdown()
	log.Info().Msg("ServiceDirector stopped.")
}
