package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/illmade-knight/go-iot-dataflows/builder/ingestion"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Use a console writer for pretty, human-readable logs.
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	// Use the flexible loader that supports flags, env vars, and files.
	cfg, err := ingestion.LoadConfigWithOverrides()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load ingestion service config")
	}

	// --- Improved Logging ---
	// Log key configuration values to show what the service is attempting to do.
	log.Info().
		Str("service_name", cfg.ServiceName).
		Str("dataflow_name", cfg.DataflowName).
		Str("director_url", cfg.ServiceDirectorURL).
		Msg("Preparing to start ingestion service")

	if cfg.ServiceDirectorURL != "" {
		log.Info().Msg("ServiceDirector URL is configured, proceeding to verify resources...")
	} else {
		log.Warn().Msg("ServiceDirectorURL is not configured. Skipping resource verification. This is not recommended for production.")
	}
	// --- End Improved Logging ---

	// Create the IngestionService instance. The service name and dataflow name are
	// passed in from its configuration. On creation, it will contact the
	// ServiceDirectorURL (also from its config) to verify resources.
	// The internal client has its own logging, but the fatal error here
	// will now have more context from the logs above.
	ingestionService, err := ingestion.NewIngestionServiceWrapper(
		cfg,
		log.Logger,
		cfg.ServiceName,
		cfg.DataflowName,
	)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create IngestionService")
	}

	log.Info().Msg("Resource verification successful. Ingestion service were created by service director.")

	// Start the service (non-blocking).
	if err := ingestionService.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start IngestionService")
	}
	log.Info().Str("port", ingestionService.GetHTTPPort()).Msg("IngestionService is running")

	// Wait for a shutdown signal.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info().Msg("Shutdown signal received, stopping IngestionService...")

	// Gracefully shut down the service.
	ingestionService.Shutdown()
	log.Info().Msg("IngestionService stopped.")
}
