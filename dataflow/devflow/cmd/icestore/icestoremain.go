package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/illmade-knight/go-iot-dataflows/builder/icestore"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Configure zerolog for console-friendly output.
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	// Load configuration using the flexible method that supports flags,
	// environment variables, and a config file.
	cfg, err := icestore.LoadConfigWithOverrides()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load icestore service config")
	}

	// Set the global log level from the loaded configuration.
	logLevel, err := zerolog.ParseLevel(cfg.LogLevel)
	if err != nil {
		logLevel = zerolog.InfoLevel // Default to info level on parsing error
		log.Warn().Err(err).Str("log_level", cfg.LogLevel).Msg("Could not parse log level, defaulting to 'info'")
	}
	zerolog.SetGlobalLevel(logLevel)

	log.Info().
		Str("service_name", cfg.ServiceName).
		Str("dataflow_name", cfg.DataflowName).
		Str("subscription_id", cfg.Consumer.SubscriptionID).
		Str("gcs_bucket", cfg.IceStore.BucketName).
		Str("log_level", logLevel.String()).
		Msg("Preparing to start IceStore service")

	// The NewIceStoreServiceWrapper constructor assembles the entire pipeline,
	// including the consumer, batcher, and uploader.
	iceStoreService, err := icestore.NewIceStoreServiceWrapper(cfg, log.Logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create IceStore Service")
	}

	// Start the service (which includes the HTTP server and the message processing pipeline).
	// This is a non-blocking call.
	if err := iceStoreService.Start(); err != nil {
		log.Fatal().Err(err).Msg("Failed to start IceStore Service")
	}
	log.Info().Str("port", iceStoreService.GetHTTPPort()).Msg("IceStore Service is running")

	// Set up a channel to listen for OS shutdown signals.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Block until a shutdown signal is received.
	<-quit
	log.Info().Msg("Shutdown signal received, stopping IceStore Service...")

	// Gracefully shut down the service, which will flush any pending batches.
	iceStoreService.Shutdown()
	log.Info().Msg("IceStore Service stopped.")
}
