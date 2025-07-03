// deployer/e2e/deploy_test.go
package e2e_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/illmade-knight/go-iot-dataflows/deployment/config"
	"github.com/illmade-knight/go-iot-dataflows/deployment/docker" // Re-using your docker package for checks
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMain for global setup/teardown (e.g., to ensure Docker is running once for all tests)
func TestMain(m *testing.M) {
	// Initialize a logger for TestMain
	testMainLogger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	zerolog.SetGlobalLevel(zerolog.InfoLevel) // Set default log level for tests

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // Give TestMain a timeout
	defer cancel()

	testMainLogger.Info().Msg("TestMain: Running Docker availability check...")
	// Pass the logger to CheckDockerAvailable
	if err := docker.CheckDockerAvailable(ctx, testMainLogger.With().Str("component", "docker-check").Logger()); err != nil {
		testMainLogger.Fatal().Err(err).Msg("Docker is not available. Please ensure Docker daemon is running.")
		os.Exit(1) // Exit if Docker is not available, cannot run further tests.
	}
	// The message "Docker daemon is available." is now logged by CheckDockerAvailable itself.
	testMainLogger.Info().Msg("TestMain: Docker availability confirmed by TestMain.")

	// You might put common emulator setup here in the future
	// For now, just a basic Docker check.

	exitCode := m.Run() // Run all tests

	// Global teardown (if any)
	testMainLogger.Info().Msg("TestMain: All tests completed.")
	os.Exit(exitCode)
}

// TestInitialSetup verifies that the orchestrator's initial setup steps (config loading, docker checks, GCR login) work.
func TestInitialSetup(t *testing.T) {
	// Capture log output for assertion
	var logBuffer bytes.Buffer
	logger := zerolog.New(&logBuffer).With().Timestamp().Logger().Level(zerolog.DebugLevel) // Capture all logs
	// Temporarily redirect global logger only for the duration of this test
	originalLogger := log.Logger
	log.Logger = logger
	defer func() { log.Logger = originalLogger }() // Restore original logger after test

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second) // Give this test a reasonable timeout
	defer cancel()

	// --- Prepare test config file ---
	// Dynamically create a test config file for this test
	testConfigContent := `
project_id: "test-gcp-project-123"
region: "us-west1"
services_definition_path: "test_services.yaml"
# default_docker_registry: "gcr.io/test-gcp-project-123" # Will be derived if not set

cloud_run_defaults:
  cpu: "100m"
  memory: "128Mi"
`
	tempDir := t.TempDir() // Creates a temporary directory for the test
	testConfigFile := filepath.Join(tempDir, "test_deployer_config.yaml")
	err := os.WriteFile(testConfigFile, []byte(testConfigContent), 0644)
	require.NoError(t, err, "Failed to write temporary config file")

	// --- Simulate command-line arguments for config loading ---
	originalArgs := os.Args
	os.Args = []string{originalArgs[0], "--config", testConfigFile, "--project-id", "gemini-test-cli-override"}
	defer func() { os.Args = originalArgs }() // Restore original args after test

	// --- Call the relevant functions from your main flow ---
	// 1. Load Deployer Configuration
	cfg, err := config.LoadConfig(logger.With().Str("component", "config").Logger())
	require.NoError(t, err, "Failed to load deployer configuration")
	assert.Equal(t, "gemini-test-cli-override", cfg.ProjectID, "Project ID should be overridden by CLI flag")
	assert.Equal(t, "us-west1", cfg.Region, "Region should be loaded from config file")
	assert.Equal(t, "gcr.io/gemini-test-cli-override", cfg.DefaultDockerRegistry, "Default Docker registry should be derived from overridden project ID")

	// 2. Login to Google Container Registry (GCR)
	// This will try to run `gcloud auth configure-docker gcr.io/gemini-test-cli-override`
	// It relies on `gcloud` being correctly authenticated on the test machine.
	logger.Info().Msg("Attempting to login to GCR from test...") // Log this for clarity in test output
	// Pass the logger to LoginToGCR so its messages are captured
	err = docker.LoginToGCR(ctx, cfg.ProjectID, logger.With().Str("component", "docker-login").Logger())
	require.NoError(t, err, "Failed to login to GCR. Ensure gcloud is installed and authenticated.")
	assert.Contains(t, logBuffer.String(), "Successfully logged in to GCR.", "Expected GCR login success message in logs")

	// 3. Initialize Docker Image Builder (just to ensure it doesn't panic)
	_ = docker.NewImageBuilder(logger)

	// Assertions on log output (optional, but good for detailed checks)
	logs := logBuffer.String()
	// Removed the assertion for "Docker daemon is available." as it's logged by TestMain's logger.
	assert.Contains(t, logs, "Deployer configuration loaded successfully.", "Expected config loaded message in logs")
	assert.Contains(t, logs, "Default Docker registry derived from project ID.", "Expected registry derivation message")
	assert.False(t, strings.Contains(logs, "ERROR"), "No ERROR logs expected for successful initial setup")
	assert.False(t, strings.Contains(logs, "FATAL"), "No FATAL logs expected for successful initial setup")

	t.Logf("Full log output:\n%s", logs)
}
