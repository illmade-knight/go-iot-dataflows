// deployer/docker/builder.go
package docker

import (
	"context"
	"fmt"
	"os"            // For os.Create and os.WriteFile
	"os/exec"       // For docker commands
	"path/filepath" // For path manipulation

	"github.com/rs/zerolog"
)

// CheckDockerAvailable checks if the Docker daemon is running and accessible.
func CheckDockerAvailable(ctx context.Context, logger zerolog.Logger) error {
	logger.Info().Msg("Checking Docker daemon availability...")
	cmd := exec.CommandContext(ctx, "docker", "info")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker not available: %w, output: %s", err, string(output))
	}
	logger.Info().Msg("Docker daemon is available.")
	return nil
}

// LoginToGCR attempts to log the Docker client into Google Container Registry (GCR)
// for the given project ID.
func LoginToGCR(ctx context.Context, projectID string, logger zerolog.Logger) error {
	registry := fmt.Sprintf("gcr.io/%s", projectID)
	logger.Info().Str("project_id", projectID).Msg("Attempting to login to GCR...")

	cmd := exec.CommandContext(ctx, "gcloud", "auth", "configure-docker", registry)
	output, err := cmd.CombinedOutput()
	if err != nil {
		logger.Error().Err(err).Str("output", string(output)).Msg("Failed to configure Docker for GCR.")
		return fmt.Errorf("failed to configure Docker for GCR (%s): %w, output: %s", registry, err, string(output))
	}
	logger.Info().Msg("Successfully configured Docker for GCR.")
	return nil
}

// ImageBuilder provides methods for building and pushing Docker images.
type ImageBuilder struct {
	logger zerolog.Logger
}

// NewImageBuilder creates a new ImageBuilder.
func NewImageBuilder(logger zerolog.Logger) *ImageBuilder {
	return &ImageBuilder{
		logger: logger.With().Str("component", "ImageBuilder").Logger(),
	}
}

// BuildImage builds a Docker image from a given Dockerfile context.
// dockerfilePath is the path to the Dockerfile to use (can be temporary).
// contextPath is the build context directory (where source code is).
// imageTag is the full tag for the image (e.g., gcr.io/my-project/my-service:latest).
func (b *ImageBuilder) BuildImage(ctx context.Context, dockerfilePath, contextPath, imageTag string) error {
	b.logger.Info().Str("image_tag", imageTag).Str("dockerfile_path", dockerfilePath).Str("context_path", contextPath).Msg("Building Docker image...")

	// Docker build command: docker build -f <dockerfile-path> -t <image-tag> <context-path>
	cmd := exec.CommandContext(ctx, "docker", "build", "-f", dockerfilePath, "-t", imageTag, contextPath)
	cmd.Stdout = b.logger // Direct output to logger
	cmd.Stderr = b.logger // Direct errors to logger

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to build Docker image %s: %w", imageTag, err)
	}

	b.logger.Info().Str("image_tag", imageTag).Msg("Docker image built successfully.")
	return nil
}

// PushImage pushes a Docker image to a registry.
func (b *ImageBuilder) PushImage(ctx context.Context, imageTag string) error {
	b.logger.Info().Str("image_tag", imageTag).Msg("Pushing Docker image...")

	cmd := exec.CommandContext(ctx, "docker", "push", imageTag)
	cmd.Stdout = b.logger
	cmd.Stderr = b.logger

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to push Docker image %s: %w", imageTag, err)
	}

	b.logger.Info().Str("image_tag", imageTag).Msg("Docker image pushed successfully.")
	return nil
}

// GenerateSimpleGoDockerfile creates a basic Dockerfile for a Go application.
// This assumes a standard Go module structure where `mainFilePathInContext`
// is relative to `contextPath` (e.g., `cmd/main.go`).
// The generated Dockerfile will be written to `outputPath`.
func (b *ImageBuilder) GenerateSimpleGoDockerfile(contextPath, mainFilePathInContext, outputPath string) error {
	b.logger.Info().Str("context_path", contextPath).Str("main_file", mainFilePathInContext).Str("output_path", outputPath).Msg("Generating simple Go Dockerfile...")

	dockerfileContent := fmt.Sprintf(`
# Use a Go base image for building
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum files and download dependencies
COPY %s/go.mod %s/go.sum ./
RUN go mod download

# Copy the rest of the application source code
COPY %s %s

# Build the Go application
# CGO_ENABLED=0 is important for static binaries on Alpine
# The output binary will be named 'server'
RUN CGO_ENABLED=0 go build -o /usr/local/bin/server %s/%s

# Use a minimal production-ready base image
FROM alpine:latest

# Set timezone
RUN apk add --no-cache tzdata

WORKDIR /app

# Copy the built executable from the builder stage
COPY --from=builder /usr/local/bin/server /usr/local/bin/server

# Expose port 8080 as per Cloud Run convention
EXPOSE 8080

# Run the executable
CMD ["/usr/local/bin/server"]
`,
		filepath.Base(contextPath), filepath.Base(contextPath), // For go.mod/go.sum
		filepath.Base(contextPath), filepath.Base(contextPath), // For source code
		filepath.Base(contextPath), mainFilePathInContext) // For build command

	err := os.WriteFile(outputPath, []byte(dockerfileContent), 0644)
	if err != nil {
		return fmt.Errorf("failed to write generated Dockerfile to %s: %w", outputPath, err)
	}

	b.logger.Info().Str("output_path", outputPath).Msg("Dockerfile generated successfully.")
	return nil
}
