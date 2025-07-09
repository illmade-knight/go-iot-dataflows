package servicedirector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/rs/zerolog"
)

// Client communicates with the Director API.
type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
	logger     zerolog.Logger
}

// NewClient creates a new client for the Director.
func NewClient(baseURL string, logger zerolog.Logger) (*Client, error) {
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse service director base URL '%s': %w", baseURL, err)
	}

	return &Client{
		baseURL: parsedURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: logger.With().Str("component", "DirectorClient").Str("director_url", baseURL).Logger(),
	}, nil
}

// VerifyDataflow contacts the Director to ensure all resources for a given
// dataflow are created and available.
func (c *Client) VerifyDataflow(ctx context.Context, dataflowName, serviceName string) error {
	c.logger.Info().Str("dataflow", dataflowName).Str("service", serviceName).Msg("Verifying dataflow resources with Director...")

	relURL := &url.URL{Path: "/verify/dataflow"}
	fullURL := c.baseURL.ResolveReference(relURL)

	reqBody, err := json.Marshal(VerifyDataflowRequest{
		DataflowName: dataflowName,
		ServiceName:  serviceName,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal verification request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL.String(), bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create verification request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send verification request to Director: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Director verification failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	c.logger.Info().Str("dataflow", dataflowName).Msg("Dataflow resources verified successfully.")
	return nil
}
