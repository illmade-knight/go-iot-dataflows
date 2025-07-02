package main

import (
	"encoding/json"
	"fmt"
)

// DeviceIDExtractor implements the mqttconverter.AttributeExtractor interface for our TestPayload.
type DeviceIDExtractor struct{}

// NewDeviceIDExtractor creates a new extractor.
func NewDeviceIDExtractor() *DeviceIDExtractor {
	return &DeviceIDExtractor{}
}

// Extract parses the JSON payload to get the device_id and returns it in a
// map with the key "uid", which is what the enrichment service's consumer expects.
func (e *DeviceIDExtractor) Extract(payload []byte) (map[string]string, error) {
	var tempPayload struct {
		DeviceID string `json:"device_id"`
	}

	if err := json.Unmarshal(payload, &tempPayload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload to extract device_id: %w", err)
	}

	if tempPayload.DeviceID == "" {
		// Returning an error here would stop the message. Returning nil attributes
		// allows it to continue without enrichment, which may be desired.
		// For our case, we expect it, so an error is appropriate if missing.
		return nil, fmt.Errorf("device_id field is empty or missing in payload")
	}

	attributes := map[string]string{
		"uid": tempPayload.DeviceID,
	}

	return attributes, nil
}
