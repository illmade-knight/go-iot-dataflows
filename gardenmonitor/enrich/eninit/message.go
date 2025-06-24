package eninit

import (
	"encoding/json"
	"fmt"

	"github.com/illmade-knight/go-iot/pkg/device"
	"github.com/illmade-knight/go-iot/pkg/messagepipeline"
	"github.com/illmade-knight/go-iot/pkg/types"
	"github.com/rs/zerolog"
	"time"
)

type TestEnrichedMessage struct {
	OriginalPayload json.RawMessage   `json:"originalPayload"`
	OriginalInfo    *types.DeviceInfo `json:"originalInfo"`
	PublishTime     time.Time         `json:"publishTime"`
	ClientID        string            `json:"clientID,omitempty"`
	LocationID      string            `json:"locationID,omitempty"`
	Category        string            `json:"category,omitempty"`
}

// NewTestMessageEnricher creates the MessageTransformer for the test.
// It correctly separates the data payload from the message's Ack/Nack functions.
func NewTestMessageEnricher(fetcher device.DeviceMetadataFetcher, logger zerolog.Logger) messagepipeline.MessageTransformer[TestEnrichedMessage] {
	return func(msg types.ConsumedMessage) (*TestEnrichedMessage, bool, error) {
		// Start with the basic, un-enriched data.
		payload := &TestEnrichedMessage{
			OriginalPayload: msg.Payload,
			OriginalInfo:    msg.DeviceInfo,
			PublishTime:     msg.PublishTime,
		}

		// If there's no device info, we can't enrich, but we still pass the message on.
		if msg.DeviceInfo == nil || msg.DeviceInfo.UID == "" {
			logger.Warn().Str("msg_id", msg.ID).Msg("Message has no device UID for enrichment, skipping lookup.")
			return payload, false, nil
		}

		// Attempt to fetch metadata for enrichment.
		deviceEUI := msg.DeviceInfo.UID
		clientID, locationID, category, err := fetcher(deviceEUI)
		if err != nil {
			logger.Error().Err(err).Str("device_eui", deviceEUI).Str("msg_id", msg.ID).Msg("Failed to fetch device metadata for enrichment.")
			// We NACK by returning an error, which stops this message from being published.
			return nil, false, fmt.Errorf("failed to enrich message for EUI %s: %w", deviceEUI, err)
		}

		// If successful, add the enriched data to the payload.
		payload.ClientID = clientID
		payload.LocationID = locationID
		payload.Category = category

		logger.Debug().Str("msg_id", msg.ID).Str("device_eui", deviceEUI).Msg("Message enriched with device metadata.")
		return payload, false, nil
	}
}
