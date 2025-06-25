//go:build integration

package managedload_test

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"testing"
)

// populateFirestoreFromLoadgenDevices seeds Firestore with metadata for a given slice of load generator devices.
// It will intentionally skip seeding `omitCount` devices from the end of the list to test unenriched message paths.
// This is useful for testing scenarios where some devices generating telemetry do not have corresponding metadata for enrichment.
// It returns:
// - seededMetadata: A map of device ID to metadata for only the devices that were actually saved to Firestore, used for verification.
func populateFirestoreFromLoadgenDevices(t *testing.T, ctx context.Context, client *firestore.Client, collection string, devices []*loadgen.Device, omitCount int) (seededMetadata map[string]map[string]interface{}) {
	t.Helper()

	numDevices := len(devices)
	if omitCount >= numDevices {
		t.Fatalf("omitCount (%d) cannot be greater than or equal to the number of devices (%d)", omitCount, numDevices)
	}

	seededMetadata = make(map[string]map[string]interface{})
	numToSeed := numDevices - omitCount

	log.Info().Int("total_devices", numDevices).Int("omitted_devices", omitCount).Int("seeding_count", numToSeed).Msg("Populating Firestore from load generator device list...")

	// We seed metadata for the first `numToSeed` devices from the list.
	// The remaining `omitCount` devices will generate telemetry but have no metadata in Firestore.
	for i := 0; i < numToSeed; i++ {
		device := devices[i]
		docData := map[string]interface{}{
			"clientID":       fmt.Sprintf("client-for-%s", device.ID),
			"locationID":     fmt.Sprintf("location-for-%s", device.ID),
			"deviceCategory": "garden-sensor-pro-v2",
		}
		seededMetadata[device.ID] = docData
		_, err := client.Collection(collection).Doc(device.ID).Set(ctx, docData)
		require.NoError(t, err, "Failed to seed device metadata in Firestore for device %s", device.ID)
	}

	log.Info().Int("seeded_count", len(seededMetadata)).Msg("Completed populating Firestore.")
	return seededMetadata
}
