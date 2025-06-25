//go:build integration

package loade2e_test

import (
	"encoding/json"
	"github.com/illmade-knight/go-iot/helpers/loadgen"
	"github.com/illmade-knight/go-iot/pkg/types"
	"math/rand"
	"sync"
	"time"
)

// gardenMonitorPayloadGenerator is a stateful payload generator.
// It implements the loadgen.PayloadGenerator interface and maintains
// the state of a device's telemetry between calls.
type gardenMonitorPayloadGenerator struct {
	mu               sync.Mutex // Protects the fields below, ensuring thread safety.
	isInitialized    bool
	sequence         *loadgen.Sequence
	lastBattery      int
	lastTemp         int
	lastHumidity     int
	lastMoisture     int
	lastWaterFlow    int
	lastWaterQuality int
	lastTankLevel    int
	lastAmbientLight int
}

// GeneratePayload creates a new GardenMonitorReadings based on the previous state.
func (g *gardenMonitorPayloadGenerator) GeneratePayload(device *loadgen.Device) ([]byte, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Lazy initialization on the first run.
	if !g.isInitialized {
		g.sequence = loadgen.NewSequence(0)
		g.lastBattery = 100
		g.lastTemp = 15 + rand.Intn(10)
		g.lastHumidity = 40 + rand.Intn(20)
		g.lastMoisture = 30 + rand.Intn(20)
		g.lastWaterFlow = 0
		g.lastWaterQuality = 95 + rand.Intn(5)
		g.lastTankLevel = 80 + rand.Intn(20)
		g.lastAmbientLight = 500 + rand.Intn(200)
		g.isInitialized = true
	}

	if g.lastBattery > 0 {
		g.lastBattery--
	}
	g.lastTemp += rand.Intn(3) - 1
	g.lastHumidity += rand.Intn(5) - 2
	g.lastMoisture += rand.Intn(3) - 1
	g.lastTankLevel--
	if g.lastTankLevel < 10 {
		g.lastTankLevel = 100
	}
	if rand.Float32() < 0.1 {
		g.lastWaterFlow = rand.Intn(50)
	} else {
		g.lastWaterFlow = 0
	}

	payload := types.GardenMonitorReadings{
		DE:           device.ID,
		SIM:          "sim-8675309",
		RSSI:         "-75dBm",
		Version:      "1.1.0",
		Sequence:     int(g.sequence.Next()),
		Battery:      g.lastBattery,
		Temperature:  g.lastTemp,
		Humidity:     g.lastHumidity,
		SoilMoisture: g.lastMoisture,
		WaterFlow:    g.lastWaterFlow,
		WaterQuality: g.lastWaterQuality,
		TankLevel:    g.lastTankLevel,
		AmbientLight: g.lastAmbientLight,
		Timestamp:    time.Now().UTC(),
	}

	return json.Marshal(payload)
}
