package mqttconverter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoadMQTTClientConfigFromEnv tests the LoadMQTTClientConfigFromEnv function.
func TestLoadMQTTClientConfigFromEnv(t *testing.T) {

	t.Run("SuccessfulLoad", func(t *testing.T) {
		t.Setenv("MQTT_KEEP_ALIVE_SECONDS", "30")
		t.Setenv("MQTT_CONNECT_TIMEOUT_SECONDS", "5")
		t.Setenv("MQTT_CA_CERT_FILE", "/path/to/ca.pem")
		t.Setenv("MQTT_CLIENT_CERT_FILE", "/path/to/client.crt")
		t.Setenv("MQTT_CLIENT_KEY_FILE", "/path/to/client.key")
		t.Setenv("MQTT_INSECURE_SKIP_VERIFY", "true")

		cfg := LoadMQTTClientConfigFromEnv()
		require.NotNil(t, cfg, "Config should not be nil")

		assert.Equal(t, 30*time.Second, cfg.KeepAlive)
		assert.Equal(t, 5*time.Second, cfg.ConnectTimeout)
		assert.Equal(t, 1*time.Second, cfg.ReconnectWaitMin)   // Default
		assert.Equal(t, 120*time.Second, cfg.ReconnectWaitMax) // Default
		assert.True(t, cfg.InsecureSkipVerify)

	})

	t.Run("DefaultClientIDPrefix", func(t *testing.T) {
		cfg := LoadMQTTClientConfigFromEnv()
		require.NotNil(t, cfg)
		assert.Equal(t, "ingestion-service-", cfg.ClientIDPrefix)
	})

	t.Run("DefaultDurations", func(t *testing.T) {

		cfg := LoadMQTTClientConfigFromEnv()
		require.NotNil(t, cfg)
		assert.Equal(t, 60*time.Second, cfg.KeepAlive)
		assert.Equal(t, 10*time.Second, cfg.ConnectTimeout)
	})

	t.Run("InvalidKeepAliveDuration", func(t *testing.T) {
		t.Setenv("MQTT_KEEP_ALIVE_SECONDS", "not-a-duration")

		cfg := LoadMQTTClientConfigFromEnv()
		require.NotNil(t, cfg)
		assert.Equal(t, 60*time.Second, cfg.KeepAlive, "KeepAlive should default if env var is invalid")
	})

	t.Run("InvalidConnectTimeoutDuration", func(t *testing.T) {
		t.Setenv("MQTT_CONNECT_TIMEOUT_SECONDS", "not-a-duration")

		cfg := LoadMQTTClientConfigFromEnv()
		require.NotNil(t, cfg)
		assert.Equal(t, 10*time.Second, cfg.ConnectTimeout, "ConnectTimeout should default if env var is invalid")
	})

	t.Run("OptionalFieldsNotSet", func(t *testing.T) {
		cfg := LoadMQTTClientConfigFromEnv()
		require.NotNil(t, cfg)

		assert.Empty(t, cfg.Username)
		assert.Empty(t, cfg.Password)
		assert.Empty(t, cfg.CACertFile)
		assert.Empty(t, cfg.ClientCertFile)
		assert.Empty(t, cfg.ClientKeyFile)
		assert.False(t, cfg.InsecureSkipVerify)
	})
}
