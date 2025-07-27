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
		t.Setenv("MQTT_BROKER_URL", "tcp://localhost:1883")
		t.Setenv("MQTT_TOPIC", "test/topic")
		t.Setenv("MQTT_CLIENT_ID_PREFIX", "test-client")
		t.Setenv("MQTT_USERNAME", "user")
		t.Setenv("MQTT_PASSWORD", "pass")
		t.Setenv("MQTT_KEEP_ALIVE_SECONDS", "30")
		t.Setenv("MQTT_CONNECT_TIMEOUT_SECONDS", "5")
		t.Setenv("MQTT_CA_CERT_FILE", "/path/to/ca.pem")
		t.Setenv("MQTT_CLIENT_CERT_FILE", "/path/to/client.crt")
		t.Setenv("MQTT_CLIENT_KEY_FILE", "/path/to/client.key")
		t.Setenv("MQTT_INSECURE_SKIP_VERIFY", "true")

		cfg, err := LoadMQTTClientConfigFromEnv()
		require.NoError(t, err, "LoadMQTTClientConfigFromEnv should not return an error with all env vars set")
		require.NotNil(t, cfg, "Config should not be nil")

		assert.Equal(t, "tcp://localhost:1883", cfg.BrokerURL)
		assert.Equal(t, "test/topic", cfg.Topic)
		assert.Equal(t, "test-client", cfg.ClientIDPrefix)
		assert.Equal(t, "user", cfg.Username)
		assert.Equal(t, "pass", cfg.Password)
		assert.Equal(t, 30*time.Second, cfg.KeepAlive)
		assert.Equal(t, 5*time.Second, cfg.ConnectTimeout)
		assert.Equal(t, 1*time.Second, cfg.ReconnectWaitMin)   // Default
		assert.Equal(t, 120*time.Second, cfg.ReconnectWaitMax) // Default
		assert.Equal(t, "/path/to/ca.pem", cfg.CACertFile)
		assert.Equal(t, "/path/to/client.crt", cfg.ClientCertFile)
		assert.Equal(t, "/path/to/client.key", cfg.ClientKeyFile)
		assert.True(t, cfg.InsecureSkipVerify)

	})

	t.Run("MissingBrokerURL", func(t *testing.T) {
		t.Setenv("MQTT_TOPIC", "test/topic") // Set one required var

		_, err := LoadMQTTClientConfigFromEnv()
		require.Error(t, err, "LoadMQTTClientConfigFromEnv should return an error if MQTT_BROKER_URL is not set")
		// we now return config even on error so we can debug its state
		//assert.Nil(t, cfg, "Config should be nil on error")
		assert.Contains(t, err.Error(), "MQTT_BROKER_URL environment variable not set")
	})

	t.Run("MissingTopic", func(t *testing.T) {
		t.Setenv("MQTT_BROKER_URL", "tcp://localhost:1883")

		_, err := LoadMQTTClientConfigFromEnv()
		require.Error(t, err, "LoadMQTTClientConfigFromEnv should return an error if MQTT_TOPIC is not set")
		// we now return config even on error so we can debug its state
		//assert.Nil(t, cfg, "Config should be nil on error")
		assert.Contains(t, err.Error(), "MQTT_TOPIC environment variable not set")
	})

	t.Run("DefaultClientIDPrefix", func(t *testing.T) {
		t.Setenv("MQTT_BROKER_URL", "tcp://localhost:1883")
		t.Setenv("MQTT_TOPIC", "test/topic")

		cfg, err := LoadMQTTClientConfigFromEnv()
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, "ingestion-service-", cfg.ClientIDPrefix)
	})

	t.Run("DefaultDurations", func(t *testing.T) {
		t.Setenv("MQTT_BROKER_URL", "tcp://localhost:1883")
		t.Setenv("MQTT_TOPIC", "test/topic")

		cfg, err := LoadMQTTClientConfigFromEnv()
		require.NoError(t, err)
		require.NotNil(t, cfg)
		assert.Equal(t, 60*time.Second, cfg.KeepAlive)
		assert.Equal(t, 10*time.Second, cfg.ConnectTimeout)
	})

	t.Run("InvalidKeepAliveDuration", func(t *testing.T) {
		t.Setenv("MQTT_BROKER_URL", "tcp://localhost:1883")
		t.Setenv("MQTT_TOPIC", "test/topic")
		t.Setenv("MQTT_KEEP_ALIVE_SECONDS", "not-a-duration")

		cfg, err := LoadMQTTClientConfigFromEnv()
		require.NoError(t, err) // Should not error, uses default
		require.NotNil(t, cfg)
		assert.Equal(t, 60*time.Second, cfg.KeepAlive, "KeepAlive should default if env var is invalid")
	})

	t.Run("InvalidConnectTimeoutDuration", func(t *testing.T) {
		t.Setenv("MQTT_BROKER_URL", "tcp://localhost:1883")
		t.Setenv("MQTT_TOPIC", "test/topic")
		t.Setenv("MQTT_CONNECT_TIMEOUT_SECONDS", "not-a-duration")

		cfg, err := LoadMQTTClientConfigFromEnv()
		require.NoError(t, err) // Should not error, uses default
		require.NotNil(t, cfg)
		assert.Equal(t, 10*time.Second, cfg.ConnectTimeout, "ConnectTimeout should default if env var is invalid")
	})

	t.Run("OptionalFieldsNotSet", func(t *testing.T) {
		t.Setenv("MQTT_BROKER_URL", "tcp://localhost:1883")
		t.Setenv("MQTT_TOPIC", "test/topic")

		cfg, err := LoadMQTTClientConfigFromEnv()
		require.NoError(t, err)
		require.NotNil(t, cfg)

		assert.Empty(t, cfg.Username)
		assert.Empty(t, cfg.Password)
		assert.Empty(t, cfg.CACertFile)
		assert.Empty(t, cfg.ClientCertFile)
		assert.Empty(t, cfg.ClientKeyFile)
		assert.False(t, cfg.InsecureSkipVerify)
	})
}
