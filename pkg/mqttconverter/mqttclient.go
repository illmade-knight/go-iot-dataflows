package mqttconverter

import (
	"github.com/rs/zerolog/log"
	"os"
	"time"
)

// MQTTClientConfig holds configuration for the Paho MQTT client.
type MQTTClientConfig struct {
	BrokerURL          string        // e.g., "tls://mqtt.example.com:8883"
	Topic              string        // e.g., "devices/+/up"
	ClientIDPrefix     string        // A prefix for the client MessageID, a unique suffix will be added.
	Username           string        // MQTT username (from env)
	Password           string        // MQTT password (from env)
	KeepAlive          time.Duration // MQTT KeepAlive interval
	ConnectTimeout     time.Duration // Timeout for establishing connection
	ReconnectWaitMin   time.Duration // Minimum wait time before attempting reconnect
	ReconnectWaitMax   time.Duration // Maximum wait time before attempting reconnect
	CACertFile         string        // Optional: Path to CA certificate file for custom CA
	ClientCertFile     string        // Optional: Path to client certificate file for mTLS
	ClientKeyFile      string        // Optional: Path to client key file for mTLS
	InsecureSkipVerify bool          // Optional: Skip TLS verification (NOT recommended for production)
}

// LoadMQTTClientConfigFromEnv loads MQTT operational configuration from environment variables.
func LoadMQTTClientConfigFromEnv() *MQTTClientConfig {

	cfg := &MQTTClientConfig{
		KeepAlive:        60 * time.Second,  // Default
		ConnectTimeout:   10 * time.Second,  // Default
		ReconnectWaitMin: 1 * time.Second,   // Default
		ReconnectWaitMax: 120 * time.Second, // Default
		ClientIDPrefix:   "ingestion-service-",
	}
	if skipVerify := os.Getenv("MQTT_INSECURE_SKIP_VERIFY"); skipVerify == "true" {
		cfg.InsecureSkipVerify = true
	}

	// Parse durations if set in env, otherwise use defaults
	if ka := os.Getenv("MQTT_KEEP_ALIVE_SECONDS"); ka != "" {
		s, err := time.ParseDuration(ka + "s")
		if err == nil {
			cfg.KeepAlive = s
		} else {
			log.Printf("mqttconverter: error parsing keepAlive seconds: %s, using default", err)
		}
	}
	if ct := os.Getenv("MQTT_CONNECT_TIMEOUT_SECONDS"); ct != "" {
		s, err := time.ParseDuration(ct + "s")
		if err == nil {
			cfg.ConnectTimeout = s
		} else {
			log.Printf("mqttconverter: error parsing connect timeout seconds: %s, using default", err)
		}
	}

	return cfg
}
