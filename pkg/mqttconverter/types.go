package mqttconverter

import "time"

type InMessage struct {
	Payload   []byte    `json:"payload"`
	Topic     string    `json:"topic"`
	MessageID string    `json:"message_id"`
	Timestamp time.Time `json:"timestamp"`
	Duplicate bool      `json:"duplicate"`
}
