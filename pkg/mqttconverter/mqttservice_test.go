// mqttconverter/mqttservice_test.go
package mqttconverter

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/illmade-knight/go-dataflow/pkg/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mocks for the new Generic Service ---

// MockPayload is a sample struct for testing the generic service.
type MockPayload struct {
	Data string `json:"data"`
}

// MockProcessor is a mock implementation of the generic MessageProcessor interface.
type MockProcessor[T any] struct {
	mu               sync.RWMutex
	ReceivedMessages []*types.BatchedMessage[T]
	stopCalled       bool
	inputChan        chan *types.BatchedMessage[T]
}

func NewMockProcessor[T any](chanSize int) *MockProcessor[T] {
	return &MockProcessor[T]{
		ReceivedMessages: make([]*types.BatchedMessage[T], 0),
		inputChan:        make(chan *types.BatchedMessage[T], chanSize),
	}
}

func (m *MockProcessor[T]) Input() chan<- *types.BatchedMessage[T] {
	return m.inputChan
}

func (m *MockProcessor[T]) Start(ctx context.Context) {
	// In the mock, we just need to drain the input channel to collect messages.
	go func() {
		for msg := range m.inputChan {
			m.mu.Lock()
			m.ReceivedMessages = append(m.ReceivedMessages, msg)
			m.mu.Unlock()
		}
	}()
}

func (m *MockProcessor[T]) Stop() {
	m.mu.Lock()
	m.stopCalled = true
	m.mu.Unlock()
	close(m.inputChan)
}

func (m *MockProcessor[T]) GetMessageCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.ReceivedMessages)
}

// --- Tests ---

// setupTestService is a helper to initialize the generic service with mocks.
func setupTestService[T any](
	t *testing.T,
	transformer func(InMessage) (*T, bool, error),
) (*IngestionService[T], *MockProcessor[T]) {
	t.Helper()

	cfg := DefaultIngestionServiceConfig()
	logger := zerolog.Nop()
	processor := NewMockProcessor[T](cfg.InputChanCapacity)

	service := NewIngestionService[T](processor, transformer, logger, cfg, MQTTClientConfig{})
	require.NotNil(t, service, "NewIngestionService should not return nil")

	return service, processor
}

// TestProcessSingleMessage ensures the core logic with a transformer works correctly.
func TestProcessSingleMessage(t *testing.T) {
	t.Run("With Transformer Success", func(t *testing.T) {
		// Arrange
		transformer := func(msg InMessage) (*MockPayload, bool, error) {
			return &MockPayload{Data: "transformed"}, false, nil
		}
		service, processor := setupTestService(t, transformer)
		msg := InMessage{Payload: []byte(`{}`), Topic: "test/topic"}

		// Act
		service.processSingleMessage(context.Background(), msg, 1)

		// Assert
		require.Equal(t, 1, processor.GetMessageCount(), "Processor should have received one message")
		publishedMsg := processor.ReceivedMessages[0]
		assert.Equal(t, "transformed", publishedMsg.Payload.Data)
		assert.Equal(t, "test/topic", publishedMsg.OriginalMessage.Attributes["mqtt_topic"])
	})

	t.Run("With Transformer Skip", func(t *testing.T) {
		// Arrange
		transformer := func(msg InMessage) (*MockPayload, bool, error) {
			return nil, true, nil // Signal to skip
		}
		service, processor := setupTestService(t, transformer)
		msg := InMessage{Payload: []byte(`{}`), Topic: "test/topic"}

		// Act
		service.processSingleMessage(context.Background(), msg, 1)

		// Assert
		assert.Equal(t, 0, processor.GetMessageCount(), "Processor should not receive a skipped message")
	})

	t.Run("With Transformer Error", func(t *testing.T) {
		// Arrange
		transformer := func(msg InMessage) (*MockPayload, bool, error) {
			return nil, false, fmt.Errorf("transformation failed")
		}
		service, processor := setupTestService(t, transformer)
		msg := InMessage{Payload: []byte(`{}`), Topic: "test/topic"}

		// Act
		service.processSingleMessage(context.Background(), msg, 1)

		// Assert
		assert.Equal(t, 0, processor.GetMessageCount(), "Processor should not receive a message that failed transformation")
		select {
		case err := <-service.Err():
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "transformation failed")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Expected an error on the ErrorChan but got none")
		}
	})
}

// TestService_InternalPipeline simulates the full internal pipeline from message arrival to processing.
func TestService_InternalPipeline(t *testing.T) {
	// Arrange
	transformer := func(msg InMessage) (*MockPayload, bool, error) {
		return &MockPayload{Data: "e2e-transformed"}, false, nil
	}
	service, processor := setupTestService(t, transformer)

	err := service.Start()
	require.NoError(t, err)
	defer service.Stop()

	// Act
	msg := InMessage{Payload: []byte(`{}`), Topic: "e2e/topic"}
	service.MessagesChan <- msg

	// Assert
	require.Eventually(t, func() bool {
		return processor.GetMessageCount() == 1
	}, 1*time.Second, 10*time.Millisecond, "Expected one message to be processed")

	publishedMsg := processor.ReceivedMessages[0]
	assert.Equal(t, "e2e-transformed", publishedMsg.Payload.Data)
	assert.True(t, processor.stopCalled, "Processor's Stop method should be called during service shutdown")
}

// TestService_Stop ensures that the service shuts down gracefully.
func TestService_Stop(t *testing.T) {
	// Arrange
	transformer := func(msg InMessage) (*MockPayload, bool, error) {
		return &MockPayload{Data: string(msg.Payload)}, false, nil
	}
	service, processor := setupTestService(t, transformer)
	err := service.Start()
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		service.MessagesChan <- InMessage{Payload: []byte(fmt.Sprintf("msg-%d", i))}
	}

	// Act
	service.Stop()

	// Assert
	assert.Panics(t, func() {
		service.MessagesChan <- InMessage{Payload: []byte("after-stop")}
	}, "Sending to a closed channel should panic")

	assert.Equal(t, 5, processor.GetMessageCount(), "All buffered messages should be processed on shutdown")
	assert.True(t, processor.stopCalled, "Processor's Stop method should be called")
}
