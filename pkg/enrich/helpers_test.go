package enrich_test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// receiveSingleMessage helper (same as before)
func receiveSingleMessage(t *testing.T, ctx context.Context, sub *pubsub.Subscription, timeout time.Duration) *pubsub.Message {
	t.Helper()
	var receivedMsg *pubsub.Message
	var mu sync.RWMutex

	receiveCtx, receiveCancel := context.WithTimeout(ctx, timeout)
	defer receiveCancel()

	err := sub.Receive(receiveCtx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		defer mu.Unlock()
		if receivedMsg == nil {
			receivedMsg = msg
			msg.Ack()
			receiveCancel()
		} else {
			msg.Nack()
		}
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		t.Logf("Receive loop ended with error: %v", err)
	}

	mu.RLock()
	defer mu.RUnlock()
	return receivedMsg
}
