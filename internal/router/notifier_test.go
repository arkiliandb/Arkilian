package router

import (
	"testing"
	"time"
)

func TestNotifier_PublishNoSubscribers(t *testing.T) {
	n := NewNotifier(100)
	// Should not panic and should not block
	n.Publish(Notification{
		Type:         PartitionCreated,
		PartitionKey: "test-key",
		PartitionID:  "test-id",
		LSN:          1,
		Timestamp:    time.Now().UnixNano(),
	})
}

func TestNotifier_SubscribeReceivesNotification(t *testing.T) {
	n := NewNotifier(100)
	sub := n.Subscribe("sub-1", nil)
	ch := sub.Ch

	done := make(chan struct{})
	go func() {
		notif := <-ch
		if notif.PartitionKey != "test-key" {
			t.Errorf("expected partition key 'test-key', got '%s'", notif.PartitionKey)
		}
		if notif.Type != PartitionCreated {
			t.Errorf("expected type PartitionCreated, got %v", notif.Type)
		}
		close(done)
	}()

	n.Publish(Notification{
		Type:         PartitionCreated,
		PartitionKey: "test-key",
		PartitionID:  "test-id",
		LSN:          1,
		Timestamp:    time.Now().UnixNano(),
	})

	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive notification within timeout")
	}
}

func TestNotifier_FilterExcludesNonMatching(t *testing.T) {
	n := NewNotifier(100)
	// Subscribe with filter for "prefix-"
	sub := n.Subscribe("sub-2", []string{"prefix-"})
	ch := sub.Ch

	// Publish notification with different key
	n.Publish(Notification{
		Type:         PartitionCreated,
		PartitionKey: "other-key",
		PartitionID:  "test-id",
		LSN:          1,
		Timestamp:    time.Now().UnixNano(),
	})

	// Should not receive the notification
	select {
	case notif := <-ch:
		t.Fatalf("received unexpected notification: %v", notif)
	case <-time.After(100 * time.Millisecond):
		// Expected - notification filtered out
	}
}

func TestNotifier_FilterIncludesMatching(t *testing.T) {
	n := NewNotifier(100)
	// Subscribe with filter for "prefix-"
	sub := n.Subscribe("sub-3", []string{"prefix-"})
	ch := sub.Ch

	done := make(chan struct{})
	go func() {
		notif := <-ch
		if notif.PartitionKey != "prefix-test" {
			t.Errorf("expected 'prefix-test', got '%s'", notif.PartitionKey)
		}
		close(done)
	}()

	n.Publish(Notification{
		Type:         PartitionCreated,
		PartitionKey: "prefix-test",
		PartitionID:  "test-id",
		LSN:          1,
		Timestamp:    time.Now().UnixNano(),
	})

	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive notification within timeout")
	}
}

func TestNotifier_FullChannelDropsNotification(t *testing.T) {
	n := NewNotifier(1) // Small buffer
	sub := n.Subscribe("sub-4", nil)
	ch := sub.Ch

	// Fill the channel
	ch <- Notification{Type: PartitionCreated, PartitionKey: "fill"}

	// This should not block - notification should be dropped
	done := make(chan struct{})
	go func() {
		n.Publish(Notification{
			Type:         PartitionCreated,
			PartitionKey: "test-key",
			PartitionID:  "test-id",
			LSN:          1,
			Timestamp:    time.Now().UnixNano(),
		})
		close(done)
	}()

	select {
	case <-done:
		// Success - publish returned without blocking
	case <-time.After(100 * time.Millisecond):
		t.Fatal("publish blocked when channel was full")
	}

	// Original notification should still be there
	select {
	case notif := <-ch:
		if notif.PartitionKey != "fill" {
			t.Errorf("expected 'fill', got '%s'", notif.PartitionKey)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("original notification was lost")
	}
}

func TestNotifier_UnsubscribeClosesChannel(t *testing.T) {
	n := NewNotifier(100)
	sub := n.Subscribe("test-sub", nil)
	ch := sub.Ch

	n.Unsubscribe("test-sub")

	// Channel should be closed
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel should be closed after unsubscribe")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("channel was not closed within timeout")
	}
}

func TestNotifier_MultipleSubscribers(t *testing.T) {
	n := NewNotifier(100)
	sub1 := n.Subscribe("sub-1", nil)
	ch1 := sub1.Ch
	sub2 := n.Subscribe("sub-2", []string{"prefix-"})
	ch2 := sub2.Ch

	// ch1 should receive both notifications (no filter)
	// ch2 should receive only "prefix-key" (has "prefix-" filter)

	// Start receivers
	done1 := make(chan struct{})
	go func() {
		count := 0
		for range ch1 {
			count++
			if count == 2 {
				close(done1)
				return
			}
		}
	}()

	done2 := make(chan struct{})
	go func() {
		notif := <-ch2
		if notif.PartitionKey != "prefix-key" {
			t.Errorf("ch2: expected 'prefix-key', got '%s'", notif.PartitionKey)
		}
		close(done2)
	}()

	// Give receivers time to start
	time.Sleep(10 * time.Millisecond)

	// Publish notifications
	n.Publish(Notification{
		Type:         PartitionCreated,
		PartitionKey: "any-key",
		PartitionID:  "id1",
		LSN:          1,
		Timestamp:    time.Now().UnixNano(),
	})

	n.Publish(Notification{
		Type:         PartitionCreated,
		PartitionKey: "prefix-key",
		PartitionID:  "id2",
		LSN:          2,
		Timestamp:    time.Now().UnixNano(),
	})

	// Wait for ch1 to receive both notifications
	select {
	case <-done1:
		// Success
	case <-time.After(time.Second):
		t.Fatal("ch1 did not receive all notifications")
	}

	// Wait for ch2 to receive "prefix-key"
	select {
	case <-done2:
		// Success
	case <-time.After(time.Second):
		t.Fatal("ch2 did not receive 'prefix-key' notification")
	}
}