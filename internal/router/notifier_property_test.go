// Package router provides an in-process write notification bus for query node cache invalidation and write visibility.
package router

import (
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// TestProperty_NotifierNonBlocking tests Property V2-10: Non-Blocking
// For any state of subscriber channels, Publish completes within 1ms
func TestProperty_NotifierNonBlocking(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("V2-10: Non-Blocking - Publish completes within 1ms", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(100)

			// Create multiple subscribers with various filter states
			numSubscribers := (seed % 10) + 1
			for i := 0; i < numSubscribers; i++ {
				filters := make([]string, 0)
				if i%2 == 0 {
					filters = []string{"prefix-"}
				}
				n.Subscribe("sub_"+string(rune('0'+i)), filters)
			}

			// Fill some subscriber channels to simulate full state
			for i := 0; i < numSubscribers; i++ {
				sub := n.Subscribe("sub_full_"+string(rune('0'+i)), nil)
				ch := sub.Ch
				// Fill the channel to capacity
				for j := 0; j < 100; j++ {
					select {
					case ch <- Notification{Type: PartitionCreated, PartitionKey: "fill"}:
					default:
						// Channel full
					}
				}
			}

			// Measure Publish duration
			start := time.Now()

			n.Publish(Notification{
				Type:         PartitionCreated,
				PartitionKey: "test-key",
				PartitionID:  "test-id",
				LSN:          1,
				Timestamp:    time.Now().UnixNano(),
			})

			elapsed := time.Since(start)

			// Publish should complete within 1ms
			if elapsed > 1*time.Millisecond {
				t.Errorf("Publish took %v, expected < 1ms", elapsed)
				return false
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_NotifierPublishNoSubscribers tests that Publish works with no subscribers
func TestProperty_NotifierPublishNoSubscribers(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("Notifier Publish No Subscribers - no panic, no block", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(100)

			// Should not panic and should not block
			done := make(chan bool)
			go func() {
				n.Publish(Notification{
					Type:         PartitionCreated,
					PartitionKey: "test-key",
					PartitionID:  "test-id",
					LSN:          1,
					Timestamp:    time.Now().UnixNano(),
				})
				done <- true
			}()

			select {
			case <-done:
				return true
			case <-time.After(100 * time.Millisecond):
				t.Error("Publish blocked")
				return false
			}
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_NotifierSubscribeReceives tests that subscribers receive notifications
func TestProperty_NotifierSubscribeReceives(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("Notifier Subscribe Receives - subscriber gets notification", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(100)
			sub := n.Subscribe("sub_"+string(rune('0'+(seed%10))), nil)
			ch := sub.Ch

			done := make(chan bool)
			go func() {
				notif := <-ch
				if notif.PartitionKey != "test-key" {
					t.Errorf("expected partition key 'test-key', got '%s'", notif.PartitionKey)
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
				return true
			case <-time.After(time.Second):
				t.Error("subscriber did not receive notification within timeout")
				return false
			}
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_NotifierFilterExcludes tests that filters exclude non-matching notifications
func TestProperty_NotifierFilterExcludes(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("Notifier Filter Excludes - non-matching keys filtered out", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(100)
			sub := n.Subscribe("sub_"+string(rune('0'+(seed%10))), []string{"prefix-"})
			ch := sub.Ch

			done := make(chan bool)
			go func() {
				select {
				case <-ch:
					t.Fatal("should not receive notification")
				case <-time.After(100 * time.Millisecond):
					// Expected - notification filtered out
					close(done)
				}
			}()

			n.Publish(Notification{
				Type:         PartitionCreated,
				PartitionKey: "other-key",
				PartitionID:  "test-id",
				LSN:          1,
				Timestamp:    time.Now().UnixNano(),
			})

			select {
			case <-done:
				return true
			case <-time.After(time.Second):
				t.Error("timeout waiting for filter to work")
				return false
			}
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_NotifierFilterIncludes tests that filters include matching notifications
func TestProperty_NotifierFilterIncludes(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("Notifier Filter Includes - matching keys received", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(100)
			sub := n.Subscribe("sub_"+string(rune('0'+(seed%10))), []string{"prefix-"})
			ch := sub.Ch

			done := make(chan bool)
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
				return true
			case <-time.After(time.Second):
				t.Error("subscriber did not receive notification within timeout")
				return false
			}
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_NotifierFullChannelDrops tests that full channels drop notifications without blocking
func TestProperty_NotifierFullChannelDrops(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("Notifier Full Channel Drops - no blocking on full channel", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(1) // Small buffer
			sub := n.Subscribe("sub_"+string(rune('0'+(seed%10))), nil)
			ch := sub.Ch

			// Fill the channel
			ch <- Notification{Type: PartitionCreated, PartitionKey: "fill"}

			// This should not block
			done := make(chan bool)
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
				t.Error("publish blocked when channel was full")
				return false
			}

			// Original notification should still be there
			select {
			case notif := <-ch:
				if notif.PartitionKey != "fill" {
					t.Errorf("expected 'fill', got '%s'", notif.PartitionKey)
					return false
				}
			case <-time.After(100 * time.Millisecond):
				t.Error("original notification was lost")
				return false
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_NotifierUnsubscribeCloses tests that unsubscribe closes the channel
func TestProperty_NotifierUnsubscribeCloses(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("Notifier Unsubscribe Closes - channel closed after unsubscribe", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(100)
			sub := n.Subscribe("sub_"+string(rune('0'+(seed%10))), nil)
			ch := sub.Ch

			n.Unsubscribe("sub_" + string(rune('0'+(seed%10))))

			// Channel should be closed
			select {
			case _, ok := <-ch:
				if ok {
					t.Fatal("channel should be closed after unsubscribe")
					return false
				}
				return true
			case <-time.After(100 * time.Millisecond):
				t.Fatal("channel was not closed within timeout")
				return false
			}
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_NotifierMultipleSubscribers tests that multiple subscribers work correctly
func TestProperty_NotifierMultipleSubscribers(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("Notifier Multiple Subscribers - all subscribers receive", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(100)
			sub1 := n.Subscribe("sub_"+string(rune('0'+(seed%10))), nil)
			ch1 := sub1.Ch
			sub2 := n.Subscribe("sub_"+string(rune('0'+((seed+1)%10))), []string{"prefix-"})
			ch2 := sub2.Ch

			// ch1 should receive both notifications (no filter)
			// ch2 should receive only "prefix-key" (has "prefix-" filter)

			done1 := make(chan bool)
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

			done2 := make(chan bool)
			go func() {
				notif := <-ch2
				if notif.PartitionKey != "prefix-key" {
					t.Errorf("ch2: expected 'prefix-key', got '%s'", notif.PartitionKey)
				}
				close(done2)
			}()

			// Give receivers time to start
			time.Sleep(10 * time.Millisecond)

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

			select {
			case <-done1:
				// Success
			case <-time.After(time.Second):
				t.Error("ch1 did not receive all notifications")
				return false
			}

			select {
			case <-done2:
				// Success
			case <-time.After(time.Second):
				t.Error("ch2 did not receive 'prefix-key' notification")
				return false
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_NotifierEmptyFilter tests that empty filter matches all
func TestProperty_NotifierEmptyFilter(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("Notifier Empty Filter - empty filter matches all", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(100)
			sub := n.Subscribe("sub_"+string(rune('0'+(seed%10))), []string{""})
			ch := sub.Ch

			done := make(chan bool)
			go func() {
				notif := <-ch
				if notif.PartitionKey != "test-key" {
					t.Errorf("expected 'test-key', got '%s'", notif.PartitionKey)
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
				return true
			case <-time.After(time.Second):
				t.Error("subscriber did not receive notification within timeout")
				return false
			}
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_NotifierNilFilter tests that nil filter matches all
func TestProperty_NotifierNilFilter(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("Notifier Nil Filter - nil filter matches all", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(100)
			sub := n.Subscribe("sub_"+string(rune('0'+(seed%10))), nil)
			ch := sub.Ch

			done := make(chan bool)
			go func() {
				notif := <-ch
				if notif.PartitionKey != "test-key" {
					t.Errorf("expected 'test-key', got '%s'", notif.PartitionKey)
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
				return true
			case <-time.After(time.Second):
				t.Error("subscriber did not receive notification within timeout")
				return false
			}
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_NotifierPrefixFilter tests prefix matching
func TestProperty_NotifierPrefixFilter(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("Notifier Prefix Filter - prefix matching works", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(100)
			sub := n.Subscribe("sub_"+string(rune('0'+(seed%10))), []string{"test-"})
			ch := sub.Ch

			done := make(chan bool)
			go func() {
				notif := <-ch
				if notif.PartitionKey != "test-key" {
					t.Errorf("expected 'test-key', got '%s'", notif.PartitionKey)
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
				return true
			case <-time.After(time.Second):
				t.Error("subscriber did not receive notification within timeout")
				return false
			}
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_NotifierNoMatchFilter tests that non-matching prefix doesn't receive
func TestProperty_NotifierNoMatchFilter(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("Notifier No Match Filter - non-matching prefix not received", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(100)
			sub := n.Subscribe("sub_"+string(rune('0'+(seed%10))), []string{"other-"})
			ch := sub.Ch

			done := make(chan bool)
			go func() {
				select {
				case <-ch:
					t.Fatal("should not receive notification")
				case <-time.After(100 * time.Millisecond):
					close(done)
				}
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
				return true
			case <-time.After(time.Second):
				t.Error("timeout waiting for filter to work")
				return false
			}
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_NotifierMultipleFilters tests multiple filters
func TestProperty_NotifierMultipleFilters(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("Notifier Multiple Filters - any matching filter works", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(100)
			sub := n.Subscribe("sub_"+string(rune('0'+(seed%10))), []string{"prefix1-", "prefix2-"})
			ch := sub.Ch

			done := make(chan bool)
			go func() {
				notif := <-ch
				if notif.PartitionKey != "prefix2-key" {
					t.Errorf("expected 'prefix2-key', got '%s'", notif.PartitionKey)
				}
				close(done)
			}()

			n.Publish(Notification{
				Type:         PartitionCreated,
				PartitionKey: "prefix2-key",
				PartitionID:  "test-id",
				LSN:          1,
				Timestamp:    time.Now().UnixNano(),
			})

			select {
			case <-done:
				return true
			case <-time.After(time.Second):
				t.Error("subscriber did not receive notification within timeout")
				return false
			}
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_NotifierConcurrentPublish tests concurrent publish safety
func TestProperty_NotifierConcurrentPublish(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("Notifier Concurrent Publish - no race conditions", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(100)

			// Create subscribers
			numSubscribers := 10
			for i := 0; i < numSubscribers; i++ {
				n.Subscribe("sub_"+string(rune('0'+i)), nil)
			}

			// Concurrently publish from multiple goroutines
			numGoroutines := 10
			notificationsPerGoroutine := 100

			done := make(chan bool, numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				go func(goroutineID int) {
					for j := 0; j < notificationsPerGoroutine; j++ {
						n.Publish(Notification{
							Type:         PartitionCreated,
							PartitionKey: "key_" + string(rune('0'+goroutineID)) + "_" + string(rune('0'+j)),
							PartitionID:  "id_" + string(rune('0'+goroutineID)) + "_" + string(rune('0'+j)),
							LSN:          uint64(goroutineID*1000 + j),
							Timestamp:    time.Now().UnixNano(),
						})
					}
					done <- true
				}(i)
			}

			for i := 0; i < numGoroutines; i++ {
				<-done
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}

// TestProperty_NotifierClose tests Close functionality
func TestProperty_NotifierClose(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	properties.Property("Notifier Close - all channels closed", prop.ForAll(
		func(seed int) bool {
			n := NewNotifier(100)

			// Create subscribers
			numSubscribers := 10
			channels := make([]chan Notification, numSubscribers)
			for i := 0; i < numSubscribers; i++ {
				sub := n.Subscribe("sub_"+string(rune('0'+i)), nil)
				channels[i] = sub.Ch
			}

			n.Close()

			// All channels should be closed
			for i := 0; i < numSubscribers; i++ {
				select {
				case _, ok := <-channels[i]:
					if ok {
						t.Errorf("channel %d should be closed", i)
						return false
					}
				case <-time.After(100 * time.Millisecond):
					t.Errorf("channel %d was not closed within timeout", i)
					return false
				}
			}

			return true
		},
		gen.Int(),
	))

	properties.TestingRun(t)
}
