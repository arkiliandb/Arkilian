// Package router provides property-based tests for notification non-blocking behavior.
package router

import (
	"reflect"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

// Validates: Requirements 11.4
func TestNotifier_Properties(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	parameters.MinSuccessfulTests = 100

	properties := gopter.NewProperties(parameters)

	// Property V2-10: Non-Blocking
	// For any state of subscriber channels, Publish completes within 1ms
	properties.Property("Non-Blocking - Publish completes within 1ms regardless of channel state", prop.ForAll(
		func(testData notifierTestData) bool {
			// Skip invalid data
			if testData.bufferSize <= 0 || testData.subscriberCount <= 0 || testData.publishCount <= 0 || len(testData.partitionKeys) == 0 {
				return true
			}

			// Create notifier with small buffer to test blocking scenarios
			n := NewNotifier(testData.bufferSize)
			defer func() {
				// Cleanup subscribers
				n.subscribers.Range(func(key, value interface{}) bool {
					sub := value.(*Subscriber)
					close(sub.Ch)
					return true
				})
			}()

			// Create subscribers with various buffer sizes
			subscribers := make([]*Subscriber, 0, testData.subscriberCount)
			for i := 0; i < testData.subscriberCount; i++ {
				subBuffer := testData.subscriberBufferSizes[i%len(testData.subscriberBufferSizes)]
				sub := n.Subscribe("sub-"+string(rune('A'+i)), testData.filters)
				// Resize channel to test different buffer states
				sub.Ch = make(chan Notification, subBuffer)
				subscribers = append(subscribers, sub)
			}

			// Fill some channels to capacity to test non-blocking
			if testData.fillChannels {
				for i, sub := range subscribers {
					if i%2 == 0 {
						for j := 0; j < cap(sub.Ch); j++ {
							sub.Ch <- Notification{
								Type:         PartitionCreated,
								PartitionKey: "test-key",
								PartitionID:  "test-id",
								LSN:          uint64(j),
								Timestamp:    time.Now().UnixNano(),
							}
						}
					}
				}
			}

			// Time the publish operation
			start := time.Now()
			for i := 0; i < testData.publishCount; i++ {
				n.Publish(Notification{
					Type:         PartitionCreated,
					PartitionKey: testData.partitionKeys[i%len(testData.partitionKeys)],
					PartitionID:  "test-id-" + string(rune('0'+i%10)),
					LSN:          uint64(i),
					Timestamp:    time.Now().UnixNano(),
				})
			}
			elapsed := time.Since(start)

			// Publish should complete within 1ms per notification
			maxDuration := time.Duration(testData.publishCount) * time.Millisecond
			if elapsed > maxDuration {
				return false
			}

			return true
		},
		genNotifierTestData(),
	))

	// Additional property: Non-blocking with many subscribers
	properties.Property("Non-Blocking - Publish to many subscribers completes quickly", prop.ForAll(
		func(testData manySubscribersTestData) bool {
			n := NewNotifier(testData.bufferSize)
			defer func() {
				n.subscribers.Range(func(key, value interface{}) bool {
					if sub, ok := value.(*Subscriber); ok {
						close(sub.Ch)
					}
					return true
				})
			}()

			// Create many subscribers
			for i := 0; i < testData.subscriberCount; i++ {
				filters := []string{}
				if i%3 == 0 {
					filters = []string{"prefix-" + string(rune('A'+i%26))}
				}
				n.Subscribe("sub-"+string(rune('A'+i%26))+string(rune('0'+i%10)), filters)
			}

			// Time publish with many subscribers
			start := time.Now()
			n.Publish(Notification{
				Type:         PartitionCreated,
				PartitionKey: "prefix-A0",
				PartitionID:  "test-id",
				LSN:          1,
				Timestamp:    time.Now().UnixNano(),
			})
			elapsed := time.Since(start)

			// Should complete within 10ms even with many subscribers
			if elapsed > 10*time.Millisecond {
				return false
			}

			return true
		},
		genManySubscribersTestData(),
	))

	properties.TestingRun(t)
}

// Test data structures
type notifierTestData struct {
	bufferSize            int
	subscriberCount       int
	subscriberBufferSizes []int
	fillChannels          bool
	publishCount          int
	partitionKeys         []string
	filters               []string
}

type manySubscribersTestData struct {
	bufferSize      int
	subscriberCount int
}

// Generators
func genNotifierTestData() gopter.Gen {
	return gen.Struct(
		reflect.TypeOf(notifierTestData{}),
		map[string]gopter.Gen{
			"bufferSize": gen.IntRange(1, 100),
			"subscriberCount": gen.IntRange(1, 50),
			"subscriberBufferSizes": gen.Const([]int{0, 1, 5, 10, 100}),
			"fillChannels": gen.Bool(),
			"publishCount": gen.IntRange(1, 20),
			"partitionKeys": gen.SliceOf(
				gen.AlphaString(),
				reflect.TypeOf(""),
			).SuchThat(func(v interface{}) bool {
				return len(v.([]string)) >= 1 && len(v.([]string)) <= 5
			}),
			"filters": gen.Const([]string{""}),
		},
	)
}

func genManySubscribersTestData() gopter.Gen {
	return gen.Struct(
		reflect.TypeOf(manySubscribersTestData{}),
		map[string]gopter.Gen{
			"bufferSize":      gen.IntRange(1, 1000),
			"subscriberCount": gen.IntRange(10, 100),
		},
	)
}