// Package router provides an in-process write notification bus for query node cache invalidation and write visibility.
package router

import (
	"sync"
	"time"
)

// NotificationType represents the type of notification.
type NotificationType int

const (
	PartitionCreated NotificationType = iota
	IndexCreated
	CompactionComplete
)

// Notification represents a write notification.
type Notification struct {
	Type         NotificationType
	PartitionKey string
	PartitionID  string
	LSN          uint64
	Timestamp    int64
}

// Notifier provides an in-process pub/sub notification bus for write visibility.
type Notifier struct {
	subscribers sync.Map
	bufferSize  int
}

// NewNotifier creates a new notifier instance.
func NewNotifier(bufferSize int) *Notifier {
	return &Notifier{
		bufferSize: bufferSize,
	}
}

// Publish sends a notification to all subscribers.
// Non-blocking: if a subscriber's channel is full, the notification is dropped.
func (n *Notifier) Publish(notif Notification) {
	n.subscribers.Range(func(key, value interface{}) bool {
		sub := value.(*Subscriber)
		if n.matchesFilter(sub, notif.PartitionKey) {
			select {
			case sub.Ch <- notif:
			default:
				// Channel full - drop notification, do NOT block
			}
		}
		return true
	})
}

// Subscribe adds a new subscriber to the notifier with a custom ID.
func (n *Notifier) Subscribe(id string, filters []string) *Subscriber {
	ch := make(chan Notification, n.bufferSize)
	sub := &Subscriber{
		ID:      id,
		Filters: filters,
		Ch:      ch,
	}
	n.subscribers.Store(sub.ID, sub)
	return sub
}

// SubscribeAutoID adds a new subscriber to the notifier with an auto-generated ID.
func (n *Notifier) SubscribeAutoID(filters ...string) chan Notification {
	id := generateSubscriberID()
	ch := make(chan Notification, n.bufferSize)
	sub := &Subscriber{
		ID:      id,
		Filters: filters,
		Ch:      ch,
	}
	n.subscribers.Store(sub.ID, sub)
	return ch
}

// Unsubscribe removes a subscriber from the notifier and closes their channel.
func (n *Notifier) Unsubscribe(subID string) {
	if value, ok := n.subscribers.LoadAndDelete(subID); ok {
		sub := value.(*Subscriber)
		close(sub.Ch)
	}
}

// matchesFilter checks if the notification matches the subscriber's filters.
func (n *Notifier) matchesFilter(sub *Subscriber, partitionKey string) bool {
	if len(sub.Filters) == 0 {
		return true // No filters - receive all notifications
	}
	for _, filter := range sub.Filters {
		if len(filter) == 0 {
			return true
		}
		if len(partitionKey) >= len(filter) && partitionKey[:len(filter)] == filter {
			return true
		}
	}
	return false
}

// Subscriber represents a notification subscriber.
type Subscriber struct {
	ID      string
	Filters []string
	Ch      chan Notification
}

// generateSubscriberID generates a unique subscriber ID.
func generateSubscriberID() string {
	return "sub_" + time.Now().Format("20060102150405000000")
}
