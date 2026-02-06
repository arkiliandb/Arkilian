// Package types provides core data types for Project Arkilian.
package types

// Row represents a single data row in the events table.
type Row struct {
	// EventID is the ULID primary key for the event (time-ordered UUID)
	EventID []byte `json:"event_id"`

	// TenantID identifies the tenant this event belongs to
	TenantID string `json:"tenant_id"`

	// UserID identifies the user who triggered the event
	UserID int64 `json:"user_id"`

	// EventTime is the Unix timestamp (nanoseconds) when the event occurred
	EventTime int64 `json:"event_time"`

	// EventType categorizes the event (e.g., "page_view", "purchase")
	EventType string `json:"event_type"`

	// Payload contains the event-specific data, compressed with Snappy
	Payload map[string]interface{} `json:"payload"`
}
