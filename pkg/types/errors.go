package types

import "errors"

// ULID-related errors
var (
	// ErrInvalidULIDLength is returned when a ULID string or byte slice has incorrect length
	ErrInvalidULIDLength = errors.New("invalid ULID length")

	// ErrInvalidULIDCharacter is returned when a ULID string contains invalid characters
	ErrInvalidULIDCharacter = errors.New("invalid ULID character")
)
