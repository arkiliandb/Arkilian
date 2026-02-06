// Package errors provides structured error types for the Arkilian system.
// All errors include a category, code, message, and retryable flag for
// consistent error handling across components.
package errors

import (
	"errors"
	"fmt"
)

// ErrorCategory classifies errors by system component.
type ErrorCategory string

const (
	ErrCategoryValidation ErrorCategory = "VALIDATION"
	ErrCategoryStorage    ErrorCategory = "STORAGE"
	ErrCategoryManifest   ErrorCategory = "MANIFEST"
	ErrCategoryQuery      ErrorCategory = "QUERY"
	ErrCategoryCompaction ErrorCategory = "COMPACTION"
	ErrCategoryInternal   ErrorCategory = "INTERNAL"
)

// Error codes for each category.
const (
	// Validation codes
	CodeInvalidSchema       = "INVALID_SCHEMA"
	CodeInvalidPartitionKey = "INVALID_PARTITION_KEY"
	CodeEmptyBatch          = "EMPTY_BATCH"

	// Storage codes
	CodeUploadFailed   = "UPLOAD_FAILED"
	CodeDownloadFailed = "DOWNLOAD_FAILED"
	CodeObjectNotFound = "OBJECT_NOT_FOUND"

	// Manifest codes
	CodeWriteConflict       = "WRITE_CONFLICT"
	CodeCorruptionDetected  = "CORRUPTION_DETECTED"
	CodePartitionNotFound   = "PARTITION_NOT_FOUND"

	// Query codes
	CodeParseError        = "PARSE_ERROR"
	CodeUnsupportedSyntax = "UNSUPPORTED_SYNTAX"
	CodeExecutionTimeout  = "EXECUTION_TIMEOUT"

	// Compaction codes
	CodeValidationFailed = "VALIDATION_FAILED"
	CodeSourceMissing    = "SOURCE_MISSING"

	// Internal codes
	CodeUnexpected = "UNEXPECTED"
)

// ArkilianError is the structured error type used throughout the system.
type ArkilianError struct {
	Category  ErrorCategory
	Code      string
	Message   string
	Details   map[string]interface{}
	Cause     error
	Retryable bool
}

// Error returns a formatted error string.
func (e *ArkilianError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("[%s:%s] %s: %v", e.Category, e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("[%s:%s] %s", e.Category, e.Code, e.Message)
}

// Unwrap returns the underlying cause for errors.Is/As compatibility.
func (e *ArkilianError) Unwrap() error {
	return e.Cause
}

// Is reports whether the target matches this error's category and code.
func (e *ArkilianError) Is(target error) bool {
	var t *ArkilianError
	if errors.As(target, &t) {
		return e.Category == t.Category && e.Code == t.Code
	}
	return false
}

// New creates a new ArkilianError.
func New(category ErrorCategory, code, message string) *ArkilianError {
	return &ArkilianError{
		Category:  category,
		Code:      code,
		Message:   message,
		Retryable: isRetryable(category, code),
	}
}

// Wrap creates a new ArkilianError wrapping an existing error.
func Wrap(category ErrorCategory, code, message string, cause error) *ArkilianError {
	return &ArkilianError{
		Category:  category,
		Code:      code,
		Message:   message,
		Cause:     cause,
		Retryable: isRetryable(category, code),
	}
}

// WithDetails returns a copy of the error with additional details.
func (e *ArkilianError) WithDetails(details map[string]interface{}) *ArkilianError {
	cp := *e
	cp.Details = details
	return &cp
}

// IsRetryable checks whether an error (or its chain) is retryable.
func IsRetryable(err error) bool {
	var ae *ArkilianError
	if errors.As(err, &ae) {
		return ae.Retryable
	}
	return false
}

// GetCategory extracts the error category from an error chain.
// Returns empty string if the error is not an ArkilianError.
func GetCategory(err error) ErrorCategory {
	var ae *ArkilianError
	if errors.As(err, &ae) {
		return ae.Category
	}
	return ""
}

// GetCode extracts the error code from an error chain.
// Returns empty string if the error is not an ArkilianError.
func GetCode(err error) string {
	var ae *ArkilianError
	if errors.As(err, &ae) {
		return ae.Code
	}
	return ""
}

// isRetryable determines if an error code is retryable based on the design spec.
func isRetryable(category ErrorCategory, code string) bool {
	switch {
	case category == ErrCategoryStorage && code == CodeUploadFailed:
		return true
	case category == ErrCategoryStorage && code == CodeDownloadFailed:
		return true
	case category == ErrCategoryManifest && code == CodeWriteConflict:
		return true
	case category == ErrCategoryQuery && code == CodeExecutionTimeout:
		return true
	default:
		return false
	}
}

// Convenience constructors for common errors.

func NewValidationError(code, message string) *ArkilianError {
	return New(ErrCategoryValidation, code, message)
}

func NewStorageError(code, message string, cause error) *ArkilianError {
	return Wrap(ErrCategoryStorage, code, message, cause)
}

func NewManifestError(code, message string, cause error) *ArkilianError {
	return Wrap(ErrCategoryManifest, code, message, cause)
}

func NewQueryError(code, message string) *ArkilianError {
	return New(ErrCategoryQuery, code, message)
}

func NewCompactionError(code, message string, cause error) *ArkilianError {
	return Wrap(ErrCategoryCompaction, code, message, cause)
}

func NewInternalError(message string, cause error) *ArkilianError {
	return Wrap(ErrCategoryInternal, CodeUnexpected, message, cause)
}
