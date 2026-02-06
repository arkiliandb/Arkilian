package errors

import (
	"errors"
	"fmt"
	"testing"
)

func TestArkilianError_Error(t *testing.T) {
	err := New(ErrCategoryStorage, CodeUploadFailed, "upload failed")
	expected := "[STORAGE:UPLOAD_FAILED] upload failed"
	if err.Error() != expected {
		t.Errorf("got %q, want %q", err.Error(), expected)
	}
}

func TestArkilianError_ErrorWithCause(t *testing.T) {
	cause := fmt.Errorf("connection refused")
	err := Wrap(ErrCategoryStorage, CodeUploadFailed, "upload failed", cause)
	expected := "[STORAGE:UPLOAD_FAILED] upload failed: connection refused"
	if err.Error() != expected {
		t.Errorf("got %q, want %q", err.Error(), expected)
	}
}

func TestArkilianError_Unwrap(t *testing.T) {
	cause := fmt.Errorf("root cause")
	err := Wrap(ErrCategoryManifest, CodeWriteConflict, "conflict", cause)
	if !errors.Is(err, cause) {
		t.Error("Unwrap should allow errors.Is to find the cause")
	}
}

func TestArkilianError_Is(t *testing.T) {
	err1 := New(ErrCategoryStorage, CodeUploadFailed, "first")
	err2 := New(ErrCategoryStorage, CodeUploadFailed, "second")
	err3 := New(ErrCategoryStorage, CodeDownloadFailed, "different code")

	if !errors.Is(err1, err2) {
		t.Error("errors with same category+code should match via Is")
	}
	if errors.Is(err1, err3) {
		t.Error("errors with different codes should not match via Is")
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		category  ErrorCategory
		code      string
		retryable bool
	}{
		{ErrCategoryStorage, CodeUploadFailed, true},
		{ErrCategoryStorage, CodeDownloadFailed, true},
		{ErrCategoryStorage, CodeObjectNotFound, false},
		{ErrCategoryManifest, CodeWriteConflict, true},
		{ErrCategoryManifest, CodeCorruptionDetected, false},
		{ErrCategoryQuery, CodeExecutionTimeout, true},
		{ErrCategoryQuery, CodeParseError, false},
		{ErrCategoryValidation, CodeInvalidSchema, false},
		{ErrCategoryCompaction, CodeValidationFailed, false},
		{ErrCategoryInternal, CodeUnexpected, false},
	}

	for _, tt := range tests {
		err := New(tt.category, tt.code, "test")
		if IsRetryable(err) != tt.retryable {
			t.Errorf("%s:%s retryable=%v, want %v", tt.category, tt.code, IsRetryable(err), tt.retryable)
		}
	}
}

func TestGetCategory(t *testing.T) {
	err := New(ErrCategoryQuery, CodeParseError, "bad sql")
	if GetCategory(err) != ErrCategoryQuery {
		t.Errorf("got %q, want %q", GetCategory(err), ErrCategoryQuery)
	}
	if GetCategory(fmt.Errorf("plain error")) != "" {
		t.Error("non-ArkilianError should return empty category")
	}
}

func TestGetCode(t *testing.T) {
	err := New(ErrCategoryQuery, CodeParseError, "bad sql")
	if GetCode(err) != CodeParseError {
		t.Errorf("got %q, want %q", GetCode(err), CodeParseError)
	}
	if GetCode(fmt.Errorf("plain error")) != "" {
		t.Error("non-ArkilianError should return empty code")
	}
}

func TestWithDetails(t *testing.T) {
	err := New(ErrCategoryValidation, CodeInvalidSchema, "bad schema")
	detailed := err.WithDetails(map[string]interface{}{"field": "tenant_id"})

	if detailed.Details["field"] != "tenant_id" {
		t.Error("WithDetails should set details")
	}
	// Original should be unmodified
	if err.Details != nil {
		t.Error("WithDetails should not modify original")
	}
}

func TestConvenienceConstructors(t *testing.T) {
	cause := fmt.Errorf("io error")

	v := NewValidationError(CodeEmptyBatch, "no rows")
	if v.Category != ErrCategoryValidation || v.Code != CodeEmptyBatch {
		t.Error("NewValidationError mismatch")
	}

	s := NewStorageError(CodeUploadFailed, "s3 down", cause)
	if s.Category != ErrCategoryStorage || !errors.Is(s, cause) {
		t.Error("NewStorageError mismatch")
	}

	m := NewManifestError(CodeWriteConflict, "locked", cause)
	if m.Category != ErrCategoryManifest {
		t.Error("NewManifestError mismatch")
	}

	q := NewQueryError(CodeParseError, "syntax error")
	if q.Category != ErrCategoryQuery {
		t.Error("NewQueryError mismatch")
	}

	c := NewCompactionError(CodeValidationFailed, "checksum mismatch", cause)
	if c.Category != ErrCategoryCompaction {
		t.Error("NewCompactionError mismatch")
	}

	i := NewInternalError("unexpected", cause)
	if i.Category != ErrCategoryInternal || i.Code != CodeUnexpected {
		t.Error("NewInternalError mismatch")
	}
}
