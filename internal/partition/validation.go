package partition

import (
	"fmt"
	"strings"

	"github.com/arkilian/arkilian/pkg/types"
)

// ValidationError represents a schema validation error.
type ValidationError struct {
	RowIndex int
	Field    string
	Message  string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("row %d, field %q: %s", e.RowIndex, e.Field, e.Message)
}

// ValidationErrors is a collection of validation errors.
type ValidationErrors []*ValidationError

func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return "no validation errors"
	}
	if len(e) == 1 {
		return e[0].Error()
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%d validation errors:\n", len(e)))
	for i, err := range e {
		if i > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString("  - ")
		sb.WriteString(err.Error())
	}
	return sb.String()
}

// SchemaValidator validates rows against a declared schema.
type SchemaValidator struct {
	schema types.Schema
}

// NewSchemaValidator creates a new schema validator.
func NewSchemaValidator(schema types.Schema) *SchemaValidator {
	return &SchemaValidator{schema: schema}
}

// ValidateRow validates a single row against the schema.
func (v *SchemaValidator) ValidateRow(row types.Row, rowIndex int) []*ValidationError {
	var errors []*ValidationError

	// Validate event_id
	if len(row.EventID) == 0 {
		// EventID can be empty - it will be generated
	} else if len(row.EventID) != 16 {
		errors = append(errors, &ValidationError{
			RowIndex: rowIndex,
			Field:    "event_id",
			Message:  fmt.Sprintf("event_id must be 16 bytes (ULID), got %d bytes", len(row.EventID)),
		})
	}

	// Validate tenant_id (required, TEXT)
	if row.TenantID == "" {
		errors = append(errors, &ValidationError{
			RowIndex: rowIndex,
			Field:    "tenant_id",
			Message:  "tenant_id is required and cannot be empty",
		})
	}

	// Validate user_id (required, INTEGER - no validation needed for int64)

	// Validate event_time (required, INTEGER - should be positive)
	if row.EventTime <= 0 {
		errors = append(errors, &ValidationError{
			RowIndex: rowIndex,
			Field:    "event_time",
			Message:  "event_time must be a positive Unix timestamp",
		})
	}

	// Validate event_type (required, TEXT)
	if row.EventType == "" {
		errors = append(errors, &ValidationError{
			RowIndex: rowIndex,
			Field:    "event_type",
			Message:  "event_type is required and cannot be empty",
		})
	}

	// Validate payload (required, but can be empty map)
	if row.Payload == nil {
		errors = append(errors, &ValidationError{
			RowIndex: rowIndex,
			Field:    "payload",
			Message:  "payload is required (use empty map {} if no data)",
		})
	}

	return errors
}

// ValidateRows validates multiple rows against the schema.
func (v *SchemaValidator) ValidateRows(rows []types.Row) ValidationErrors {
	var allErrors ValidationErrors

	for i, row := range rows {
		rowErrors := v.ValidateRow(row, i)
		allErrors = append(allErrors, rowErrors...)
	}

	return allErrors
}

// Validate validates rows and returns an error if any validation fails.
func (v *SchemaValidator) Validate(rows []types.Row) error {
	errors := v.ValidateRows(rows)
	if len(errors) > 0 {
		return errors
	}
	return nil
}

// ValidateRowsStrict validates rows and returns on first error.
func (v *SchemaValidator) ValidateRowsStrict(rows []types.Row) error {
	for i, row := range rows {
		errors := v.ValidateRow(row, i)
		if len(errors) > 0 {
			return errors[0]
		}
	}
	return nil
}

// ValidateSchema validates the schema definition itself.
func ValidateSchema(schema types.Schema) error {
	if schema.Version < 1 {
		return fmt.Errorf("schema version must be >= 1, got %d", schema.Version)
	}

	if len(schema.Columns) == 0 {
		return fmt.Errorf("schema must have at least one column")
	}

	// Check for primary key
	hasPrimaryKey := false
	columnNames := make(map[string]bool)

	for _, col := range schema.Columns {
		if col.Name == "" {
			return fmt.Errorf("column name cannot be empty")
		}

		if columnNames[col.Name] {
			return fmt.Errorf("duplicate column name: %s", col.Name)
		}
		columnNames[col.Name] = true

		if col.PrimaryKey {
			hasPrimaryKey = true
		}

		// Validate column type
		validTypes := map[string]bool{
			"TEXT":    true,
			"INTEGER": true,
			"BLOB":    true,
			"REAL":    true,
		}
		if !validTypes[col.Type] {
			return fmt.Errorf("invalid column type %q for column %q", col.Type, col.Name)
		}
	}

	if !hasPrimaryKey {
		return fmt.Errorf("schema must have at least one primary key column")
	}

	// Validate indexes
	for _, idx := range schema.Indexes {
		if idx.Name == "" {
			return fmt.Errorf("index name cannot be empty")
		}

		if len(idx.Columns) == 0 {
			return fmt.Errorf("index %q must have at least one column", idx.Name)
		}

		for _, colName := range idx.Columns {
			if !columnNames[colName] {
				return fmt.Errorf("index %q references unknown column %q", idx.Name, colName)
			}
		}
	}

	return nil
}

// DefaultValidator returns a validator for the default events schema.
func DefaultValidator() *SchemaValidator {
	return NewSchemaValidator(DefaultSchema())
}
