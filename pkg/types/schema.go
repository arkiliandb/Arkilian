package types

// Schema defines the structure of a partition's data.
type Schema struct {
	// Version tracks schema evolution for backward compatibility
	Version int `json:"version"`

	// Columns defines the columns in the schema
	Columns []ColumnDef `json:"columns"`

	// Indexes defines the indexes to create on the partition
	Indexes []IndexDef `json:"indexes"`
}

// ColumnDef defines a single column in the schema.
type ColumnDef struct {
	// Name is the column name
	Name string `json:"name"`

	// Type is the SQLite type: TEXT, INTEGER, BLOB, REAL
	Type string `json:"type"`

	// Nullable indicates whether the column can contain NULL values
	Nullable bool `json:"nullable"`

	// PrimaryKey indicates whether this column is part of the primary key
	PrimaryKey bool `json:"primary_key"`
}

// IndexDef defines an index on the partition.
type IndexDef struct {
	// Name is the index name
	Name string `json:"name"`

	// Columns lists the columns included in the index
	Columns []string `json:"columns"`

	// Unique indicates whether the index enforces uniqueness
	Unique bool `json:"unique"`
}
