package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"

	"github.com/siherrmann/grapher/helper"
)

// Metadata represents JSONB metadata stored in PostgreSQL
type Metadata map[string]interface{}

// Value implements the driver.Valuer interface for database storage
func (m Metadata) Value() (driver.Value, error) {
	return m.Marshal()
}

// Scan implements the sql.Scanner interface for database retrieval
func (m *Metadata) Scan(value interface{}) error {
	return m.Unmarshal(value)
}

// Marshal converts Metadata to JSON bytes
func (m Metadata) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal converts JSON bytes or Metadata to Metadata
func (m *Metadata) Unmarshal(value interface{}) error {
	if value == nil {
		*m = Metadata{}
		return nil
	}

	if s, ok := value.(Metadata); ok {
		*m = Metadata(s)
		return nil
	}

	b, ok := value.([]byte)
	if !ok {
		return helper.NewError("byte assertion", errors.New("type assertion to []byte failed"))
	}

	return json.Unmarshal(b, m)
}
