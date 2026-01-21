package model

import (
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

// Document represents a source document
type Document struct {
	ID        int64     `json:"id"`
	RID       uuid.UUID `json:"rid"`
	Title     string    `json:"title"`
	Source    string    `json:"source,omitempty"`
	Content   string    `json:"content,omitempty" db:"-"` // Temporary field for processing, not stored in DB
	Metadata  Metadata  `json:"metadata,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// NewDocumentFromFile reads a file and creates a Document with the file content
// The title defaults to the filename, and source to the file path
func NewDocumentFromFile(filePath string, metadata Metadata) (*Document, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	// Get filename without extension for default title
	filename := filepath.Base(filePath)
	title := filename[:len(filename)-len(filepath.Ext(filename))]
	if title == "" {
		title = filename
	}

	return &Document{
		Title:    title,
		Source:   filePath,
		Content:  string(content),
		Metadata: metadata,
	}, nil
}
