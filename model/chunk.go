package model

import (
	"time"

	"github.com/google/uuid"
)

// Chunk represents a document chunk (node in the graph)
type Chunk struct {
	ID          uuid.UUID `json:"id"`
	DocumentID  int64     `json:"document_id"`
	DocumentRID uuid.UUID `json:"document_rid"`
	Content     string    `json:"content"`
	Path        string    `json:"path"` // ltree path
	Embedding   []float32 `json:"embedding,omitempty"`
	StartPos    *int      `json:"start_pos,omitempty"`
	EndPos      *int      `json:"end_pos,omitempty"`
	ChunkIndex  *int      `json:"chunk_index,omitempty"`
	Metadata    Metadata  `json:"metadata,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	// Results
	Similarity *float64 `json:"similarity,omitempty"`
	IsMatch    *bool    `json:"is_match,omitempty"`
}
