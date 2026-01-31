package model

import (
	"time"

	"github.com/google/uuid"
)

type RetrievalMethod string

const (
	RetrievalMethodVector     RetrievalMethod = "vector"
	RetrievalMethodEntity     RetrievalMethod = "entity"
	RetrievalMethodNeighbor   RetrievalMethod = "neighbor"
	RetrievalMethodContextual RetrievalMethod = "contextual"
	RetrievalMethodGraph      RetrievalMethod = "graph"
	RetrievalMethodHybrid     RetrievalMethod = "hybrid"
)

// Chunk represents a document chunk (node in the graph)
type Chunk struct {
	ID          int       `json:"id"`
	DocumentID  int       `json:"document_id"`
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
	Similarity      float64         `json:"similarity,omitempty"`
	Distance        float64         `json:"distance,omitempty"`
	IsMatch         bool            `json:"is_match,omitempty"`
	PathFromSource  []int           `json:"path_from_source,omitempty"`
	Score           float64         `json:"score,omitempty"`
	RetrievalMethod RetrievalMethod `json:"retrieval_method,omitempty"`
}
