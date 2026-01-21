package model

import (
	"time"

	"github.com/google/uuid"
)

// EdgeType represents the type of relationship between nodes
type EdgeType string

const (
	EdgeTypeSemantic      EdgeType = "semantic"
	EdgeTypeHierarchical  EdgeType = "hierarchical"
	EdgeTypeReference     EdgeType = "reference"
	EdgeTypeEntityMention EdgeType = "entity_mention"
	EdgeTypeTemporal      EdgeType = "temporal"
	EdgeTypeCausal        EdgeType = "causal"
	EdgeTypeCustom        EdgeType = "custom"
)

// Edge represents a relationship between chunks and/or entities
type Edge struct {
	ID             uuid.UUID  `json:"id"`
	SourceChunkID  *uuid.UUID `json:"source_chunk_id,omitempty"`
	TargetChunkID  *uuid.UUID `json:"target_chunk_id,omitempty"`
	SourceEntityID *uuid.UUID `json:"source_entity_id,omitempty"`
	TargetEntityID *uuid.UUID `json:"target_entity_id,omitempty"`
	EdgeType       EdgeType   `json:"edge_type"`
	Weight         float64    `json:"weight"`
	Bidirectional  bool       `json:"bidirectional"`
	Metadata       Metadata   `json:"metadata,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
}

// EdgeConnection represents an edge with directional information
type EdgeConnection struct {
	Edge       *Edge `json:"edge"`
	IsOutgoing bool  `json:"is_outgoing"`
}

// TraversalNode represents a node in a graph traversal
type TraversalNode struct {
	ChunkID uuid.UUID   `json:"chunk_id"`
	Depth   int         `json:"depth"`
	Path    []uuid.UUID `json:"path"`
}
