package model

import (
	"time"

	"github.com/google/uuid"
)

// Entity represents a named entity (person, place, concept, etc.)
type Entity struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Type      string    `json:"entity_type"`
	Metadata  Metadata  `json:"metadata,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// ChunkMention represents a chunk that mentions an entity
type ChunkMention struct {
	ChunkID      uuid.UUID `json:"chunk_id"`
	EdgeID       uuid.UUID `json:"edge_id"`
	EdgeMetadata Metadata  `json:"edge_metadata,omitempty"`
}
