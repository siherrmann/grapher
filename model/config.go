package model

import "github.com/google/uuid"

// QueryConfig represents configuration for a retrieval query
type QueryConfig struct {
	// Vector search parameters
	TopK                int     `json:"top_k"`
	SimilarityThreshold float64 `json:"similarity_threshold,omitempty"`

	// Document filtering
	DocumentRIDs []uuid.UUID `json:"document_rids,omitempty"` // Filter by specific documents

	// Graph traversal parameters
	MaxHops             int        `json:"max_hops,omitempty"`
	EdgeTypes           []EdgeType `json:"edge_types,omitempty"` // Filter by edge types
	FollowBidirectional bool       `json:"follow_bidirectional"`

	// Ltree parameters
	IncludeAncestors   bool `json:"include_ancestors"`
	IncludeDescendants bool `json:"include_descendants"`
	IncludeSiblings    bool `json:"include_siblings"`

	// Ranking parameters
	VectorWeight    float64 `json:"vector_weight"`    // Weight for similarity score
	GraphWeight     float64 `json:"graph_weight"`     // Weight for graph distance
	HierarchyWeight float64 `json:"hierarchy_weight"` // Weight for hierarchy distance
	EntityWeight    float64 `json:"entity_weight"`    // Weight for entity mentions
}

// DefaultQueryConfig returns a sensible default configuration
func DefaultQueryConfig() QueryConfig {
	return QueryConfig{
		TopK:                5,
		SimilarityThreshold: 0.7,
		MaxHops:             2,
		EdgeTypes:           nil, // All types
		FollowBidirectional: true,
		IncludeAncestors:    false,
		IncludeDescendants:  false,
		IncludeSiblings:     true,
		VectorWeight:        0.6,
		GraphWeight:         0.3,
		HierarchyWeight:     0.1,
		EntityWeight:        0.5, // Boost for entity mentions
	}
}
