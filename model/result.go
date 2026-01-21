package model

// RetrievalResult represents a chunk retrieved by a query
type RetrievalResult struct {
	Chunk             *Chunk   `json:"chunk"`
	Score             float64  `json:"score"`            // Combined score from ranking
	SimilarityScore   float64  `json:"similarity_score"` // Cosine similarity score
	GraphDistance     int      `json:"graph_distance"`   // Distance from query node in graph
	RetrievalMethod   string   `json:"retrieval_method"` // How it was retrieved (vector, graph, ltree)
	ConnectedEntities []Entity `json:"connected_entities,omitempty"`
}
