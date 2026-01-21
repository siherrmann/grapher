package retrieval

import (
	"context"
	"sort"

	"github.com/google/uuid"
	"github.com/siherrmann/grapher/model"
)

// Strategy defines a retrieval strategy
type Strategy interface {
	Retrieve(ctx context.Context, embedding []float32, config *model.QueryConfig) ([]*model.RetrievalResult, error)
}

// VectorOnlyStrategy performs pure vector similarity search
type VectorOnlyStrategy struct {
	engine *Engine
}

// NewVectorOnlyStrategy creates a new vector-only strategy
func NewVectorOnlyStrategy(engine *Engine) *VectorOnlyStrategy {
	return &VectorOnlyStrategy{engine: engine}
}

// Retrieve performs vector-only retrieval
func (s *VectorOnlyStrategy) Retrieve(ctx context.Context, embedding []float32, config *model.QueryConfig) ([]*model.RetrievalResult, error) {
	return s.engine.VectorRetrieve(ctx, embedding, config)
}

// ContextualStrategy combines vector search with immediate neighbors and hierarchical context
type ContextualStrategy struct {
	engine *Engine
}

// NewContextualStrategy creates a new contextual strategy
func NewContextualStrategy(engine *Engine) *ContextualStrategy {
	return &ContextualStrategy{engine: engine}
}

// Retrieve performs contextual retrieval
func (s *ContextualStrategy) Retrieve(ctx context.Context, embedding []float32, config *model.QueryConfig) ([]*model.RetrievalResult, error) {
	// First, get top-k similar chunks
	vectorResults, err := s.engine.VectorRetrieve(ctx, embedding, config)
	if err != nil {
		return nil, err
	}

	resultMap := make(map[string]*model.RetrievalResult)

	// Add vector results
	for _, result := range vectorResults {
		resultMap[result.Chunk.ID.String()] = result
	}

	// For each vector result, add neighbors and hierarchical context
	for _, result := range vectorResults {
		// Get neighbors
		neighbors, err := s.engine.GetNeighbors(ctx, result.Chunk.ID, config.EdgeTypes, config.FollowBidirectional)
		if err != nil {
			continue
		}

		for _, neighbor := range neighbors {
			if _, exists := resultMap[neighbor.ID.String()]; !exists {
				resultMap[neighbor.ID.String()] = &model.RetrievalResult{
					Chunk:           neighbor,
					Score:           result.Score * config.GraphWeight,
					SimilarityScore: 0,
					GraphDistance:   1,
					RetrievalMethod: "graph_neighbor",
				}
			}
		}

		// Get hierarchical context
		hierarchicalChunks, err := s.engine.GetHierarchicalContext(ctx, result.Chunk.Path, config)
		if err != nil {
			continue
		}

		for _, hChunk := range hierarchicalChunks {
			if _, exists := resultMap[hChunk.ID.String()]; !exists {
				resultMap[hChunk.ID.String()] = &model.RetrievalResult{
					Chunk:           hChunk,
					Score:           result.Score * config.HierarchyWeight,
					SimilarityScore: 0,
					GraphDistance:   0,
					RetrievalMethod: "hierarchical",
				}
			}
		}
	}

	// Convert map to slice
	results := make([]*model.RetrievalResult, 0, len(resultMap))
	for _, result := range resultMap {
		results = append(results, result)
	}

	// Sort by score
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	return results, nil
}

// MultiHopStrategy performs graph traversal from top vector results
type MultiHopStrategy struct {
	engine *Engine
}

// NewMultiHopStrategy creates a new multi-hop strategy
func NewMultiHopStrategy(engine *Engine) *MultiHopStrategy {
	return &MultiHopStrategy{
		engine: engine,
	}
}

// Retrieve performs multi-hop retrieval
func (s *MultiHopStrategy) Retrieve(ctx context.Context, embedding []float32, config *model.QueryConfig) ([]*model.RetrievalResult, error) {
	// First, get top-k similar chunks as starting points
	vectorResults, err := s.engine.VectorRetrieve(ctx, embedding, config)
	if err != nil {
		return nil, err
	}

	resultMap := make(map[string]*model.RetrievalResult)

	// Add vector results
	for _, result := range vectorResults {
		resultMap[result.Chunk.ID.String()] = result
	}

	// For each starting point, perform BFS/DFS
	for _, result := range vectorResults {
		traversalResults, err := s.engine.BFS(
			ctx,
			result.Chunk.ID,
			config.MaxHops,
			config.EdgeTypes,
			config.FollowBidirectional,
		)
		if err != nil {
			continue
		}

		for _, tResult := range traversalResults {
			// Skip the source chunk (already in results)
			if tResult.Distance == 0 {
				continue
			}

			chunkIDStr := tResult.Chunk.ID.String()
			if _, exists := resultMap[chunkIDStr]; !exists {
				// Calculate score based on distance and original similarity
				score := result.Score * config.GraphWeight / float64(tResult.Distance+1)

				resultMap[chunkIDStr] = &model.RetrievalResult{
					Chunk:           tResult.Chunk,
					Score:           score,
					SimilarityScore: 0,
					GraphDistance:   tResult.Distance,
					RetrievalMethod: "multi_hop",
				}
			}
		}
	}

	// Convert map to slice
	results := make([]*model.RetrievalResult, 0, len(resultMap))
	for _, result := range resultMap {
		results = append(results, result)
	}

	// Sort by score
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	return results, nil
}

// HybridStrategy combines vector, graph, and hierarchical signals with configurable weights
type HybridStrategy struct {
	engine *Engine
}

// NewHybridStrategy creates a new hybrid strategy
func NewHybridStrategy(engine *Engine) *HybridStrategy {
	return &HybridStrategy{
		engine: engine,
	}
}

// Retrieve performs hybrid retrieval with weighted combination
func (s *HybridStrategy) Retrieve(ctx context.Context, embedding []float32, config *model.QueryConfig) ([]*model.RetrievalResult, error) {
	// Get vector results
	vectorResults, err := s.engine.VectorRetrieve(ctx, embedding, config)
	if err != nil {
		return nil, err
	}

	resultMap := make(map[string]*model.RetrievalResult)

	// Process each vector result
	for _, vResult := range vectorResults {
		chunkIDStr := vResult.Chunk.ID.String()

		// Initialize with vector score
		score := vResult.SimilarityScore * config.VectorWeight

		resultMap[chunkIDStr] = &model.RetrievalResult{
			Chunk:           vResult.Chunk,
			Score:           score,
			SimilarityScore: vResult.SimilarityScore,
			GraphDistance:   0,
			RetrievalMethod: "hybrid",
		}

		// Add graph neighbors
		if config.MaxHops > 0 {
			traversalResults, err := s.engine.BFS(
				ctx,
				vResult.Chunk.ID,
				config.MaxHops,
				config.EdgeTypes,
				config.FollowBidirectional,
			)
			if err == nil {
				for _, tResult := range traversalResults {
					tChunkIDStr := tResult.Chunk.ID.String()

					if existing, exists := resultMap[tChunkIDStr]; exists {
						// Update score with graph component
						if tResult.Distance > 0 {
							graphScore := config.GraphWeight / float64(tResult.Distance)
							existing.Score += graphScore
						}
					} else if tResult.Distance > 0 {
						// New chunk from graph traversal
						graphScore := config.GraphWeight / float64(tResult.Distance)
						resultMap[tChunkIDStr] = &model.RetrievalResult{
							Chunk:           tResult.Chunk,
							Score:           graphScore,
							SimilarityScore: 0,
							GraphDistance:   tResult.Distance,
							RetrievalMethod: "hybrid",
						}
					}
				}
			}
		}

		// Add hierarchical context
		if config.IncludeAncestors || config.IncludeDescendants || config.IncludeSiblings {
			hierarchicalChunks, err := s.engine.GetHierarchicalContext(ctx, vResult.Chunk.Path, config)
			if err == nil {
				for _, hChunk := range hierarchicalChunks {
					hChunkIDStr := hChunk.ID.String()

					if existing, exists := resultMap[hChunkIDStr]; exists {
						// Update score with hierarchy component
						existing.Score += config.HierarchyWeight
					} else {
						// New chunk from hierarchical context
						resultMap[hChunkIDStr] = &model.RetrievalResult{
							Chunk:           hChunk,
							Score:           config.HierarchyWeight,
							SimilarityScore: 0,
							GraphDistance:   0,
							RetrievalMethod: "hybrid",
						}
					}
				}
			}
		}
	}

	// Convert map to slice
	results := make([]*model.RetrievalResult, 0, len(resultMap))
	for _, result := range resultMap {
		results = append(results, result)
	}

	// Sort by combined score
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Limit to top-k
	if len(results) > config.TopK {
		results = results[:config.TopK]
	}

	return results, nil
}

// EntityCentricStrategy retrieves all chunks related to specific entities
type EntityCentricStrategy struct {
	engine     *Engine
	entitiesDB EntitiesDB
}

// EntitiesDB defines the interface for entity operations
type EntitiesDB interface {
	GetEntity(ctx context.Context, id string) (*model.Entity, error)
	GetChunksForEntity(ctx context.Context, entityID string) ([]*model.Chunk, error)
}

// NewEntityCentricStrategy creates a new entity-centric strategy
func NewEntityCentricStrategy(engine *Engine, entitiesDB EntitiesDB) *EntityCentricStrategy {
	return &EntityCentricStrategy{
		engine:     engine,
		entitiesDB: entitiesDB,
	}
}

// Retrieve performs entity-centric retrieval
func (s *EntityCentricStrategy) Retrieve(ctx context.Context, entityID uuid.UUID, config *model.QueryConfig) ([]*model.RetrievalResult, error) {
	resultMap := make(map[string]*model.RetrievalResult)

	// Get chunks directly linked to the entity
	chunks, err := s.entitiesDB.GetChunksForEntity(ctx, entityID.String())
	if err != nil {
		return nil, err
	}

	// Add all chunks related to the entity
	for _, chunk := range chunks {
		resultMap[chunk.ID.String()] = &model.RetrievalResult{
			Chunk:           chunk,
			Score:           1.0,
			SimilarityScore: 0,
			GraphDistance:   0,
			RetrievalMethod: "entity_centric",
		}
	}

	// Optionally expand via graph traversal
	if config.MaxHops > 0 {
		for _, chunk := range chunks {
			traversalResults, err := s.engine.BFS(
				ctx,
				chunk.ID,
				config.MaxHops,
				config.EdgeTypes,
				config.FollowBidirectional,
			)
			if err != nil {
				continue
			}

			for _, tResult := range traversalResults {
				if tResult.Distance == 0 {
					continue // Skip source
				}

				tChunkIDStr := tResult.Chunk.ID.String()
				if _, exists := resultMap[tChunkIDStr]; !exists {
					score := config.GraphWeight / float64(tResult.Distance)
					resultMap[tChunkIDStr] = &model.RetrievalResult{
						Chunk:           tResult.Chunk,
						Score:           score,
						SimilarityScore: 0,
						GraphDistance:   tResult.Distance,
						RetrievalMethod: "entity_fanout",
					}
				}
			}
		}
	}

	// Convert to slice
	results := make([]*model.RetrievalResult, 0, len(resultMap))
	for _, result := range resultMap {
		results = append(results, result)
	}

	// Sort by score
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Limit to top-k if specified
	if config.TopK > 0 && len(results) > config.TopK {
		results = results[:config.TopK]
	}

	return results, nil
}
