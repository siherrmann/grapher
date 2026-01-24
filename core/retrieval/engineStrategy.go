package retrieval

import (
	"context"
	"sort"

	"github.com/siherrmann/grapher/model"
)

// Similarity performs vector-only retrieval
func (e *Engine) Similarity(ctx context.Context, embedding []float32, config *model.QueryConfig) ([]*model.Chunk, error) {
	// Get vector results
	chunks, err := e.chunks.SelectChunksBySimilarity(embedding, config.TopK, config.SimilarityThreshold, config.DocumentRIDs)
	if err != nil {
		return nil, err
	}

	return chunks, nil
}

// Contextual performs contextual retrieval
func (e *Engine) Contextual(ctx context.Context, embedding []float32, config *model.QueryConfig) ([]*model.Chunk, error) {
	// Get vector results
	chunks, err := e.chunks.SelectChunksBySimilarity(embedding, config.TopK, config.SimilarityThreshold, config.DocumentRIDs)
	if err != nil {
		return nil, err
	}

	// For each vector result, add neighbors and hierarchical context
	resultMap := make(map[int]*model.Chunk)
	for _, csim := range chunks {
		// Get neighbors
		neighbors, err := e.GetNeighbors(ctx, csim.ID, config.EdgeTypes, config.FollowBidirectional)
		if err != nil {
			continue
		}

		for _, cnei := range neighbors {
			if _, exists := resultMap[cnei.ID]; !exists {
				resultMap[cnei.ID] = cnei
			}
		}

		// Get hierarchical context
		hierarchicalChunks, err := e.GetHierarchicalContext(ctx, csim.Path, config)
		if err != nil {
			continue
		}

		for _, hChunk := range hierarchicalChunks {
			if _, exists := resultMap[hChunk.ID]; !exists {
				resultMap[hChunk.ID] = hChunk
			}
		}
	}

	// Sort
	results := e.sortResults(resultMap, config.TopK)

	return results, nil
}

// MultiHop performs multi-hop retrieval
func (e *Engine) MultiHop(ctx context.Context, embedding []float32, config *model.QueryConfig) ([]*model.Chunk, error) {
	// Get vector results
	chunks, err := e.chunks.SelectChunksBySimilarity(embedding, config.TopK, config.SimilarityThreshold, config.DocumentRIDs)
	if err != nil {
		return nil, err
	}

	// Process each vector result
	resultMap := make(map[int]*model.Chunk)
	for _, csim := range chunks {
		resultMap[csim.ID] = csim

		// Get graph traversal results
		if config.MaxHops > 0 {
			chunks, err := e.chunks.SelectChunksByBFS(csim.ID, config.MaxHops, config.EdgeTypes, config.FollowBidirectional)
			if err != nil {
				continue
			}
			for _, cnei := range chunks {
				if _, exists := resultMap[cnei.ID]; !exists {
					resultMap[cnei.ID] = cnei
				}
			}
		}
	}

	// Sort
	results := e.sortResults(resultMap, config.TopK)

	return results, nil
}

// Hybrid performs hybrid retrieval with weighted combination including entity-based search
func (e *Engine) Hybrid(ctx context.Context, embedding []float32, config *model.QueryConfig) ([]*model.Chunk, error) {
	// Get vector results
	chunks, err := e.chunks.SelectChunksBySimilarity(embedding, config.TopK, config.SimilarityThreshold, config.DocumentRIDs)
	if err != nil {
		return nil, err
	}

	// Process each vector result
	resultMap := make(map[int]*model.Chunk)
	for _, csim := range chunks {
		resultMap[csim.ID] = csim

		// Get graph traversal results
		if config.MaxHops > 0 {
			chunks, err := e.chunks.SelectChunksByBFS(csim.ID, config.MaxHops, config.EdgeTypes, config.FollowBidirectional)
			if err != nil {
				continue
			}
			for _, cnei := range chunks {
				if _, exists := resultMap[cnei.ID]; !exists {
					resultMap[cnei.ID] = cnei
				}
			}
		}

		// Add hierarchical context
		if config.IncludeAncestors || config.IncludeDescendants || config.IncludeSiblings {
			hierarchicalChunks, err := e.GetHierarchicalContext(ctx, csim.Path, config)
			if err != nil {
				continue
			}
			for _, chie := range hierarchicalChunks {
				if existing, exists := resultMap[chie.ID]; exists {
					// Update score with hierarchy component
					existing.Score += config.HierarchyWeight
				} else {
					// New chunk from hierarchical context
					resultMap[chie.ID] = chie
				}
			}
		}
	}

	// Sort
	results := e.sortResults(resultMap, config.TopK)

	return results, nil
}

// Retrieve performs entity-centric retrieval
func (e *Engine) EntityCentric(ctx context.Context, entityID int, config *model.QueryConfig) ([]*model.Chunk, error) {
	// Get chunks directly linked to the entity
	chunks, err := e.chunks.SelectChunksByEntity(entityID)
	if err != nil {
		return nil, err
	}

	// Add all chunks related to the entity
	resultMap := make(map[int]*model.Chunk)
	for _, chunk := range chunks {
		resultMap[chunk.ID] = chunk
	}

	// Optionally expand via graph traversal
	if config.MaxHops > 0 {
		for _, csim := range chunks {
			chunks, err := e.chunks.SelectChunksByBFS(csim.ID, config.MaxHops, config.EdgeTypes, config.FollowBidirectional)
			if err != nil {
				continue
			}
			for _, cnei := range chunks {
				if _, exists := resultMap[cnei.ID]; !exists {
					resultMap[cnei.ID] = cnei
				}
			}
		}
	}

	// Sort
	results := e.sortResults(resultMap, config.TopK)

	return results, nil
}

func (e *Engine) sortResults(resultMap map[int]*model.Chunk, topK int) []*model.Chunk {
	// Convert map to slice
	results := make([]*model.Chunk, 0, len(resultMap))
	for _, result := range resultMap {
		results = append(results, result)
	}

	// Sort by combined score
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Limit to top-k
	if len(results) > topK {
		results = results[:topK]
	}

	return results
}
