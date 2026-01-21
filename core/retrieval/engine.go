package retrieval

import (
	"context"

	"github.com/google/uuid"
	"github.com/siherrmann/grapher/database"
	"github.com/siherrmann/grapher/model"
)

// Engine provides hybrid retrieval and graph traversal capabilities
type Engine struct {
	chunks   *database.ChunksDBHandler
	edges    *database.EdgesDBHandler
	entities *database.EntitiesDBHandler
}

// NewEngine creates a new retrieval engine
func NewEngine(chunks *database.ChunksDBHandler, edges *database.EdgesDBHandler, entities *database.EntitiesDBHandler) *Engine {
	return &Engine{
		chunks:   chunks,
		edges:    edges,
		entities: entities,
	}
}

// VectorRetrieve performs pure vector similarity search
func (e *Engine) VectorRetrieve(ctx context.Context, embedding []float32, config *model.QueryConfig) ([]*model.RetrievalResult, error) {
	chunks, err := e.chunks.SelectChunksBySimilarity(embedding, config.TopK, config.SimilarityThreshold, config.DocumentRIDs)
	if err != nil {
		return nil, err
	}

	results := make([]*model.RetrievalResult, len(chunks))
	for i, chunk := range chunks {
		score := 0.0
		if chunk.Similarity != nil {
			score = *chunk.Similarity
		}
		results[i] = &model.RetrievalResult{
			Chunk:           chunk,
			Score:           score,
			SimilarityScore: score,
			GraphDistance:   0,
			RetrievalMethod: "vector",
		}
	}

	return results, nil
}

// GetNeighbors retrieves immediate neighbors of a chunk
func (e *Engine) GetNeighbors(ctx context.Context, chunkID uuid.UUID, edgeTypes []model.EdgeType, followBidirectional bool) ([]*model.Chunk, error) {
	allEdges, err := e.edges.SelectEdgesFromChunk(chunkID, nil)
	if err != nil {
		return nil, err
	}

	// Filter by edge types if specified
	var edges []*model.Edge
	if len(edgeTypes) == 0 {
		edges = allEdges
	} else {
		for _, edge := range allEdges {
			for _, edgeType := range edgeTypes {
				if edge.EdgeType == edgeType {
					edges = append(edges, edge)
					break
				}
			}
		}
	}

	var neighbors []*model.Chunk
	visited := make(map[uuid.UUID]bool)

	for _, edge := range edges {
		var targetID uuid.UUID

		// Determine target based on edge direction
		if edge.SourceChunkID != nil && *edge.SourceChunkID == chunkID && edge.TargetChunkID != nil {
			targetID = *edge.TargetChunkID
		} else if edge.Bidirectional && edge.TargetChunkID != nil && *edge.TargetChunkID == chunkID && edge.SourceChunkID != nil {
			targetID = *edge.SourceChunkID
		} else {
			continue
		}

		// Skip duplicates
		if visited[targetID] {
			continue
		}
		visited[targetID] = true

		// Get chunk
		chunk, err := e.chunks.SelectChunk(targetID)
		if err != nil {
			continue
		}

		neighbors = append(neighbors, chunk)
	}

	return neighbors, nil
}

// GetHierarchicalContext retrieves hierarchical context using ltree
func (e *Engine) GetHierarchicalContext(ctx context.Context, path string, config *model.QueryConfig) ([]*model.Chunk, error) {
	var allChunks []*model.Chunk

	if config.IncludeAncestors {
		chunks, err := e.chunks.SelectAllChunksByPathAncestor(path)
		if err == nil {
			allChunks = append(allChunks, chunks...)
		}
	}

	if config.IncludeDescendants {
		chunks, err := e.chunks.SelectAllChunksByPathDescendant(path)
		if err == nil {
			allChunks = append(allChunks, chunks...)
		}
	}

	if config.IncludeSiblings {
		chunks, err := e.chunks.SelectSiblingChunks(path)
		if err == nil {
			allChunks = append(allChunks, chunks...)
		}
	}

	return allChunks, nil
}

// TraversalResult contains a chunk and its distance from the source
type TraversalResult struct {
	Chunk    *model.Chunk
	Distance int
	Path     []uuid.UUID // Path from source to this chunk
}

// BFS performs breadth-first search from a source chunk
func (e *Engine) BFS(ctx context.Context, sourceID uuid.UUID, maxHops int, edgeTypes []model.EdgeType, followBidirectional bool) ([]*TraversalResult, error) {
	visited := make(map[uuid.UUID]bool)
	queue := []TraversalResult{{
		Chunk:    nil,
		Distance: 0,
		Path:     []uuid.UUID{sourceID},
	}}

	// Get source chunk
	sourceChunk, err := e.chunks.SelectChunk(sourceID)
	if err != nil {
		return nil, err
	}
	queue[0].Chunk = sourceChunk

	var results []*TraversalResult
	visited[sourceID] = true

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		results = append(results, &current)

		// Stop if we've reached max hops
		if current.Distance >= maxHops {
			continue
		}

		// Get edges from current chunk
		allEdges, err := e.edges.SelectEdgesFromChunk(current.Chunk.ID, nil)
		if err != nil {
			return nil, err
		}

		// Filter by edge types if specified
		var edges []*model.Edge
		if len(edgeTypes) == 0 {
			edges = allEdges
		} else {
			for _, edge := range allEdges {
				for _, edgeType := range edgeTypes {
					if edge.EdgeType == edgeType {
						edges = append(edges, edge)
						break
					}
				}
			}
		}

		// Process each edge
		for _, edge := range edges {
			var targetID uuid.UUID

			// Determine target based on edge direction
			if edge.SourceChunkID != nil && *edge.SourceChunkID == current.Chunk.ID && edge.TargetChunkID != nil {
				targetID = *edge.TargetChunkID
			} else if edge.Bidirectional && edge.TargetChunkID != nil && *edge.TargetChunkID == current.Chunk.ID && edge.SourceChunkID != nil {
				targetID = *edge.SourceChunkID
			} else {
				continue // Skip entity edges or invalid edges
			}

			// Skip if already visited
			if visited[targetID] {
				continue
			}

			// Get target chunk
			targetChunk, err := e.chunks.SelectChunk(targetID)
			if err != nil {
				continue // Skip if chunk not found
			}

			visited[targetID] = true

			// Create new path
			newPath := make([]uuid.UUID, len(current.Path))
			copy(newPath, current.Path)
			newPath = append(newPath, targetID)

			queue = append(queue, TraversalResult{
				Chunk:    targetChunk,
				Distance: current.Distance + 1,
				Path:     newPath,
			})
		}
	}

	return results, nil
}

// DFS performs depth-first search from a source chunk
func (e *Engine) DFS(ctx context.Context, sourceID uuid.UUID, maxHops int, edgeTypes []model.EdgeType, followBidirectional bool) ([]*TraversalResult, error) {
	visited := make(map[uuid.UUID]bool)
	var results []*TraversalResult

	// Get source chunk
	sourceChunk, err := e.chunks.SelectChunk(sourceID)
	if err != nil {
		return nil, err
	}

	// Start recursive DFS
	e.dfsRecursive(ctx, sourceChunk, 0, maxHops, []uuid.UUID{sourceID}, edgeTypes, followBidirectional, visited, &results)

	return results, nil
}

// dfsRecursive is the recursive helper for DFS
func (e *Engine) dfsRecursive(
	ctx context.Context,
	current *model.Chunk,
	distance int,
	maxHops int,
	path []uuid.UUID,
	edgeTypes []model.EdgeType,
	followBidirectional bool,
	visited map[uuid.UUID]bool,
	results *[]*TraversalResult,
) {
	// Mark as visited
	visited[current.ID] = true

	// Add to results
	pathCopy := make([]uuid.UUID, len(path))
	copy(pathCopy, path)
	*results = append(*results, &TraversalResult{
		Chunk:    current,
		Distance: distance,
		Path:     pathCopy,
	})

	// Stop if we've reached max hops
	if distance >= maxHops {
		return
	}

	// Get edges from current chunk
	allEdges, err := e.edges.SelectEdgesFromChunk(current.ID, nil)
	if err != nil {
		return
	}

	// Filter by edge types if specified
	var edges []*model.Edge
	if len(edgeTypes) == 0 {
		edges = allEdges
	} else {
		for _, edge := range allEdges {
			for _, edgeType := range edgeTypes {
				if edge.EdgeType == edgeType {
					edges = append(edges, edge)
					break
				}
			}
		}
	}

	// Process each edge
	for _, edge := range edges {
		var targetID uuid.UUID

		// Determine target based on edge direction
		if edge.SourceChunkID != nil && *edge.SourceChunkID == current.ID && edge.TargetChunkID != nil {
			targetID = *edge.TargetChunkID
		} else if edge.Bidirectional && edge.TargetChunkID != nil && *edge.TargetChunkID == current.ID && edge.SourceChunkID != nil {
			targetID = *edge.SourceChunkID
		} else {
			continue // Skip entity edges or invalid edges
		}

		// Skip if already visited
		if visited[targetID] {
			continue
		}

		// Get target chunk
		targetChunk, err := e.chunks.SelectChunk(targetID)
		if err != nil {
			continue // Skip if chunk not found
		}

		// Create new path
		newPath := make([]uuid.UUID, len(path))
		copy(newPath, path)
		newPath = append(newPath, targetID)

		// Recurse
		e.dfsRecursive(ctx, targetChunk, distance+1, maxHops, newPath, edgeTypes, followBidirectional, visited, results)
	}
}
