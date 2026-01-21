package graph

import (
	"context"

	"github.com/google/uuid"
	"github.com/siherrmann/grapher/model"
)

// GraphDB defines the interface for graph operations
type GraphDB interface {
	GetChunk(ctx context.Context, id string) (*model.Chunk, error)
	GetEdgesFromChunk(ctx context.Context, chunkID string, edgeTypes []model.EdgeType, followBidirectional bool) ([]*model.Edge, error)
}

// TraversalResult contains a chunk and its distance from the source
type TraversalResult struct {
	Chunk    *model.Chunk
	Distance int
	Path     []uuid.UUID // Path from source to this chunk
}

// BFS performs breadth-first search from a source chunk
func BFS(ctx context.Context, db GraphDB, sourceID uuid.UUID, maxHops int, edgeTypes []model.EdgeType, followBidirectional bool) ([]*TraversalResult, error) {
	visited := make(map[uuid.UUID]bool)
	queue := []TraversalResult{{
		Chunk:    nil,
		Distance: 0,
		Path:     []uuid.UUID{sourceID},
	}}

	// Get source chunk
	sourceChunk, err := db.GetChunk(ctx, sourceID.String())
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
		edges, err := db.GetEdgesFromChunk(ctx, current.Chunk.ID.String(), edgeTypes, followBidirectional)
		if err != nil {
			return nil, err
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
			targetChunk, err := db.GetChunk(ctx, targetID.String())
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
func DFS(ctx context.Context, db GraphDB, sourceID uuid.UUID, maxHops int, edgeTypes []model.EdgeType, followBidirectional bool) ([]*TraversalResult, error) {
	visited := make(map[uuid.UUID]bool)
	var results []*TraversalResult

	// Get source chunk
	sourceChunk, err := db.GetChunk(ctx, sourceID.String())
	if err != nil {
		return nil, err
	}

	// Start recursive DFS
	dfsRecursive(ctx, db, sourceChunk, 0, maxHops, []uuid.UUID{sourceID}, edgeTypes, followBidirectional, visited, &results)

	return results, nil
}

// dfsRecursive is the recursive helper for DFS
func dfsRecursive(
	ctx context.Context,
	db GraphDB,
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
	edges, err := db.GetEdgesFromChunk(ctx, current.ID.String(), edgeTypes, followBidirectional)
	if err != nil {
		return
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
		targetChunk, err := db.GetChunk(ctx, targetID.String())
		if err != nil {
			continue // Skip if chunk not found
		}

		// Create new path
		newPath := make([]uuid.UUID, len(path))
		copy(newPath, path)
		newPath = append(newPath, targetID)

		// Recurse
		dfsRecursive(ctx, db, targetChunk, distance+1, maxHops, newPath, edgeTypes, followBidirectional, visited, results)
	}
}

// GetNeighbors retrieves immediate neighbors (1-hop) of a chunk
func GetNeighbors(ctx context.Context, db GraphDB, chunkID uuid.UUID, edgeTypes []model.EdgeType, followBidirectional bool) ([]*model.Chunk, error) {
	results, err := BFS(ctx, db, chunkID, 1, edgeTypes, followBidirectional)
	if err != nil {
		return nil, err
	}

	// Skip the source chunk itself (first result)
	neighbors := make([]*model.Chunk, 0, len(results)-1)
	for i := 1; i < len(results); i++ {
		neighbors = append(neighbors, results[i].Chunk)
	}

	return neighbors, nil
}
