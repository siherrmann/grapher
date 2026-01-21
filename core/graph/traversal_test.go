package graph

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/siherrmann/grapher/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockGraphDB is a mock implementation of GraphDB for testing
type MockGraphDB struct {
	chunks map[string]*model.Chunk
	edges  map[string][]*model.Edge
}

func NewMockGraphDB() *MockGraphDB {
	return &MockGraphDB{
		chunks: make(map[string]*model.Chunk),
		edges:  make(map[string][]*model.Edge),
	}
}

func (m *MockGraphDB) GetChunk(ctx context.Context, id string) (*model.Chunk, error) {
	chunk, ok := m.chunks[id]
	if !ok {
		return nil, assert.AnError
	}
	return chunk, nil
}

func (m *MockGraphDB) GetEdgesFromChunk(ctx context.Context, chunkID string, edgeTypes []model.EdgeType, followBidirectional bool) ([]*model.Edge, error) {
	edges, ok := m.edges[chunkID]
	if !ok {
		return []*model.Edge{}, nil
	}
	return edges, nil
}

func TestBFS(t *testing.T) {
	mockDB := NewMockGraphDB()

	// Create test graph: A -> B -> C
	//                     A -> D
	idA := uuid.New()
	idB := uuid.New()
	idC := uuid.New()
	idD := uuid.New()

	chunkA := &model.Chunk{ID: idA, Content: "Chunk A", Path: "doc.a"}
	chunkB := &model.Chunk{ID: idB, Content: "Chunk B", Path: "doc.b"}
	chunkC := &model.Chunk{ID: idC, Content: "Chunk C", Path: "doc.c"}
	chunkD := &model.Chunk{ID: idD, Content: "Chunk D", Path: "doc.d"}

	mockDB.chunks[idA.String()] = chunkA
	mockDB.chunks[idB.String()] = chunkB
	mockDB.chunks[idC.String()] = chunkC
	mockDB.chunks[idD.String()] = chunkD

	edgeAB := &model.Edge{SourceChunkID: &idA, TargetChunkID: &idB, EdgeType: model.EdgeTypeReference}
	edgeAD := &model.Edge{SourceChunkID: &idA, TargetChunkID: &idD, EdgeType: model.EdgeTypeReference}
	edgeBC := &model.Edge{SourceChunkID: &idB, TargetChunkID: &idC, EdgeType: model.EdgeTypeReference}

	mockDB.edges[idA.String()] = []*model.Edge{edgeAB, edgeAD}
	mockDB.edges[idB.String()] = []*model.Edge{edgeBC}

	t.Run("BFS from source with max hops 1", func(t *testing.T) {
		results, err := BFS(context.Background(), mockDB, idA, 1, []model.EdgeType{}, false)

		assert.NoError(t, err, "Expected BFS to not return an error")
		require.NotEmpty(t, results, "Expected results")
		assert.Equal(t, idA, results[0].Chunk.ID, "Expected first result to be source")
		assert.Equal(t, 0, results[0].Distance, "Expected source distance to be 0")

		// Should include A, B, and D (1-hop neighbors)
		assert.LessOrEqual(t, len(results), 3, "Expected at most 3 results for max hops 1")
	})

	t.Run("BFS from source with max hops 2", func(t *testing.T) {
		results, err := BFS(context.Background(), mockDB, idA, 2, []model.EdgeType{}, false)

		assert.NoError(t, err, "Expected BFS to not return an error")
		require.NotEmpty(t, results, "Expected results")

		// Should include A, B, C, D
		assert.GreaterOrEqual(t, len(results), 1, "Expected at least the source node")

		// Verify source is first
		assert.Equal(t, idA, results[0].Chunk.ID, "Expected first result to be source")
		assert.Equal(t, 0, results[0].Distance, "Expected source distance to be 0")
	})

	t.Run("BFS with edge type filter", func(t *testing.T) {
		results, err := BFS(context.Background(), mockDB, idA, 2, []model.EdgeType{model.EdgeTypeReference}, false)

		assert.NoError(t, err, "Expected BFS to not return an error")
		require.NotEmpty(t, results, "Expected results")
	})

	t.Run("BFS from isolated node", func(t *testing.T) {
		isolatedID := uuid.New()
		isolatedChunk := &model.Chunk{ID: isolatedID, Content: "Isolated", Path: "doc.isolated"}
		mockDB.chunks[isolatedID.String()] = isolatedChunk

		results, err := BFS(context.Background(), mockDB, isolatedID, 2, []model.EdgeType{}, false)

		assert.NoError(t, err, "Expected BFS to not return an error")
		require.Len(t, results, 1, "Expected only source node for isolated chunk")
		assert.Equal(t, isolatedID, results[0].Chunk.ID, "Expected result to be isolated chunk")
		assert.Equal(t, 0, results[0].Distance, "Expected distance to be 0")
	})

	t.Run("BFS with max hops 0", func(t *testing.T) {
		results, err := BFS(context.Background(), mockDB, idA, 0, []model.EdgeType{}, false)

		assert.NoError(t, err, "Expected BFS to not return an error")
		require.Len(t, results, 1, "Expected only source node for max hops 0")
		assert.Equal(t, idA, results[0].Chunk.ID, "Expected result to be source")
		assert.Equal(t, 0, results[0].Distance, "Expected distance to be 0")
	})

	t.Run("BFS with bidirectional edges", func(t *testing.T) {
		bidirID1 := uuid.New()
		bidirID2 := uuid.New()

		bidirChunk1 := &model.Chunk{ID: bidirID1, Content: "Bidir 1", Path: "doc.bidir1"}
		bidirChunk2 := &model.Chunk{ID: bidirID2, Content: "Bidir 2", Path: "doc.bidir2"}

		mockDB.chunks[bidirID1.String()] = bidirChunk1
		mockDB.chunks[bidirID2.String()] = bidirChunk2

		bidirEdge := &model.Edge{
			SourceChunkID: &bidirID1,
			TargetChunkID: &bidirID2,
			EdgeType:      model.EdgeTypeSemantic,
			Bidirectional: true,
		}

		mockDB.edges[bidirID1.String()] = []*model.Edge{bidirEdge}
		mockDB.edges[bidirID2.String()] = []*model.Edge{bidirEdge}

		results, err := BFS(context.Background(), mockDB, bidirID1, 1, []model.EdgeType{}, true)

		assert.NoError(t, err, "Expected BFS to not return an error")
		require.GreaterOrEqual(t, len(results), 1, "Expected at least source node")
	})

	t.Run("BFS bidirectional backward traversal (target to source)", func(t *testing.T) {
		// Test that when traversing from target, bidirectional edge allows going back to source
		sourceID := uuid.New()
		targetID := uuid.New()

		sourceChunk := &model.Chunk{ID: sourceID, Content: "Source", Path: "doc.source"}
		targetChunk := &model.Chunk{ID: targetID, Content: "Target", Path: "doc.target"}

		mockDB.chunks[sourceID.String()] = sourceChunk
		mockDB.chunks[targetID.String()] = targetChunk

		// Edge goes source -> target, but is bidirectional
		bidirEdge := &model.Edge{
			SourceChunkID: &sourceID,
			TargetChunkID: &targetID,
			EdgeType:      model.EdgeTypeSemantic,
			Bidirectional: true,
		}

		mockDB.edges[targetID.String()] = []*model.Edge{bidirEdge}

		// Start from target, should be able to reach source via bidirectional edge
		results, err := BFS(context.Background(), mockDB, targetID, 1, []model.EdgeType{}, true)

		assert.NoError(t, err, "Expected BFS to not return an error")
		require.GreaterOrEqual(t, len(results), 2, "Expected at least source and target nodes")

		// Verify target is first (source node of traversal)
		assert.Equal(t, targetID, results[0].Chunk.ID, "Expected first result to be target")
		assert.Equal(t, 0, results[0].Distance, "Expected target distance to be 0")

		// Verify source was reached via bidirectional edge
		foundSource := false
		for _, result := range results {
			if result.Chunk.ID == sourceID {
				foundSource = true
				assert.Equal(t, 1, result.Distance, "Expected source distance to be 1")
				break
			}
		}
		assert.True(t, foundSource, "Expected to reach source via bidirectional edge")
	})

	t.Run("BFS skips entity edges (edges without chunk IDs)", func(t *testing.T) {
		// Test that edges with entity IDs instead of chunk IDs are skipped
		chunkID := uuid.New()
		entityID := uuid.New()

		chunk := &model.Chunk{ID: chunkID, Content: "Chunk", Path: "doc.chunk"}
		mockDB.chunks[chunkID.String()] = chunk

		// Entity edge: has SourceEntityID instead of SourceChunkID
		entityEdge := &model.Edge{
			SourceEntityID: &entityID,
			TargetChunkID:  &chunkID,
			EdgeType:       model.EdgeTypeReference,
		}

		mockDB.edges[chunkID.String()] = []*model.Edge{entityEdge}

		results, err := BFS(context.Background(), mockDB, chunkID, 1, []model.EdgeType{}, false)

		assert.NoError(t, err, "Expected BFS to not return an error")
		require.Len(t, results, 1, "Expected only source chunk (entity edges should be skipped)")
		assert.Equal(t, chunkID, results[0].Chunk.ID, "Expected result to be source chunk")
	})

	t.Run("BFS skips invalid edges (nil chunk IDs)", func(t *testing.T) {
		// Test that edges with nil chunk IDs are skipped
		chunkID := uuid.New()
		chunk := &model.Chunk{ID: chunkID, Content: "Chunk", Path: "doc.chunk"}
		mockDB.chunks[chunkID.String()] = chunk

		// Invalid edges with nil pointers
		invalidEdges := []*model.Edge{
			{SourceChunkID: nil, TargetChunkID: &chunkID, EdgeType: model.EdgeTypeReference},
			{SourceChunkID: &chunkID, TargetChunkID: nil, EdgeType: model.EdgeTypeReference},
			{SourceChunkID: nil, TargetChunkID: nil, EdgeType: model.EdgeTypeReference},
		}

		mockDB.edges[chunkID.String()] = invalidEdges

		results, err := BFS(context.Background(), mockDB, chunkID, 1, []model.EdgeType{}, false)

		assert.NoError(t, err, "Expected BFS to not return an error")
		require.Len(t, results, 1, "Expected only source chunk (invalid edges should be skipped)")
		assert.Equal(t, chunkID, results[0].Chunk.ID, "Expected result to be source chunk")
	})

	t.Run("BFS handles mixed edge directions correctly", func(t *testing.T) {
		// Test that edges are followed in the correct direction
		node1ID := uuid.New()
		node2ID := uuid.New()
		node3ID := uuid.New()

		node1 := &model.Chunk{ID: node1ID, Content: "Node 1", Path: "doc.1"}
		node2 := &model.Chunk{ID: node2ID, Content: "Node 2", Path: "doc.2"}
		node3 := &model.Chunk{ID: node3ID, Content: "Node 3", Path: "doc.3"}

		mockDB.chunks[node1ID.String()] = node1
		mockDB.chunks[node2ID.String()] = node2
		mockDB.chunks[node3ID.String()] = node3

		// Forward edge: node1 -> node2
		forwardEdge := &model.Edge{
			SourceChunkID: &node1ID,
			TargetChunkID: &node2ID,
			EdgeType:      model.EdgeTypeReference,
			Bidirectional: false,
		}

		// Bidirectional edge: node1 <-> node3
		bidirEdge := &model.Edge{
			SourceChunkID: &node3ID,
			TargetChunkID: &node1ID,
			EdgeType:      model.EdgeTypeSemantic,
			Bidirectional: true,
		}

		mockDB.edges[node1ID.String()] = []*model.Edge{forwardEdge, bidirEdge}

		// Start from node1, should reach node2 (forward) and node3 (bidirectional)
		results, err := BFS(context.Background(), mockDB, node1ID, 1, []model.EdgeType{}, true)

		assert.NoError(t, err, "Expected BFS to not return an error")
		require.GreaterOrEqual(t, len(results), 2, "Expected at least node1 and its neighbors")

		// Verify node1 is first
		assert.Equal(t, node1ID, results[0].Chunk.ID, "Expected first result to be source")

		// Should find both node2 and node3
		foundNode2 := false
		foundNode3 := false
		for _, result := range results {
			if result.Chunk.ID == node2ID {
				foundNode2 = true
			}
			if result.Chunk.ID == node3ID {
				foundNode3 = true
			}
		}
		assert.True(t, foundNode2, "Expected to reach node2 via forward edge")
		assert.True(t, foundNode3, "Expected to reach node3 via bidirectional edge")
	})
}

func TestDFS(t *testing.T) {
	mockDB := NewMockGraphDB()

	// Create test graph: A -> B -> C
	//                     A -> D
	idA := uuid.New()
	idB := uuid.New()
	idC := uuid.New()
	idD := uuid.New()

	chunkA := &model.Chunk{ID: idA, Content: "Chunk A", Path: "doc.a"}
	chunkB := &model.Chunk{ID: idB, Content: "Chunk B", Path: "doc.b"}
	chunkC := &model.Chunk{ID: idC, Content: "Chunk C", Path: "doc.c"}
	chunkD := &model.Chunk{ID: idD, Content: "Chunk D", Path: "doc.d"}

	mockDB.chunks[idA.String()] = chunkA
	mockDB.chunks[idB.String()] = chunkB
	mockDB.chunks[idC.String()] = chunkC
	mockDB.chunks[idD.String()] = chunkD

	edgeAB := &model.Edge{SourceChunkID: &idA, TargetChunkID: &idB, EdgeType: model.EdgeTypeReference}
	edgeAD := &model.Edge{SourceChunkID: &idA, TargetChunkID: &idD, EdgeType: model.EdgeTypeReference}
	edgeBC := &model.Edge{SourceChunkID: &idB, TargetChunkID: &idC, EdgeType: model.EdgeTypeReference}

	mockDB.edges[idA.String()] = []*model.Edge{edgeAB, edgeAD}
	mockDB.edges[idB.String()] = []*model.Edge{edgeBC}

	t.Run("DFS from source with max hops 1", func(t *testing.T) {
		results, err := DFS(context.Background(), mockDB, idA, 1, []model.EdgeType{}, false)

		assert.NoError(t, err, "Expected DFS to not return an error")
		require.NotEmpty(t, results, "Expected results")
		assert.Equal(t, idA, results[0].Chunk.ID, "Expected first result to be source")
		assert.Equal(t, 0, results[0].Distance, "Expected source distance to be 0")
	})

	t.Run("DFS from source with max hops 2", func(t *testing.T) {
		results, err := DFS(context.Background(), mockDB, idA, 2, []model.EdgeType{}, false)

		assert.NoError(t, err, "Expected DFS to not return an error")
		require.NotEmpty(t, results, "Expected results")

		// Verify source is first
		assert.Equal(t, idA, results[0].Chunk.ID, "Expected first result to be source")
		assert.Equal(t, 0, results[0].Distance, "Expected source distance to be 0")
	})

	t.Run("DFS from isolated node", func(t *testing.T) {
		isolatedID := uuid.New()
		isolatedChunk := &model.Chunk{ID: isolatedID, Content: "Isolated", Path: "doc.isolated"}
		mockDB.chunks[isolatedID.String()] = isolatedChunk

		results, err := DFS(context.Background(), mockDB, isolatedID, 2, []model.EdgeType{}, false)

		assert.NoError(t, err, "Expected DFS to not return an error")
		require.Len(t, results, 1, "Expected only source node for isolated chunk")
		assert.Equal(t, isolatedID, results[0].Chunk.ID, "Expected result to be isolated chunk")
	})

	t.Run("DFS with max hops 0", func(t *testing.T) {
		results, err := DFS(context.Background(), mockDB, idA, 0, []model.EdgeType{}, false)

		assert.NoError(t, err, "Expected DFS to not return an error")
		require.Len(t, results, 1, "Expected only source node for max hops 0")
		assert.Equal(t, idA, results[0].Chunk.ID, "Expected result to be source")
	})

	t.Run("DFS bidirectional backward traversal (target to source)", func(t *testing.T) {
		// Test that when traversing from target, bidirectional edge allows going back to source
		sourceID := uuid.New()
		targetID := uuid.New()

		sourceChunk := &model.Chunk{ID: sourceID, Content: "Source", Path: "doc.source"}
		targetChunk := &model.Chunk{ID: targetID, Content: "Target", Path: "doc.target"}

		mockDB.chunks[sourceID.String()] = sourceChunk
		mockDB.chunks[targetID.String()] = targetChunk

		// Edge goes source -> target, but is bidirectional
		bidirEdge := &model.Edge{
			SourceChunkID: &sourceID,
			TargetChunkID: &targetID,
			EdgeType:      model.EdgeTypeSemantic,
			Bidirectional: true,
		}

		mockDB.edges[targetID.String()] = []*model.Edge{bidirEdge}

		// Start from target, should be able to reach source via bidirectional edge
		results, err := DFS(context.Background(), mockDB, targetID, 1, []model.EdgeType{}, true)

		assert.NoError(t, err, "Expected DFS to not return an error")
		require.GreaterOrEqual(t, len(results), 2, "Expected at least source and target nodes")

		// Verify target is first (source node of traversal)
		assert.Equal(t, targetID, results[0].Chunk.ID, "Expected first result to be target")
		assert.Equal(t, 0, results[0].Distance, "Expected target distance to be 0")

		// Verify source was reached via bidirectional edge
		foundSource := false
		for _, result := range results {
			if result.Chunk.ID == sourceID {
				foundSource = true
				assert.Equal(t, 1, result.Distance, "Expected source distance to be 1")
				break
			}
		}
		assert.True(t, foundSource, "Expected to reach source via bidirectional edge")
	})

	t.Run("DFS skips entity edges (edges without chunk IDs)", func(t *testing.T) {
		// Test that edges with entity IDs instead of chunk IDs are skipped
		chunkID := uuid.New()
		entityID := uuid.New()

		chunk := &model.Chunk{ID: chunkID, Content: "Chunk", Path: "doc.chunk"}
		mockDB.chunks[chunkID.String()] = chunk

		// Entity edge: has SourceEntityID instead of SourceChunkID
		entityEdge := &model.Edge{
			SourceEntityID: &entityID,
			TargetChunkID:  &chunkID,
			EdgeType:       model.EdgeTypeReference,
		}

		mockDB.edges[chunkID.String()] = []*model.Edge{entityEdge}

		results, err := DFS(context.Background(), mockDB, chunkID, 1, []model.EdgeType{}, false)

		assert.NoError(t, err, "Expected DFS to not return an error")
		require.Len(t, results, 1, "Expected only source chunk (entity edges should be skipped)")
		assert.Equal(t, chunkID, results[0].Chunk.ID, "Expected result to be source chunk")
	})

	t.Run("DFS skips invalid edges (nil chunk IDs)", func(t *testing.T) {
		// Test that edges with nil chunk IDs are skipped
		chunkID := uuid.New()
		chunk := &model.Chunk{ID: chunkID, Content: "Chunk", Path: "doc.chunk"}
		mockDB.chunks[chunkID.String()] = chunk

		// Invalid edges with nil pointers
		invalidEdges := []*model.Edge{
			{SourceChunkID: nil, TargetChunkID: &chunkID, EdgeType: model.EdgeTypeReference},
			{SourceChunkID: &chunkID, TargetChunkID: nil, EdgeType: model.EdgeTypeReference},
			{SourceChunkID: nil, TargetChunkID: nil, EdgeType: model.EdgeTypeReference},
		}

		mockDB.edges[chunkID.String()] = invalidEdges

		results, err := DFS(context.Background(), mockDB, chunkID, 1, []model.EdgeType{}, false)

		assert.NoError(t, err, "Expected DFS to not return an error")
		require.Len(t, results, 1, "Expected only source chunk (invalid edges should be skipped)")
		assert.Equal(t, chunkID, results[0].Chunk.ID, "Expected result to be source chunk")
	})

	t.Run("DFS handles mixed edge directions correctly", func(t *testing.T) {
		// Test that edges are followed in the correct direction
		node1ID := uuid.New()
		node2ID := uuid.New()
		node3ID := uuid.New()

		node1 := &model.Chunk{ID: node1ID, Content: "Node 1", Path: "doc.1"}
		node2 := &model.Chunk{ID: node2ID, Content: "Node 2", Path: "doc.2"}
		node3 := &model.Chunk{ID: node3ID, Content: "Node 3", Path: "doc.3"}

		mockDB.chunks[node1ID.String()] = node1
		mockDB.chunks[node2ID.String()] = node2
		mockDB.chunks[node3ID.String()] = node3

		// Forward edge: node1 -> node2
		forwardEdge := &model.Edge{
			SourceChunkID: &node1ID,
			TargetChunkID: &node2ID,
			EdgeType:      model.EdgeTypeReference,
			Bidirectional: false,
		}

		// Bidirectional edge: node1 <-> node3
		bidirEdge := &model.Edge{
			SourceChunkID: &node3ID,
			TargetChunkID: &node1ID,
			EdgeType:      model.EdgeTypeSemantic,
			Bidirectional: true,
		}

		mockDB.edges[node1ID.String()] = []*model.Edge{forwardEdge, bidirEdge}

		// Start from node1, should reach node2 (forward) and node3 (bidirectional)
		results, err := DFS(context.Background(), mockDB, node1ID, 1, []model.EdgeType{}, true)

		assert.NoError(t, err, "Expected DFS to not return an error")
		require.GreaterOrEqual(t, len(results), 2, "Expected at least node1 and its neighbors")

		// Verify node1 is first
		assert.Equal(t, node1ID, results[0].Chunk.ID, "Expected first result to be source")

		// Should find both node2 and node3
		foundNode2 := false
		foundNode3 := false
		for _, result := range results {
			if result.Chunk.ID == node2ID {
				foundNode2 = true
			}
			if result.Chunk.ID == node3ID {
				foundNode3 = true
			}
		}
		assert.True(t, foundNode2, "Expected to reach node2 via forward edge")
		assert.True(t, foundNode3, "Expected to reach node3 via bidirectional edge")
	})
}

func TestGetNeighbors(t *testing.T) {
	mockDB := NewMockGraphDB()

	// Create test graph
	sourceID := uuid.New()
	neighbor1ID := uuid.New()
	neighbor2ID := uuid.New()

	sourceChunk := &model.Chunk{ID: sourceID, Content: "Source", Path: "doc.source"}
	neighbor1Chunk := &model.Chunk{ID: neighbor1ID, Content: "Neighbor 1", Path: "doc.n1"}
	neighbor2Chunk := &model.Chunk{ID: neighbor2ID, Content: "Neighbor 2", Path: "doc.n2"}

	mockDB.chunks[sourceID.String()] = sourceChunk
	mockDB.chunks[neighbor1ID.String()] = neighbor1Chunk
	mockDB.chunks[neighbor2ID.String()] = neighbor2Chunk

	edge1 := &model.Edge{SourceChunkID: &sourceID, TargetChunkID: &neighbor1ID, EdgeType: model.EdgeTypeReference}
	edge2 := &model.Edge{SourceChunkID: &sourceID, TargetChunkID: &neighbor2ID, EdgeType: model.EdgeTypeSemantic}

	mockDB.edges[sourceID.String()] = []*model.Edge{edge1, edge2}

	t.Run("Get neighbors of source chunk", func(t *testing.T) {
		neighbors, err := GetNeighbors(context.Background(), mockDB, sourceID, []model.EdgeType{}, false)

		assert.NoError(t, err, "Expected GetNeighbors to not return an error")
		require.NotEmpty(t, neighbors, "Expected neighbors")
		assert.LessOrEqual(t, len(neighbors), 2, "Expected at most 2 neighbors")
	})

	t.Run("Get neighbors with edge type filter", func(t *testing.T) {
		neighbors, err := GetNeighbors(context.Background(), mockDB, sourceID, []model.EdgeType{model.EdgeTypeReference}, false)

		assert.NoError(t, err, "Expected GetNeighbors to not return an error")
		require.NotNil(t, neighbors, "Expected result to not be nil")
	})

	t.Run("Get neighbors of isolated chunk", func(t *testing.T) {
		isolatedID := uuid.New()
		isolatedChunk := &model.Chunk{ID: isolatedID, Content: "Isolated", Path: "doc.isolated"}
		mockDB.chunks[isolatedID.String()] = isolatedChunk

		neighbors, err := GetNeighbors(context.Background(), mockDB, isolatedID, []model.EdgeType{}, false)

		assert.NoError(t, err, "Expected GetNeighbors to not return an error")
		assert.Empty(t, neighbors, "Expected no neighbors for isolated chunk")
	})
}
