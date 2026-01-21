package database

import (
	"testing"
	"time"

	"github.com/siherrmann/grapher/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEdgesNewEdgesDBHandler(t *testing.T) {
	database := initDB(t)

	t.Run("Valid call NewEdgesDBHandler", func(t *testing.T) {
		edgesDbHandler, err := NewEdgesDBHandler(database, true)
		assert.NoError(t, err, "Expected NewEdgesDBHandler to not return an error")
		require.NotNil(t, edgesDbHandler, "Expected NewEdgesDBHandler to return a non-nil instance")
		require.NotNil(t, edgesDbHandler.db, "Expected NewEdgesDBHandler to have a non-nil database instance")
		require.NotNil(t, edgesDbHandler.db.Instance, "Expected NewEdgesDBHandler to have a non-nil database connection instance")
	})

	t.Run("Invalid call NewEdgesDBHandler with nil database", func(t *testing.T) {
		_, err := NewEdgesDBHandler(nil, false)
		assert.Error(t, err, "Expected error when creating EdgesDBHandler with nil database")
		assert.Contains(t, err.Error(), "database connection is nil", "Expected specific error message for nil database connection")
	})
}

func TestEdgesInsert(t *testing.T) {
	database := initDB(t)

	edgesDbHandler, err := NewEdgesDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, edgesDbHandler, 384, true)
	require.NoError(t, err)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	// Create a document and chunks
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	chunk1 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 1",
		Path:       "root.1",
		Metadata:   map[string]interface{}{},
	}
	err = chunksDbHandler.InsertChunk(chunk1)
	require.NoError(t, err)

	chunk2 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 2",
		Path:       "root.2",
		Metadata:   map[string]interface{}{},
	}
	err = chunksDbHandler.InsertChunk(chunk2)
	require.NoError(t, err)

	t.Run("Insert edge between chunks", func(t *testing.T) {
		edge := &model.Edge{
			SourceChunkID: &chunk1.ID,
			TargetChunkID: &chunk2.ID,
			EdgeType:      model.EdgeTypeReference,
			Weight:        1.0,
			Bidirectional: false,
			Metadata:      map[string]interface{}{"context": "test"},
		}

		err := edgesDbHandler.InsertEdge(edge)
		assert.NoError(t, err, "Expected Insert to not return an error")
		assert.NotEmpty(t, edge.ID, "Expected inserted edge to have an ID")
		assert.WithinDuration(t, edge.CreatedAt, time.Now(), 2*time.Second, "Expected CreatedAt to be set")

		// Cleanup
		edgesDbHandler.DeleteEdge(edge.ID)
	})

	// Cleanup
	chunksDbHandler.DeleteChunk(chunk1.ID)
	chunksDbHandler.DeleteChunk(chunk2.ID)
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestEdgesGet(t *testing.T) {
	database := initDB(t)

	edgesDbHandler, err := NewEdgesDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, edgesDbHandler, 384, true)
	require.NoError(t, err)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	// Create document and chunks
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	chunk1 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 1",
		Path:       "root",
		Metadata:   map[string]interface{}{},
	}
	err = chunksDbHandler.InsertChunk(chunk1)
	require.NoError(t, err)

	chunk2 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 2",
		Path:       "root",
		Metadata:   map[string]interface{}{},
	}
	err = chunksDbHandler.InsertChunk(chunk2)
	require.NoError(t, err)

	// Create an edge
	edge := &model.Edge{
		SourceChunkID: &chunk1.ID,
		TargetChunkID: &chunk2.ID,
		EdgeType:      model.EdgeTypeReference,
		Weight:        0.8,
		Metadata:      map[string]interface{}{},
	}
	err = edgesDbHandler.InsertEdge(edge)
	require.NoError(t, err)

	// Test Get
	retrievedEdge, err := edgesDbHandler.SelectEdge(edge.ID)
	assert.NoError(t, err, "Expected Get to not return an error")
	assert.NotNil(t, retrievedEdge, "Expected Get to return a non-nil edge")
	assert.Equal(t, edge.ID, retrievedEdge.ID, "Expected edge IDs to match")
	assert.Equal(t, edge.EdgeType, retrievedEdge.EdgeType, "Expected edge types to match")

	// Cleanup
	edgesDbHandler.DeleteEdge(edge.ID)
	chunksDbHandler.DeleteChunk(chunk1.ID)
	chunksDbHandler.DeleteChunk(chunk2.ID)
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestEdgesGetFromChunk(t *testing.T) {
	database := initDB(t)

	edgesDbHandler, err := NewEdgesDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, edgesDbHandler, 384, true)
	require.NoError(t, err)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	// Setup
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	documentsDbHandler.InsertDocument(doc)

	sourceChunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Source",
		Path:       "root",
		Metadata:   map[string]interface{}{},
	}
	chunksDbHandler.InsertChunk(sourceChunk)

	targetChunk1 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Target 1",
		Path:       "root",
		Metadata:   map[string]interface{}{},
	}
	chunksDbHandler.InsertChunk(targetChunk1)

	targetChunk2 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Target 2",
		Path:       "root",
		Metadata:   map[string]interface{}{},
	}
	chunksDbHandler.InsertChunk(targetChunk2)

	// Create edges from source chunk
	edge1 := &model.Edge{
		SourceChunkID: &sourceChunk.ID,
		TargetChunkID: &targetChunk1.ID,
		EdgeType:      model.EdgeTypeReference,
		Weight:        1.0,
		Metadata:      map[string]interface{}{},
	}
	edgesDbHandler.InsertEdge(edge1)

	edge2 := &model.Edge{
		SourceChunkID: &sourceChunk.ID,
		TargetChunkID: &targetChunk2.ID,
		EdgeType:      model.EdgeTypeSemantic,
		Weight:        0.9,
		Metadata:      map[string]interface{}{},
	}
	edgesDbHandler.InsertEdge(edge2)

	// Test GetFromChunk
	edges, err := edgesDbHandler.SelectEdgesFromChunk(sourceChunk.ID, nil)
	assert.NoError(t, err, "Expected GetFromChunk to not return an error")
	assert.Len(t, edges, 2, "Expected to find 2 edges from source chunk")

	// Test GetFromChunk with type filter
	refType := model.EdgeTypeReference
	refEdges, err := edgesDbHandler.SelectEdgesFromChunk(sourceChunk.ID, &refType)
	assert.NoError(t, err, "Expected GetFromChunk with type to not return an error")
	assert.Len(t, refEdges, 1, "Expected to find 1 reference edge")

	// Cleanup
	edgesDbHandler.DeleteEdge(edge1.ID)
	edgesDbHandler.DeleteEdge(edge2.ID)
	chunksDbHandler.DeleteChunk(sourceChunk.ID)
	chunksDbHandler.DeleteChunk(targetChunk1.ID)
	chunksDbHandler.DeleteChunk(targetChunk2.ID)
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestEdgesGetToChunk(t *testing.T) {
	database := initDB(t)

	edgesDbHandler, err := NewEdgesDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, edgesDbHandler, 384, true)
	require.NoError(t, err)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	// Setup
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	documentsDbHandler.InsertDocument(doc)

	chunk1 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 1",
		Path:       "root",
		Metadata:   map[string]interface{}{},
	}
	chunksDbHandler.InsertChunk(chunk1)

	chunk2 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 2",
		Path:       "root",
		Metadata:   map[string]interface{}{},
	}
	chunksDbHandler.InsertChunk(chunk2)

	targetChunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Target",
		Path:       "root",
		Metadata:   map[string]interface{}{},
	}
	chunksDbHandler.InsertChunk(targetChunk)

	// Create edges to target chunk
	edge1 := &model.Edge{
		SourceChunkID: &chunk1.ID,
		TargetChunkID: &targetChunk.ID,
		EdgeType:      model.EdgeTypeReference,
		Weight:        1.0,
		Metadata:      map[string]interface{}{},
	}
	edgesDbHandler.InsertEdge(edge1)

	edge2 := &model.Edge{
		SourceChunkID: &chunk2.ID,
		TargetChunkID: &targetChunk.ID,
		EdgeType:      model.EdgeTypeReference,
		Weight:        0.8,
		Metadata:      map[string]interface{}{},
	}
	edgesDbHandler.InsertEdge(edge2)

	// Test GetToChunk
	edges, err := edgesDbHandler.SelectEdgesToChunk(targetChunk.ID, nil)
	assert.NoError(t, err, "Expected GetToChunk to not return an error")
	assert.Len(t, edges, 2, "Expected to find 2 edges to target chunk")

	// Cleanup
	edgesDbHandler.DeleteEdge(edge1.ID)
	edgesDbHandler.DeleteEdge(edge2.ID)
	chunksDbHandler.DeleteChunk(chunk1.ID)
	chunksDbHandler.DeleteChunk(chunk2.ID)
	chunksDbHandler.DeleteChunk(targetChunk.ID)
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestEdgesDelete(t *testing.T) {
	database := initDB(t)

	edgesDbHandler, err := NewEdgesDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, edgesDbHandler, 384, true)
	require.NoError(t, err)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	// Setup
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	documentsDbHandler.InsertDocument(doc)

	chunk1 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 1",
		Path:       "root",
		Metadata:   map[string]interface{}{},
	}
	chunksDbHandler.InsertChunk(chunk1)

	chunk2 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 2",
		Path:       "root",
		Metadata:   map[string]interface{}{},
	}
	chunksDbHandler.InsertChunk(chunk2)

	edge := &model.Edge{
		SourceChunkID: &chunk1.ID,
		TargetChunkID: &chunk2.ID,
		EdgeType:      model.EdgeTypeReference,
		Weight:        1.0,
		Metadata:      map[string]interface{}{},
	}
	edgesDbHandler.InsertEdge(edge)

	// Test Delete
	err = edgesDbHandler.DeleteEdge(edge.ID)
	assert.NoError(t, err, "Expected Delete to not return an error")

	// Verify deletion
	_, err = edgesDbHandler.SelectEdge(edge.ID)
	assert.Error(t, err, "Expected Get to return an error for deleted edge")

	// Cleanup
	chunksDbHandler.DeleteChunk(chunk1.ID)
	chunksDbHandler.DeleteChunk(chunk2.ID)
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestEdgesUpdateWeight(t *testing.T) {
	database := initDB(t)

	edgesDbHandler, err := NewEdgesDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, edgesDbHandler, 384, true)
	require.NoError(t, err)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	// Setup
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	documentsDbHandler.InsertDocument(doc)

	chunk1 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 1",
		Path:       "root",
		Metadata:   map[string]interface{}{},
	}
	chunksDbHandler.InsertChunk(chunk1)

	chunk2 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 2",
		Path:       "root",
		Metadata:   map[string]interface{}{},
	}
	chunksDbHandler.InsertChunk(chunk2)

	edge := &model.Edge{
		SourceChunkID: &chunk1.ID,
		TargetChunkID: &chunk2.ID,
		EdgeType:      model.EdgeTypeReference,
		Weight:        0.5,
		Metadata:      map[string]interface{}{},
	}
	edgesDbHandler.InsertEdge(edge)

	// Test UpdateWeight
	newWeight := 0.9
	err = edgesDbHandler.UpdateEdgeWeight(edge.ID, newWeight)
	assert.NoError(t, err, "Expected UpdateWeight to not return an error")

	// Verify update
	retrievedEdge, err := edgesDbHandler.SelectEdge(edge.ID)
	require.NoError(t, err)
	assert.Equal(t, newWeight, retrievedEdge.Weight, "Expected weight to be updated")

	// Cleanup
	edgesDbHandler.DeleteEdge(edge.ID)
	chunksDbHandler.DeleteChunk(chunk1.ID)
	chunksDbHandler.DeleteChunk(chunk2.ID)
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestSelectEdgesConnectedToChunk(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, nil, 384, true)
	require.NoError(t, err)

	edgesDbHandler, err := NewEdgesDBHandler(database, true)
	require.NoError(t, err)

	// Create a document
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create chunks
	chunk1 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 1",
		Path:       "root.chunk1",
		Metadata:   map[string]interface{}{},
	}
	chunk2 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 2",
		Path:       "root.chunk2",
		Metadata:   map[string]interface{}{},
	}
	chunk3 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 3",
		Path:       "root.chunk3",
		Metadata:   map[string]interface{}{},
	}

	chunksDbHandler.InsertChunk(chunk1)
	chunksDbHandler.InsertChunk(chunk2)
	chunksDbHandler.InsertChunk(chunk3)

	// Create edges
	edge1 := &model.Edge{
		SourceChunkID: &chunk1.ID,
		TargetChunkID: &chunk2.ID,
		EdgeType:      model.EdgeTypeSemantic,
		Weight:        1.0,
		Bidirectional: false,
	}
	edge2 := &model.Edge{
		SourceChunkID: &chunk2.ID,
		TargetChunkID: &chunk3.ID,
		EdgeType:      model.EdgeTypeSemantic,
		Weight:        1.0,
		Bidirectional: true,
	}

	edgesDbHandler.InsertEdge(edge1)
	edgesDbHandler.InsertEdge(edge2)

	t.Run("Get edges connected to chunk", func(t *testing.T) {
		edges, err := edgesDbHandler.SelectEdgesConnectedToChunk(chunk2.ID, nil)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(edges), 1, "Expected at least 1 edge connected to chunk2")
	})

	t.Run("Filter by edge type", func(t *testing.T) {
		edgeType := model.EdgeTypeSemantic
		edges, err := edgesDbHandler.SelectEdgesConnectedToChunk(chunk2.ID, &edgeType)
		assert.NoError(t, err)
		assert.NotEmpty(t, edges)
	})

	// Cleanup
	edgesDbHandler.DeleteEdge(edge1.ID)
	edgesDbHandler.DeleteEdge(edge2.ID)
	chunksDbHandler.DeleteChunk(chunk1.ID)
	chunksDbHandler.DeleteChunk(chunk2.ID)
	chunksDbHandler.DeleteChunk(chunk3.ID)
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestSelectEdgesFromEntity(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, nil, 384, true)
	require.NoError(t, err)

	edgesDbHandler, err := NewEdgesDBHandler(database, true)
	require.NoError(t, err)

	entitiesDbHandler, err := NewEntitiesDBHandler(database, true)
	require.NoError(t, err)

	// Create a document
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create entity
	entity := &model.Entity{
		Name:     "Test Entity",
		Type:     "Person",
		Metadata: map[string]interface{}{},
	}
	entitiesDbHandler.InsertEntity(entity)

	// Create chunk
	chunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 1",
		Path:       "root.chunk1",
		Metadata:   map[string]interface{}{},
	}
	chunksDbHandler.InsertChunk(chunk)

	// Create edge from entity to chunk
	edge := &model.Edge{
		SourceEntityID: &entity.ID,
		TargetChunkID:  &chunk.ID,
		EdgeType:       model.EdgeTypeEntityMention,
		Weight:         1.0,
		Bidirectional:  false,
	}
	edgesDbHandler.InsertEdge(edge)

	t.Run("Get edges from entity", func(t *testing.T) {
		edges, err := edgesDbHandler.SelectEdgesFromEntity(entity.ID, nil)
		assert.NoError(t, err)
		assert.Len(t, edges, 1, "Expected 1 edge from entity")
		assert.Equal(t, edge.ID, edges[0].ID)
	})

	t.Run("Filter by edge type", func(t *testing.T) {
		edgeType := model.EdgeTypeEntityMention
		edges, err := edgesDbHandler.SelectEdgesFromEntity(entity.ID, &edgeType)
		assert.NoError(t, err)
		assert.Len(t, edges, 1)
	})

	// Cleanup
	edgesDbHandler.DeleteEdge(edge.ID)
	chunksDbHandler.DeleteChunk(chunk.ID)
	entitiesDbHandler.DeleteEntity(entity.ID)
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestSelectEdgesToEntity(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, nil, 384, true)
	require.NoError(t, err)

	edgesDbHandler, err := NewEdgesDBHandler(database, true)
	require.NoError(t, err)

	entitiesDbHandler, err := NewEntitiesDBHandler(database, true)
	require.NoError(t, err)

	// Create a document
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create entity
	entity := &model.Entity{
		Name:     "Test Entity",
		Type:     "Person",
		Metadata: map[string]interface{}{},
	}
	entitiesDbHandler.InsertEntity(entity)

	// Create chunk
	chunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 1",
		Path:       "root.chunk1",
		Metadata:   map[string]interface{}{},
	}
	chunksDbHandler.InsertChunk(chunk)

	// Create edge to entity
	edge := &model.Edge{
		SourceChunkID:  &chunk.ID,
		TargetEntityID: &entity.ID,
		EdgeType:       model.EdgeTypeEntityMention,
		Weight:         1.0,
		Bidirectional:  false,
	}
	edgesDbHandler.InsertEdge(edge)

	t.Run("Get edges to entity", func(t *testing.T) {
		edges, err := edgesDbHandler.SelectEdgesToEntity(entity.ID, nil)
		assert.NoError(t, err)
		assert.Len(t, edges, 1, "Expected 1 edge to entity")
		assert.Equal(t, edge.ID, edges[0].ID)
	})

	t.Run("Filter by edge type", func(t *testing.T) {
		edgeType := model.EdgeTypeEntityMention
		edges, err := edgesDbHandler.SelectEdgesToEntity(entity.ID, &edgeType)
		assert.NoError(t, err)
		assert.Len(t, edges, 1)
	})

	// Cleanup
	edgesDbHandler.DeleteEdge(edge.ID)
	chunksDbHandler.DeleteChunk(chunk.ID)
	entitiesDbHandler.DeleteEntity(entity.ID)
	documentsDbHandler.DeleteDocument(doc.RID)
}
