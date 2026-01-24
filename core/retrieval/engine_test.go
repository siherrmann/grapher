package retrieval

import (
	"context"
	"testing"

	"github.com/siherrmann/grapher/database"
	"github.com/siherrmann/grapher/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEngine(t *testing.T) {
	t.Run("Create new engine", func(t *testing.T) {
		chunks, edges, entities := initHandlers(t)
		engine := NewEngine(chunks, edges, entities)
		require.NotNil(t, engine, "Expected NewEngine to return a non-nil instance")
		assert.NotNil(t, engine.chunks, "Expected engine to have chunks handler")
	})
}

func TestVectorRetrieve(t *testing.T) {
	chunks, edges, entities := initHandlers(t)
	engine := NewEngine(chunks, edges, entities)

	// Create test document first
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}

	// We need documents handler to insert document
	db := initDB(t)
	documentsHandler, err := database.NewDocumentsDBHandler(db, false)
	require.NoError(t, err)
	err = documentsHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create test chunks with embeddings
	embedding1 := make([]float32, 384)
	embedding2 := make([]float32, 384)
	for i := range embedding1 {
		embedding1[i] = 0.1
		embedding2[i] = 0.2
	}

	chunk1 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Test chunk 1",
		Path:       "doc.section1",
		Embedding:  embedding1,
		Metadata:   map[string]interface{}{},
	}
	chunk2 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Test chunk 2",
		Path:       "doc.section2",
		Embedding:  embedding2,
		Metadata:   map[string]interface{}{},
	}

	err = chunks.InsertChunk(chunk1)
	require.NoError(t, err)
	err = chunks.InsertChunk(chunk2)
	require.NoError(t, err)

	t.Run("Vector retrieve with results", func(t *testing.T) {
		config := &model.QueryConfig{
			TopK:                10,
			SimilarityThreshold: 0.0,
		}

		queryEmbedding := make([]float32, 384)
		for i := range queryEmbedding {
			queryEmbedding[i] = 0.15 // Between the two embeddings
		}

		results, err := engine.Similarity(context.Background(), queryEmbedding, config)

		assert.NoError(t, err, "Expected VectorRetrieve to not return an error")
		assert.NotEmpty(t, results, "Expected at least one result")
		// assert.Equal(t, "vector", results[0].RetrievalMethod, "Expected retrieval method to be 'vector'")
		assert.Equal(t, 0, results[0].Distance, "Expected graph distance to be 0 for vector results")
	})

	// Cleanup
	chunks.DeleteChunk(chunk1.ID)
	chunks.DeleteChunk(chunk2.ID)
	documentsHandler.DeleteDocument(doc.RID)
}

func TestGetNeighbors(t *testing.T) {
	chunks, edges, entities := initHandlers(t)
	engine := NewEngine(chunks, edges, entities)

	// Create test document
	db := initDB(t)
	documentsHandler, err := database.NewDocumentsDBHandler(db, false)
	require.NoError(t, err)

	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create test chunks
	sourceChunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Source",
		Path:       "doc.s1",
		Metadata:   map[string]interface{}{},
	}
	target1Chunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Target 1",
		Path:       "doc.s2",
		Metadata:   map[string]interface{}{},
	}
	target2Chunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Target 2",
		Path:       "doc.s3",
		Metadata:   map[string]interface{}{},
	}

	err = chunks.InsertChunk(sourceChunk)
	require.NoError(t, err)
	err = chunks.InsertChunk(target1Chunk)
	require.NoError(t, err)
	err = chunks.InsertChunk(target2Chunk)
	require.NoError(t, err)

	// Create edges
	edge1 := &model.Edge{
		SourceChunkID: &sourceChunk.ID,
		TargetChunkID: &target1Chunk.ID,
		EdgeType:      model.EdgeTypeReference,
		Metadata:      map[string]interface{}{},
	}
	edge2 := &model.Edge{
		SourceChunkID: &sourceChunk.ID,
		TargetChunkID: &target2Chunk.ID,
		EdgeType:      model.EdgeTypeSemantic,
		Bidirectional: true,
		Metadata:      map[string]interface{}{},
	}

	err = edges.InsertEdge(edge1)
	require.NoError(t, err)
	err = edges.InsertEdge(edge2)
	require.NoError(t, err)

	t.Run("Get neighbors from source chunk", func(t *testing.T) {
		neighbors, err := engine.GetNeighbors(context.Background(), sourceChunk.ID, []model.EdgeType{}, true)

		assert.NoError(t, err)
		assert.Len(t, neighbors, 2)
	})

	t.Run("Get neighbors with edge type filter", func(t *testing.T) {
		neighbors, err := engine.GetNeighbors(context.Background(), sourceChunk.ID, []model.EdgeType{model.EdgeTypeReference}, false)

		assert.NoError(t, err)
		require.NotEmpty(t, neighbors)
	})

	// Cleanup
	edges.DeleteEdge(edge1.ID)
	edges.DeleteEdge(edge2.ID)
	chunks.DeleteChunk(sourceChunk.ID)
	chunks.DeleteChunk(target1Chunk.ID)
	chunks.DeleteChunk(target2Chunk.ID)
	documentsHandler.DeleteDocument(doc.RID)
}

func TestGetHierarchicalContext(t *testing.T) {
	chunkHandler, edgesHandler, entitiesHandler := initHandlers(t)
	engine := NewEngine(chunkHandler, edgesHandler, entitiesHandler)

	// Create test document
	db := initDB(t)
	documentsHandler, err := database.NewDocumentsDBHandler(db, false)
	require.NoError(t, err)

	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create test chunks with hierarchical paths
	chunk1 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 1",
		Path:       "doc.chapter1.section1",
		Metadata:   map[string]interface{}{},
	}
	chunk2 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 2",
		Path:       "doc.chapter1.section2",
		Metadata:   map[string]interface{}{},
	}

	err = chunkHandler.InsertChunk(chunk1)
	require.NoError(t, err)
	err = chunkHandler.InsertChunk(chunk2)
	require.NoError(t, err)

	t.Run("Get hierarchical context", func(t *testing.T) {
		config := &model.QueryConfig{
			IncludeAncestors:   true,
			IncludeDescendants: false,
			IncludeSiblings:    false,
		}

		chunks, err := engine.GetHierarchicalContext(context.Background(), "doc.chapter1.section1", config)

		assert.NoError(t, err)
		assert.NotNil(t, chunks)
	})

	// Cleanup
	chunkHandler.DeleteChunk(chunk1.ID)
	chunkHandler.DeleteChunk(chunk2.ID)
	documentsHandler.DeleteDocument(doc.RID)
}

func TestDFS(t *testing.T) {
	chunks, edges, entities := initHandlers(t)
	engine := NewEngine(chunks, edges, entities)

	// Create test document
	db := initDB(t)
	documentsHandler, err := database.NewDocumentsDBHandler(db, false)
	require.NoError(t, err)

	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create test chunks
	chunk1 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 1",
		Path:       "doc.section1",
		Metadata:   map[string]interface{}{},
	}
	chunk2 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 2",
		Path:       "doc.section2",
		Metadata:   map[string]interface{}{},
	}
	chunk3 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Chunk 3",
		Path:       "doc.section3",
		Metadata:   map[string]interface{}{},
	}

	err = chunks.InsertChunk(chunk1)
	require.NoError(t, err)
	err = chunks.InsertChunk(chunk2)
	require.NoError(t, err)
	err = chunks.InsertChunk(chunk3)
	require.NoError(t, err)

	// Create edges to form a graph
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
		Bidirectional: false,
	}

	err = edges.InsertEdge(edge1)
	require.NoError(t, err)
	err = edges.InsertEdge(edge2)
	require.NoError(t, err)

	t.Run("DFS traverses graph depth-first", func(t *testing.T) {
		results, err := engine.DFS(context.Background(), chunk1.ID, 2, []model.EdgeType{}, false)

		assert.NoError(t, err)
		assert.NotEmpty(t, results)
		// First result should be the source chunk with distance 0
		assert.Equal(t, chunk1.ID, results[0].Chunk.ID)
		assert.Equal(t, 0, results[0].Distance)
	})

	t.Run("DFS respects max hops", func(t *testing.T) {
		results, err := engine.DFS(context.Background(), chunk1.ID, 1, []model.EdgeType{}, false)

		assert.NoError(t, err)
		// Should only go 1 hop deep
		for _, result := range results {
			assert.LessOrEqual(t, result.Distance, 1)
		}
	})

	t.Run("DFS with edge type filter", func(t *testing.T) {
		results, err := engine.DFS(context.Background(), chunk1.ID, 2, []model.EdgeType{model.EdgeTypeSemantic}, false)

		assert.NoError(t, err)
		assert.NotEmpty(t, results)
	})

	// Cleanup
	edges.DeleteEdge(edge1.ID)
	edges.DeleteEdge(edge2.ID)
	chunks.DeleteChunk(chunk1.ID)
	chunks.DeleteChunk(chunk2.ID)
	chunks.DeleteChunk(chunk3.ID)
	documentsHandler.DeleteDocument(doc.RID)
}
