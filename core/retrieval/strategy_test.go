package retrieval

import (
	"context"
	"testing"

	"github.com/siherrmann/grapher/database"
	"github.com/siherrmann/grapher/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewVectorOnlyStrategy(t *testing.T) {
	t.Run("Create vector-only strategy", func(t *testing.T) {
		chunks, edges, entities := initHandlers(t)
		engine := NewEngine(chunks, edges, entities)
		strategy := NewVectorOnlyStrategy(engine)

		require.NotNil(t, strategy)
		assert.NotNil(t, strategy.engine)
	})
}

func TestVectorOnlyStrategyRetrieve(t *testing.T) {
	chunks, edges, entities := initHandlers(t)
	engine := NewEngine(chunks, edges, entities)
	strategy := NewVectorOnlyStrategy(engine)

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

	// Create test chunk with embedding
	embedding := make([]float32, 384)
	for i := range embedding {
		embedding[i] = 0.5
	}

	chunk1 := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Test chunk",
		Path:       "doc.s1",
		Embedding:  embedding,
		Metadata:   map[string]interface{}{},
	}

	err = chunks.InsertChunk(chunk1)
	require.NoError(t, err)

	t.Run("Vector-only retrieve", func(t *testing.T) {
		config := &model.QueryConfig{
			TopK:                10,
			SimilarityThreshold: 0.0,
		}

		queryEmbedding := make([]float32, 384)
		for i := range queryEmbedding {
			queryEmbedding[i] = 0.5
		}

		results, err := strategy.Retrieve(context.Background(), queryEmbedding, config)

		assert.NoError(t, err)
		require.NotEmpty(t, results)
		assert.Equal(t, "vector", results[0].RetrievalMethod)
	})

	// Cleanup
	chunks.DeleteChunk(chunk1.ID)
	documentsHandler.DeleteDocument(doc.RID)
}

func TestNewContextualStrategy(t *testing.T) {
	t.Run("Create contextual strategy", func(t *testing.T) {
		chunks, edges, entities := initHandlers(t)
		engine := NewEngine(chunks, edges, entities)
		strategy := NewContextualStrategy(engine)

		require.NotNil(t, strategy)
		assert.NotNil(t, strategy.engine)
	})
}

func TestContextualStrategyRetrieve(t *testing.T) {
	chunks, edges, entities := initHandlers(t)
	engine := NewEngine(chunks, edges, entities)
	strategy := NewContextualStrategy(engine)

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
	embedding1 := make([]float32, 384)
	embedding2 := make([]float32, 384)
	for i := range embedding1 {
		embedding1[i] = 0.5
		embedding2[i] = 0.4
	}

	sourceChunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Source",
		Path:       "doc.chapter1.section1",
		Embedding:  embedding1,
		Metadata:   map[string]interface{}{},
	}
	neighborChunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Neighbor",
		Path:       "doc.chapter1.section2",
		Embedding:  embedding2,
		Metadata:   map[string]interface{}{},
	}

	err = chunks.InsertChunk(sourceChunk)
	require.NoError(t, err)
	err = chunks.InsertChunk(neighborChunk)
	require.NoError(t, err)

	// Create edge
	edge := &model.Edge{
		SourceChunkID: &sourceChunk.ID,
		TargetChunkID: &neighborChunk.ID,
		EdgeType:      model.EdgeTypeReference,
		Metadata:      map[string]interface{}{},
	}
	err = edges.InsertEdge(edge)
	require.NoError(t, err)

	t.Run("Contextual retrieve with neighbors and hierarchy", func(t *testing.T) {
		config := &model.QueryConfig{
			TopK:                10,
			SimilarityThreshold: 0.0,
			GraphWeight:         0.5,
			HierarchyWeight:     0.3,
			IncludeAncestors:    true,
			IncludeDescendants:  true,
		}

		queryEmbedding := make([]float32, 384)
		for i := range queryEmbedding {
			queryEmbedding[i] = 0.5
		}

		results, err := strategy.Retrieve(context.Background(), queryEmbedding, config)

		assert.NoError(t, err)
		require.NotEmpty(t, results)
		assert.GreaterOrEqual(t, len(results), 1)
	})

	t.Run("Contextual retrieve with only vector", func(t *testing.T) {
		config := &model.QueryConfig{
			TopK:                10,
			SimilarityThreshold: 0.0,
			GraphWeight:         0.0,
			HierarchyWeight:     0.0,
		}

		queryEmbedding := make([]float32, 384)
		for i := range queryEmbedding {
			queryEmbedding[i] = 0.5
		}

		results, err := strategy.Retrieve(context.Background(), queryEmbedding, config)

		assert.NoError(t, err)
		require.NotEmpty(t, results)
	})

	// Cleanup
	edges.DeleteEdge(edge.ID)
	chunks.DeleteChunk(sourceChunk.ID)
	chunks.DeleteChunk(neighborChunk.ID)
	documentsHandler.DeleteDocument(doc.RID)
}

func TestNewMultiHopStrategy(t *testing.T) {
	t.Run("Create multi-hop strategy", func(t *testing.T) {
		chunks, edges, entities := initHandlers(t)
		engine := NewEngine(chunks, edges, entities)
		strategy := NewMultiHopStrategy(engine)

		require.NotNil(t, strategy)
		assert.NotNil(t, strategy.engine)
	})
}

func TestMultiHopStrategyRetrieve(t *testing.T) {
	chunks, edges, entities := initHandlers(t)
	engine := NewEngine(chunks, edges, entities)
	strategy := NewMultiHopStrategy(engine)

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
	embedding1 := make([]float32, 384)
	embedding2 := make([]float32, 384)
	embedding3 := make([]float32, 384)
	for i := range embedding1 {
		embedding1[i] = 0.5
		embedding2[i] = 0.4
		embedding3[i] = 0.3
	}

	sourceChunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Source",
		Path:       "doc.s1",
		Embedding:  embedding1,
		Metadata:   map[string]interface{}{},
	}
	hop1Chunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Hop 1",
		Path:       "doc.s2",
		Embedding:  embedding2,
		Metadata:   map[string]interface{}{},
	}
	hop2Chunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Hop 2",
		Path:       "doc.s3",
		Embedding:  embedding3,
		Metadata:   map[string]interface{}{},
	}

	err = chunks.InsertChunk(sourceChunk)
	require.NoError(t, err)
	err = chunks.InsertChunk(hop1Chunk)
	require.NoError(t, err)
	err = chunks.InsertChunk(hop2Chunk)
	require.NoError(t, err)

	// Create edges
	edge1 := &model.Edge{
		SourceChunkID: &sourceChunk.ID,
		TargetChunkID: &hop1Chunk.ID,
		EdgeType:      model.EdgeTypeReference,
		Metadata:      map[string]interface{}{},
	}
	edge2 := &model.Edge{
		SourceChunkID: &hop1Chunk.ID,
		TargetChunkID: &hop2Chunk.ID,
		EdgeType:      model.EdgeTypeReference,
		Metadata:      map[string]interface{}{},
	}

	err = edges.InsertEdge(edge1)
	require.NoError(t, err)
	err = edges.InsertEdge(edge2)
	require.NoError(t, err)

	t.Run("Multi-hop retrieve", func(t *testing.T) {
		config := &model.QueryConfig{
			TopK:                10,
			SimilarityThreshold: 0.0,
			MaxHops:             2,
			GraphWeight:         0.5,
		}

		queryEmbedding := make([]float32, 384)
		for i := range queryEmbedding {
			queryEmbedding[i] = 0.5
		}

		results, err := strategy.Retrieve(context.Background(), queryEmbedding, config)

		assert.NoError(t, err)
		require.NotEmpty(t, results)
	})

	t.Run("Multi-hop with max hops 1", func(t *testing.T) {
		config := &model.QueryConfig{
			TopK:                10,
			SimilarityThreshold: 0.0,
			MaxHops:             1,
			GraphWeight:         0.5,
		}

		queryEmbedding := make([]float32, 384)
		for i := range queryEmbedding {
			queryEmbedding[i] = 0.5
		}

		results, err := strategy.Retrieve(context.Background(), queryEmbedding, config)

		assert.NoError(t, err)
		require.NotEmpty(t, results)
	})

	// Cleanup
	edges.DeleteEdge(edge1.ID)
	edges.DeleteEdge(edge2.ID)
	chunks.DeleteChunk(sourceChunk.ID)
	chunks.DeleteChunk(hop1Chunk.ID)
	chunks.DeleteChunk(hop2Chunk.ID)
	documentsHandler.DeleteDocument(doc.RID)
}

func TestNewHybridStrategy(t *testing.T) {
	t.Run("Create hybrid strategy", func(t *testing.T) {
		chunks, edges, entities := initHandlers(t)
		engine := NewEngine(chunks, edges, entities)
		strategy := NewHybridStrategy(engine)

		require.NotNil(t, strategy)
		assert.NotNil(t, strategy.engine)
	})
}

func TestHybridStrategyRetrieve(t *testing.T) {
	chunks, edges, entities := initHandlers(t)
	engine := NewEngine(chunks, edges, entities)
	strategy := NewHybridStrategy(engine)

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
	embedding1 := make([]float32, 384)
	embedding2 := make([]float32, 384)
	for i := range embedding1 {
		embedding1[i] = 0.5
		embedding2[i] = 0.4
	}

	sourceChunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Source",
		Path:       "doc.chapter1.section1",
		Embedding:  embedding1,
		Metadata:   map[string]interface{}{},
	}
	neighborChunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Neighbor",
		Path:       "doc.chapter1.section2",
		Embedding:  embedding2,
		Metadata:   map[string]interface{}{},
	}

	err = chunks.InsertChunk(sourceChunk)
	require.NoError(t, err)
	err = chunks.InsertChunk(neighborChunk)
	require.NoError(t, err)

	// Create edge
	edge := &model.Edge{
		SourceChunkID: &sourceChunk.ID,
		TargetChunkID: &neighborChunk.ID,
		EdgeType:      model.EdgeTypeReference,
		Metadata:      map[string]interface{}{},
	}
	err = edges.InsertEdge(edge)
	require.NoError(t, err)

	t.Run("Hybrid retrieve with all components", func(t *testing.T) {
		config := &model.QueryConfig{
			TopK:                10,
			SimilarityThreshold: 0.0,
			VectorWeight:        1.0,
			GraphWeight:         0.5,
			HierarchyWeight:     0.3,
			MaxHops:             1,
			IncludeAncestors:    true,
		}

		queryEmbedding := make([]float32, 384)
		for i := range queryEmbedding {
			queryEmbedding[i] = 0.5
		}

		results, err := strategy.Retrieve(context.Background(), queryEmbedding, config)

		assert.NoError(t, err)
		require.NotEmpty(t, results)
		assert.Equal(t, "hybrid", results[0].RetrievalMethod)
	})

	t.Run("Hybrid retrieve respects TopK limit", func(t *testing.T) {
		config := &model.QueryConfig{
			TopK:                1,
			SimilarityThreshold: 0.0,
			VectorWeight:        1.0,
			GraphWeight:         0.5,
			MaxHops:             1,
		}

		queryEmbedding := make([]float32, 384)
		for i := range queryEmbedding {
			queryEmbedding[i] = 0.5
		}

		results, err := strategy.Retrieve(context.Background(), queryEmbedding, config)

		assert.NoError(t, err)
		assert.LessOrEqual(t, len(results), 1)
	})

	t.Run("Hybrid retrieve without graph traversal", func(t *testing.T) {
		config := &model.QueryConfig{
			TopK:                10,
			SimilarityThreshold: 0.0,
			VectorWeight:        1.0,
			GraphWeight:         0.0,
			MaxHops:             0,
		}

		queryEmbedding := make([]float32, 384)
		for i := range queryEmbedding {
			queryEmbedding[i] = 0.5
		}

		results, err := strategy.Retrieve(context.Background(), queryEmbedding, config)

		assert.NoError(t, err)
		require.NotEmpty(t, results)
	})

	// Cleanup
	edges.DeleteEdge(edge.ID)
	chunks.DeleteChunk(sourceChunk.ID)
	chunks.DeleteChunk(neighborChunk.ID)
	documentsHandler.DeleteDocument(doc.RID)
}

func TestNewEntityCentricStrategy(t *testing.T) {
	t.Run("Create entity-centric strategy", func(t *testing.T) {
		chunks, edges, entities := initHandlers(t)
		engine := NewEngine(chunks, edges, entities)
		strategy := NewEntityCentricStrategy(engine, entities)

		require.NotNil(t, strategy)
		assert.NotNil(t, strategy.engine)
	})
}

func TestEntityCentricStrategyRetrieve(t *testing.T) {
	chunks, edges, entities := initHandlers(t)
	engine := NewEngine(chunks, edges, entities)
	strategy := NewEntityCentricStrategy(engine, entities)

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

	// Create test entity
	entity := &model.Entity{
		Name:     "Test Entity",
		Type:     "Person",
		Metadata: map[string]interface{}{},
	}
	err = entities.InsertEntity(entity)
	require.NoError(t, err)

	// Create test chunk with embedding
	embedding := make([]float32, 384)
	for i := range embedding {
		embedding[i] = 0.5
	}

	chunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Test chunk with entity",
		Path:       "doc.section1",
		Metadata:   map[string]interface{}{},
		Embedding:  embedding,
	}
	err = chunks.InsertChunk(chunk)
	require.NoError(t, err)

	// Create edge from entity to chunk
	edge := &model.Edge{
		SourceEntityID: &entity.ID,
		TargetChunkID:  &chunk.ID,
		EdgeType:       model.EdgeTypeEntityMention,
		Weight:         1.0,
		Bidirectional:  false,
	}
	err = edges.InsertEdge(edge)
	require.NoError(t, err)

	t.Run("EntityCentric strategy retrieves entities and connected chunks", func(t *testing.T) {
		config := &model.QueryConfig{
			TopK:                10,
			SimilarityThreshold: 0.0,
			MaxHops:             1,
		}

		results, err := strategy.Retrieve(context.Background(), entity.ID, config)

		assert.NoError(t, err)
		require.NotNil(t, results)
	})

	t.Run("EntityCentric strategy respects config settings", func(t *testing.T) {
		config := &model.QueryConfig{
			TopK:                5,
			SimilarityThreshold: 0.5,
			MaxHops:             2,
		}

		results, err := strategy.Retrieve(context.Background(), entity.ID, config)

		assert.NoError(t, err)
		require.NotNil(t, results)
		// Results should respect max results
		assert.LessOrEqual(t, len(results), config.TopK)
	})

	// Cleanup
	edges.DeleteEdge(edge.ID)
	chunks.DeleteChunk(chunk.ID)
	entities.DeleteEntity(entity.ID)
	documentsHandler.DeleteDocument(doc.RID)
}
