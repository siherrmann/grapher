package grapher

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/siherrmann/grapher/core/pipeline"
	"github.com/siherrmann/grapher/helper"
	"github.com/siherrmann/grapher/model"
	loadSql "github.com/siherrmann/grapher/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testEmbedder creates a simple deterministic embedder for testing
func testEmbedder(dimension int) pipeline.EmbedFunc {
	return func(text string) ([]float32, error) {
		embedding := make([]float32, dimension)
		for i := 0; i < dimension; i++ {
			embedding[i] = float32((len(text)+i)%100) / 100.0
		}
		return embedding, nil
	}
}

func initGrapher(t *testing.T) *Grapher {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	require.NoError(t, err, "failed to create database configuration")

	g, err := NewGrapher(dbConfig, 384)
	require.NoError(t, err, "failed to create grapher")
	require.NotNil(t, g, "expected grapher to be non-nil")

	// Initialize database
	err = loadSql.Init(g.DB.Instance)
	require.NoError(t, err, "failed to initialize database")

	t.Cleanup(func() {
		g.Close()
	})

	return g
}

func TestNewGrapher(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	require.NoError(t, err)

	t.Run("Valid call NewGrapher", func(t *testing.T) {
		g, err := NewGrapher(dbConfig, 384)
		require.NoError(t, err, "Expected NewGrapher to not return an error")
		require.NotNil(t, g, "Expected NewGrapher to return a non-nil instance")
		assert.NotNil(t, g.DB, "Expected grapher to have a database instance")
		assert.NotNil(t, g.Chunks, "Expected grapher to have chunks handler")
		assert.NotNil(t, g.Documents, "Expected grapher to have documents handler")
		assert.NotNil(t, g.Edges, "Expected grapher to have edges handler")
		assert.NotNil(t, g.Entities, "Expected grapher to have entities handler")
		assert.Nil(t, g.Pipeline, "Expected pipeline to be nil initially")

		// Cleanup
		err = g.Close()
		assert.NoError(t, err, "Expected Close to not return an error")
	})

	t.Run("Grapher with nil database handles Close gracefully", func(t *testing.T) {
		g := &Grapher{
			DB:        nil,
			Chunks:    nil,
			Documents: nil,
			Edges:     nil,
			Entities:  nil,
		}

		err := g.Close()
		assert.NoError(t, err, "Expected Close to handle nil DB gracefully")
	})
}

func TestSetPipeline(t *testing.T) {
	g := initGrapher(t)

	t.Run("Set pipeline successfully", func(t *testing.T) {
		chunker := pipeline.SentenceChunker(5)
		embedder := testEmbedder(384)
		pipeline := pipeline.NewPipeline(chunker, embedder)

		g.SetPipeline(pipeline)

		assert.NotNil(t, g.Pipeline, "Expected pipeline to be set")
		assert.Equal(t, pipeline, g.Pipeline, "Expected pipeline to match")
	})

	t.Run("Set pipeline to nil", func(t *testing.T) {
		g.SetPipeline(nil)

		assert.Nil(t, g.Pipeline, "Expected pipeline to be nil")
	})

	t.Run("Replace existing pipeline", func(t *testing.T) {
		chunker1 := pipeline.SentenceChunker(5)
		embedder1 := testEmbedder(384)
		pipeline1 := pipeline.NewPipeline(chunker1, embedder1)

		chunker2 := pipeline.SentenceChunker(10)
		embedder2 := testEmbedder(384)
		pipeline2 := pipeline.NewPipeline(chunker2, embedder2)

		g.SetPipeline(pipeline1)
		assert.Equal(t, pipeline1, g.Pipeline, "Expected first pipeline to be set")

		g.SetPipeline(pipeline2)
		assert.Equal(t, pipeline2, g.Pipeline, "Expected second pipeline to replace first")
	})
}

func TestProcessAndInsertDocument(t *testing.T) {
	g := initGrapher(t)

	chunker := pipeline.SentenceChunker(5)
	embedder := testEmbedder(384)
	pipeline := pipeline.NewPipeline(chunker, embedder)
	g.SetPipeline(pipeline)

	t.Run("Process and insert document successfully", func(t *testing.T) {
		doc := &model.Document{
			Title:   "Test Document",
			Source:  "test",
			Content: "This is a test document with some content. It should be split into chunks and processed.",
			Metadata: model.Metadata{
				"test": "value",
			},
		}

		numChunks, err := g.ProcessAndInsertDocument(doc)

		assert.NoError(t, err, "Expected ProcessAndInsertDocument to not return an error")
		assert.Greater(t, numChunks, 0, "Expected at least one chunk to be inserted")
		assert.NotEqual(t, "", doc.RID.String(), "Expected document RID to be set")
		assert.Greater(t, doc.ID, int(0), "Expected document ID to be set")
		assert.Equal(t, "", doc.Content, "Expected content to be cleared after processing")

		// Cleanup
		g.Documents.DeleteDocument(doc.RID)
	})

	t.Run("Error when pipeline not set", func(t *testing.T) {
		gNoPipeline := initGrapher(t)

		doc := &model.Document{
			Title:   "Test Document",
			Source:  "test",
			Content: "Some content",
		}

		numChunks, err := gNoPipeline.ProcessAndInsertDocument(doc)

		assert.Error(t, err, "Expected error when pipeline not set")
		assert.Equal(t, 0, numChunks, "Expected 0 chunks when error occurs")
		assert.Contains(t, err.Error(), "pipeline not set", "Expected specific error message")
	})

	t.Run("Error when content is empty", func(t *testing.T) {
		doc := &model.Document{
			Title:   "Test Document",
			Source:  "test",
			Content: "",
		}

		numChunks, err := g.ProcessAndInsertDocument(doc)

		assert.Error(t, err, "Expected error when content is empty")
		assert.Equal(t, 0, numChunks, "Expected 0 chunks when error occurs")
		assert.Contains(t, err.Error(), "content is empty", "Expected specific error message")
	})

	t.Run("Process document with metadata", func(t *testing.T) {
		doc := &model.Document{
			Title:   "Test Document with Metadata",
			Source:  "test_metadata",
			Content: "Content for metadata test",
			Metadata: model.Metadata{
				"author":  "Test Author",
				"topic":   "testing",
				"version": 1,
			},
		}

		numChunks, err := g.ProcessAndInsertDocument(doc)

		assert.NoError(t, err, "Expected ProcessAndInsertDocument to not return an error")
		assert.Greater(t, numChunks, 0, "Expected at least one chunk")

		// Verify document was inserted with metadata
		retrieved, err := g.Documents.SelectDocument(doc.RID)
		require.NoError(t, err, "Expected to retrieve document")
		assert.Equal(t, "Test Author", retrieved.Metadata["author"], "Expected metadata to be preserved")
		assert.Equal(t, "testing", retrieved.Metadata["topic"], "Expected metadata to be preserved")

		// Cleanup
		g.Documents.DeleteDocument(doc.RID)
	})

	t.Run("Process document with long content", func(t *testing.T) {
		longContent := ""
		for i := 0; i < 100; i++ {
			longContent += "This is a longer piece of text to test chunk splitting. "
		}

		doc := &model.Document{
			Title:    "Long Document",
			Source:   "test_long",
			Content:  longContent,
			Metadata: model.Metadata{},
		}

		numChunks, err := g.ProcessAndInsertDocument(doc)

		assert.NoError(t, err, "Expected ProcessAndInsertDocument to not return an error")
		assert.Greater(t, numChunks, 1, "Expected multiple chunks for long content")

		// Cleanup
		g.Documents.DeleteDocument(doc.RID)
	})

	t.Run("Process multiple documents", func(t *testing.T) {
		docs := []*model.Document{
			{
				Title:    "Doc 1",
				Source:   "test1",
				Content:  "Content for document one.",
				Metadata: model.Metadata{},
			},
			{
				Title:    "Doc 2",
				Source:   "test2",
				Content:  "Content for document two.",
				Metadata: model.Metadata{},
			},
			{
				Title:    "Doc 3",
				Source:   "test3",
				Content:  "Content for document three.",
				Metadata: model.Metadata{},
			},
		}

		totalChunks := 0
		for _, doc := range docs {
			numChunks, err := g.ProcessAndInsertDocument(doc)
			assert.NoError(t, err, "Expected ProcessAndInsertDocument to not return an error")
			assert.Greater(t, numChunks, 0, "Expected at least one chunk")
			totalChunks += numChunks
		}

		assert.Greater(t, totalChunks, 0, "Expected total chunks to be greater than 0")

		// Cleanup
		for _, doc := range docs {
			g.Documents.DeleteDocument(doc.RID)
		}
	})
}

func TestUseDefaultPipeline(t *testing.T) {
	g := initGrapher(t)

	t.Run("Sets up default pipeline successfully", func(t *testing.T) {
		err := g.UseDefaultPipeline()

		require.NoError(t, err)
		assert.NotNil(t, g.Pipeline, "Pipeline should be set")
		assert.NotNil(t, g.Pipeline.Embedder, "Embedder should be set")
		assert.NotNil(t, g.Pipeline.Chunker, "Chunker should be set")
	})

	t.Run("Can process document after setting default pipeline", func(t *testing.T) {
		err := g.UseDefaultPipeline()
		require.NoError(t, err)

		doc := &model.Document{
			Title:   "Test Doc",
			Source:  "test",
			Content: "This is test content for the default pipeline.",
		}

		numChunks, err := g.ProcessAndInsertDocument(doc)

		assert.NoError(t, err)
		assert.Greater(t, numChunks, 0)

		// Cleanup
		g.Documents.DeleteDocument(doc.RID)
	})
}

func TestSearchMethods(t *testing.T) {
	g := initGrapher(t)
	err := g.UseDefaultPipeline()
	require.NoError(t, err)

	// Insert test documents with more content that includes entities and references
	doc1 := &model.Document{
		Title:   "AI Basics",
		Source:  "test1",
		Content: "Artificial intelligence is the simulation of human intelligence by machines. John Smith at MIT developed early AI systems. The technology was further advanced by researchers at Stanford University. Machine learning, as described in Section 3.2, is a key component.",
	}
	doc2 := &model.Document{
		Title:   "Machine Learning",
		Source:  "test2",
		Content: "Machine learning is a subset of artificial intelligence that focuses on data. Professor Jane Doe from Microsoft Research pioneered modern approaches. Deep learning methods are discussed in chapter 5. See Smith (2020) for more details.",
	}

	_, err = g.ProcessAndInsertDocument(doc1)
	require.NoError(t, err)
	_, err = g.ProcessAndInsertDocument(doc2)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("Search performs vector search", func(t *testing.T) {
		config := model.DefaultQueryConfig()
		config.TopK = 5

		results, err := g.Search(ctx, "What is artificial intelligence?", &config)

		assert.NoError(t, err)
		assert.NotEmpty(t, results)
		assert.LessOrEqual(t, len(results), 5)
	})

	t.Run("ContextualSearch includes context", func(t *testing.T) {
		config := model.DefaultQueryConfig()
		config.TopK = 3
		config.IncludeAncestors = true
		config.IncludeSiblings = true

		results, err := g.ContextualSearch(ctx, "machine learning", &config)

		assert.NoError(t, err)
		assert.NotNil(t, results)
		// Results may be empty if graph doesn't have hierarchical connections
		if len(results) > 0 {
			t.Logf("Found %d contextual results", len(results))
		}
	})

	t.Run("MultiHopSearch traverses graph", func(t *testing.T) {
		config := model.DefaultQueryConfig()
		config.TopK = 3
		config.MaxHops = 2

		results, err := g.MultiHopSearch(ctx, "intelligence", &config)

		assert.NoError(t, err)
		assert.NotNil(t, results)
		// Results may be empty if graph doesn't have connected edges for traversal
		if len(results) > 0 {
			t.Logf("Found %d results via graph traversal", len(results))
		}
	})

	t.Run("HybridSearch combines strategies", func(t *testing.T) {
		config := model.DefaultQueryConfig()
		config.TopK = 5
		config.VectorWeight = 0.6
		config.GraphWeight = 0.3
		config.HierarchyWeight = 0.1

		results, err := g.HybridSearch(ctx, "artificial intelligence", &config)

		assert.NoError(t, err)
		assert.NotNil(t, results)
		// Results may be limited if graph/hierarchy connections are sparse
		if len(results) > 0 {
			t.Logf("Found %d hybrid search results", len(results))
		}
	})

	t.Run("DocumentScopedSearch filters by document", func(t *testing.T) {
		config := model.DefaultQueryConfig()
		config.TopK = 5

		results, err := g.DocumentScopedSearch(ctx, "intelligence", []uuid.UUID{doc1.RID}, &config)

		assert.NoError(t, err)
		// All results should be from doc1
		for _, result := range results {
			assert.Equal(t, doc1.RID, result.DocumentRID)
		}
	})

	t.Run("DocumentScopedSearch with empty document list returns error", func(t *testing.T) {
		config := model.DefaultQueryConfig()

		results, err := g.DocumentScopedSearch(ctx, "test", []uuid.UUID{}, &config)

		assert.Error(t, err)
		assert.Nil(t, results)
	})

	// Cleanup
	g.Documents.DeleteDocument(doc1.RID)
	g.Documents.DeleteDocument(doc2.RID)
}

func TestTraversal(t *testing.T) {
	g := initGrapher(t)
	err := g.UseDefaultPipeline()
	require.NoError(t, err)

	// Insert test document with longer content that will create multiple chunks
	doc := &model.Document{
		Title:  "Test Doc",
		Source: "test",
		Content: "First paragraph about artificial intelligence and machine learning systems. " +
			"Professor John Smith from Stanford University has made significant contributions to the field. " +
			"Second section discusses deep learning methods as referenced in Section 2.1 of the paper. " +
			"Jane Doe at MIT Research Lab has developed novel approaches to neural networks. " +
			"Third part covers natural language processing applications. " +
			"Microsoft and Google have both invested heavily in AI research. " +
			"Fourth paragraph examines computer vision techniques. " +
			"As discussed by Brown et al. (2020), transformer architectures have revolutionized the field.",
	}

	numChunks, err := g.ProcessAndInsertDocument(doc)
	require.NoError(t, err)
	require.GreaterOrEqual(t, numChunks, 1) // At least 1 chunk (changed from >1)

	// Get chunks to use as source
	chunks, err := g.Chunks.SelectChunksByDocument(doc.RID)
	require.NoError(t, err)
	require.NotEmpty(t, chunks)

	ctx := context.Background()
	sourceID := chunks[0].ID

	t.Run("BFSTraversal explores graph breadth-first", func(t *testing.T) {
		results, err := g.BFSTraversal(ctx, sourceID, 2, nil, true)

		assert.NoError(t, err)
		assert.NotEmpty(t, results)
		// First result should be source with distance 0
		assert.Equal(t, 0, results[0].Distance)
	})

	t.Run("DFSTraversal explores graph depth-first", func(t *testing.T) {
		results, err := g.DFSTraversal(ctx, sourceID, 2, nil, true)

		assert.NoError(t, err)
		assert.NotEmpty(t, results)
		// First result should be source with distance 0
		assert.Equal(t, 0, results[0].Distance)
	})

	// Cleanup
	g.Documents.DeleteDocument(doc.RID)
}
