package database

import (
	"testing"
	"time"

	"github.com/siherrmann/grapher/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChunksNewChunksDBHandler(t *testing.T) {
	database := initDB(t)

	t.Run("Valid call NewChunksDBHandler", func(t *testing.T) {
		// Create documents handler first to ensure documents table exists (needed for foreign key)
		_, err := NewDocumentsDBHandler(database, true)
		require.NoError(t, err, "Expected NewDocumentsDBHandler to not return an error")

		chunksDbHandler, err := NewChunksDBHandler(database, nil, 384, true)
		assert.NoError(t, err, "Expected NewChunksDBHandler to not return an error")
		require.NotNil(t, chunksDbHandler, "Expected NewChunksDBHandler to return a non-nil instance")
		require.NotNil(t, chunksDbHandler.db, "Expected NewChunksDBHandler to have a non-nil database instance")
		require.NotNil(t, chunksDbHandler.db.Instance, "Expected NewChunksDBHandler to have a non-nil database connection instance")
	})

	t.Run("Invalid call NewChunksDBHandler with nil database", func(t *testing.T) {
		_, err := NewChunksDBHandler(nil, nil, 384, false)
		assert.Error(t, err, "Expected error when creating ChunksDBHandler with nil database")
		assert.Contains(t, err.Error(), "database connection is nil", "Expected specific error message for nil database connection")
	})
}

func TestChunksInsert(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err, "Expected NewDocumentsDBHandler to not return an error")

	chunksDbHandler, err := NewChunksDBHandler(database, nil, 384, true)
	require.NoError(t, err, "Expected NewChunksDBHandler to not return an error")

	// Create a document first
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test_source.txt",
		Metadata: map[string]interface{}{"author": "Test Author"},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err, "Expected Insert document to not return an error")

	t.Run("Insert chunk without embedding", func(t *testing.T) {
		startPos := 0
		endPos := 20
		chunkIndex := 0
		chunk := &model.Chunk{
			DocumentID: doc.ID,
			Content:    "This is a test chunk",
			Path:       "root.section1",
			StartPos:   &startPos,
			EndPos:     &endPos,
			ChunkIndex: &chunkIndex,
			Metadata:   map[string]interface{}{"type": "paragraph"},
		}

		err := chunksDbHandler.InsertChunk(chunk)
		assert.NoError(t, err, "Expected Insert to not return an error")
		assert.NotEmpty(t, chunk.ID, "Expected inserted chunk to have an ID")
		assert.WithinDuration(t, chunk.CreatedAt, time.Now(), 2*time.Second, "Expected CreatedAt to be set")
	})

	t.Run("Insert chunk with embedding", func(t *testing.T) {
		startPos := 21
		endPos := 46
		chunkIndex := 1
		// Create 384-dimension embedding
		embedding := make([]float32, 384)
		for i := range embedding {
			embedding[i] = float32(i) / 384.0
		}
		chunk := &model.Chunk{
			DocumentID: doc.ID,
			Content:    "This is another test chunk",
			Path:       "root.section2",
			Embedding:  embedding,
			StartPos:   &startPos,
			EndPos:     &endPos,
			ChunkIndex: &chunkIndex,
			Metadata:   map[string]interface{}{"type": "paragraph"},
		}

		err := chunksDbHandler.InsertChunk(chunk)
		assert.NoError(t, err, "Expected Insert to not return an error")
		assert.NotEmpty(t, chunk.ID, "Expected inserted chunk to have an ID")
		assert.Equal(t, 384, len(chunk.Embedding), "Expected embedding to be preserved")
	})

	// Cleanup
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestChunksGet(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err, "Expected NewDocumentsDBHandler to not return an error")

	chunksDbHandler, err := NewChunksDBHandler(database, nil, 384, true)
	require.NoError(t, err, "Expected NewChunksDBHandler to not return an error")

	// Create a document and chunk
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test_source.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	chunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Test content",
		Path:       "root",
		Metadata:   map[string]interface{}{},
	}
	err = chunksDbHandler.InsertChunk(chunk)
	require.NoError(t, err)

	// Test Get
	retrievedChunk, err := chunksDbHandler.SelectChunk(chunk.ID)
	assert.NoError(t, err, "Expected Get to not return an error")
	assert.NotNil(t, retrievedChunk, "Expected Get to return a non-nil chunk")
	assert.Equal(t, chunk.ID, retrievedChunk.ID, "Expected chunk IDs to match")
	assert.Equal(t, chunk.Content, retrievedChunk.Content, "Expected chunk content to match")

	// Cleanup
	chunksDbHandler.DeleteChunk(chunk.ID)
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestChunksGetByDocument(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, nil, 384, true)
	require.NoError(t, err)

	// Create a document
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create multiple chunks for the document
	chunkCount := 3
	chunks := make([]*model.Chunk, chunkCount)
	for i := 0; i < chunkCount; i++ {
		index := i
		chunks[i] = &model.Chunk{
			DocumentID: doc.ID,
			Content:    "Test content",
			Path:       "root",
			ChunkIndex: &index,
			Metadata:   map[string]interface{}{},
		}
		err = chunksDbHandler.InsertChunk(chunks[i])
		require.NoError(t, err)
	}

	// Test GetByDocument
	retrievedChunks, err := chunksDbHandler.SelectAllChunksByDocument(doc.RID)
	assert.NoError(t, err, "Expected GetByDocument to not return an error")
	assert.Len(t, retrievedChunks, chunkCount, "Expected to retrieve all chunks")

	// Cleanup
	for _, chunk := range chunks {
		chunksDbHandler.DeleteChunk(chunk.ID)
	}
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestChunksSearchBySimilarity(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, nil, 384, true)
	require.NoError(t, err)

	// Create a document
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create chunks with 384-dimension embeddings
	embeddings := make([][]float32, 3)
	for i := range embeddings {
		embeddings[i] = make([]float32, 384)
		// Set one dimension to 1.0 to make them distinct
		embeddings[i][i] = 1.0
	}

	chunks := make([]*model.Chunk, len(embeddings))
	for i, emb := range embeddings {
		chunks[i] = &model.Chunk{
			DocumentID: doc.ID,
			Content:    "Test content",
			Path:       "root",
			Embedding:  emb,
			Metadata:   map[string]interface{}{},
		}
		err = chunksDbHandler.InsertChunk(chunks[i])
		require.NoError(t, err)
	}

	// Search for similar chunks - create 384-dimension query
	queryEmbedding := make([]float32, 384)
	queryEmbedding[0] = 0.9
	queryEmbedding[1] = 0.1
	results, err := chunksDbHandler.SelectChunksBySimilarity(queryEmbedding, 2, 0.0, nil)
	assert.NoError(t, err, "Expected SearchBySimilarity to not return an error")
	assert.NotEmpty(t, results, "Expected to find similar chunks")
	assert.LessOrEqual(t, len(results), 2, "Expected at most 2 results")

	// Cleanup
	for _, chunk := range chunks {
		chunksDbHandler.DeleteChunk(chunk.ID)
	}
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestChunksDelete(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, nil, 384, true)
	require.NoError(t, err)

	// Create a document and chunk
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	chunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Test content",
		Path:       "root",
		Metadata:   map[string]interface{}{},
	}
	err = chunksDbHandler.InsertChunk(chunk)
	require.NoError(t, err)

	// Delete the chunk
	err = chunksDbHandler.DeleteChunk(chunk.ID)
	assert.NoError(t, err, "Expected Delete to not return an error")

	// Verify deletion
	_, err = chunksDbHandler.SelectChunk(chunk.ID)
	assert.Error(t, err, "Expected Get to return an error for deleted chunk")

	// Cleanup
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestChunksUpdateEmbedding(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, nil, 384, true)
	require.NoError(t, err)

	// Create a document and chunk
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create 384-dimension embedding
	embedding := make([]float32, 384)
	for i := range embedding {
		embedding[i] = 0.1
	}
	chunk := &model.Chunk{
		DocumentID: doc.ID,
		Content:    "Test content",
		Path:       "root",
		Embedding:  embedding,
		Metadata:   map[string]interface{}{},
	}
	err = chunksDbHandler.InsertChunk(chunk)
	require.NoError(t, err)

	// Update embedding - create new 384-dimension embedding
	newEmbedding := make([]float32, 384)
	for i := range newEmbedding {
		newEmbedding[i] = 0.5
	}
	err = chunksDbHandler.UpdateChunkEmbedding(chunk.ID, newEmbedding)
	assert.NoError(t, err, "Expected UpdateEmbedding to not return an error")

	// Verify update
	retrievedChunk, err := chunksDbHandler.SelectChunk(chunk.ID)
	require.NoError(t, err)
	assert.Equal(t, newEmbedding, retrievedChunk.Embedding, "Expected embedding to be updated")

	// Cleanup
	chunksDbHandler.DeleteChunk(chunk.ID)
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestChunksSelectSiblingChunks(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, nil, 384, true)
	require.NoError(t, err)

	// Create a document
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create a hierarchical structure:
	// root
	//   ├── section1
	//   │   ├── para1
	//   │   └── para2
	//   ├── section2
	//   │   ├── para1
	//   │   └── para2
	//   └── section3
	chunks := []*model.Chunk{
		{DocumentID: doc.ID, Content: "Root", Path: "root", Metadata: map[string]interface{}{}},
		{DocumentID: doc.ID, Content: "Section 1", Path: "root.section1", Metadata: map[string]interface{}{}},
		{DocumentID: doc.ID, Content: "Section 1 Para 1", Path: "root.section1.para1", Metadata: map[string]interface{}{}},
		{DocumentID: doc.ID, Content: "Section 1 Para 2", Path: "root.section1.para2", Metadata: map[string]interface{}{}},
		{DocumentID: doc.ID, Content: "Section 2", Path: "root.section2", Metadata: map[string]interface{}{}},
		{DocumentID: doc.ID, Content: "Section 2 Para 1", Path: "root.section2.para1", Metadata: map[string]interface{}{}},
		{DocumentID: doc.ID, Content: "Section 2 Para 2", Path: "root.section2.para2", Metadata: map[string]interface{}{}},
		{DocumentID: doc.ID, Content: "Section 3", Path: "root.section3", Metadata: map[string]interface{}{}},
	}

	for _, chunk := range chunks {
		err = chunksDbHandler.InsertChunk(chunk)
		require.NoError(t, err)
	}

	t.Run("Siblings at section level", func(t *testing.T) {
		// Find siblings of section1 (should be section2 and section3)
		siblings, err := chunksDbHandler.SelectSiblingChunks("root.section1")
		assert.NoError(t, err, "Expected SelectSiblingChunks to not return an error")
		assert.Len(t, siblings, 2, "Expected to find 2 siblings")

		// Verify the sibling paths
		siblingPaths := make([]string, len(siblings))
		for i, sibling := range siblings {
			siblingPaths[i] = sibling.Path
		}
		assert.Contains(t, siblingPaths, "root.section2")
		assert.Contains(t, siblingPaths, "root.section3")
	})

	t.Run("Siblings at paragraph level", func(t *testing.T) {
		// Find siblings of section1.para1 (should be section1.para2)
		siblings, err := chunksDbHandler.SelectSiblingChunks("root.section1.para1")
		assert.NoError(t, err, "Expected SelectSiblingChunks to not return an error")
		assert.Len(t, siblings, 1, "Expected to find 1 sibling")
		assert.Equal(t, "root.section1.para2", siblings[0].Path)
	})

	t.Run("No siblings (only child)", func(t *testing.T) {
		// Find siblings of root (has no siblings)
		siblings, err := chunksDbHandler.SelectSiblingChunks("root")
		assert.NoError(t, err, "Expected SelectSiblingChunks to not return an error")
		assert.Empty(t, siblings, "Expected no siblings for root")
	})

	t.Run("Siblings at different parent", func(t *testing.T) {
		// Find siblings of section2.para1 (should be section2.para2, not section1.para1)
		siblings, err := chunksDbHandler.SelectSiblingChunks("root.section2.para1")
		assert.NoError(t, err, "Expected SelectSiblingChunks to not return an error")
		assert.Len(t, siblings, 1, "Expected to find 1 sibling")
		assert.Equal(t, "root.section2.para2", siblings[0].Path)

		// Verify section1.para1 is not included
		for _, sibling := range siblings {
			assert.NotEqual(t, "root.section1.para1", sibling.Path)
		}
	})

	// Cleanup
	for _, chunk := range chunks {
		chunksDbHandler.DeleteChunk(chunk.ID)
	}
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestSelectAllChunksByPathDescendant(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, nil, 384, true)
	require.NoError(t, err)

	// Create a document
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create hierarchical chunks
	chunks := []*model.Chunk{
		{
			DocumentID: doc.ID,
			Content:    "Root content",
			Path:       "root",
			Metadata:   map[string]interface{}{},
		},
		{
			DocumentID: doc.ID,
			Content:    "Section 1",
			Path:       "root.section1",
			Metadata:   map[string]interface{}{},
		},
		{
			DocumentID: doc.ID,
			Content:    "Section 2",
			Path:       "root.section2",
			Metadata:   map[string]interface{}{},
		},
		{
			DocumentID: doc.ID,
			Content:    "Paragraph 1",
			Path:       "root.section1.para1",
			Metadata:   map[string]interface{}{},
		},
	}

	for _, chunk := range chunks {
		err = chunksDbHandler.InsertChunk(chunk)
		require.NoError(t, err)
	}

	t.Run("Get all descendants", func(t *testing.T) {
		descendants, err := chunksDbHandler.SelectAllChunksByPathDescendant("root")
		assert.NoError(t, err)
		assert.Len(t, descendants, 4, "Expected 4 nodes (root + 3 descendants)")
	})

	t.Run("Get descendants of section", func(t *testing.T) {
		descendants, err := chunksDbHandler.SelectAllChunksByPathDescendant("root.section1")
		assert.NoError(t, err)
		assert.Len(t, descendants, 2, "Expected 2 nodes (section1 + para1)")
		// Should include section1 and its child
		paths := make(map[string]bool)
		for _, chunk := range descendants {
			paths[chunk.Path] = true
		}
		assert.True(t, paths["root.section1"])
		assert.True(t, paths["root.section1.para1"])
	})

	t.Run("Get descendants of leaf", func(t *testing.T) {
		descendants, err := chunksDbHandler.SelectAllChunksByPathDescendant("root.section1.para1")
		assert.NoError(t, err)
		assert.Len(t, descendants, 1, "Expected 1 node (the leaf itself)")
		assert.Equal(t, "root.section1.para1", descendants[0].Path)
	})

	// Cleanup
	for _, chunk := range chunks {
		chunksDbHandler.DeleteChunk(chunk.ID)
	}
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestSelectAllChunksByPathAncestor(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, nil, 384, true)
	require.NoError(t, err)

	// Create a document
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create hierarchical chunks
	chunks := []*model.Chunk{
		{
			DocumentID: doc.ID,
			Content:    "Root content",
			Path:       "root",
			Metadata:   map[string]interface{}{},
		},
		{
			DocumentID: doc.ID,
			Content:    "Section 1",
			Path:       "root.section1",
			Metadata:   map[string]interface{}{},
		},
		{
			DocumentID: doc.ID,
			Content:    "Paragraph 1",
			Path:       "root.section1.para1",
			Metadata:   map[string]interface{}{},
		},
	}

	for _, chunk := range chunks {
		err = chunksDbHandler.InsertChunk(chunk)
		require.NoError(t, err)
	}

	t.Run("Get all ancestors of leaf", func(t *testing.T) {
		ancestors, err := chunksDbHandler.SelectAllChunksByPathAncestor("root.section1.para1")
		assert.NoError(t, err)
		assert.Len(t, ancestors, 3, "Expected 3 nodes (self + 2 ancestors)")

		paths := make(map[string]bool)
		for _, chunk := range ancestors {
			paths[chunk.Path] = true
		}
		assert.True(t, paths["root"])
		assert.True(t, paths["root.section1"])
		assert.True(t, paths["root.section1.para1"])
	})

	t.Run("Get ancestors of section", func(t *testing.T) {
		ancestors, err := chunksDbHandler.SelectAllChunksByPathAncestor("root.section1")
		assert.NoError(t, err)
		assert.Len(t, ancestors, 2, "Expected 2 nodes (self + root)")
		paths := make(map[string]bool)
		for _, chunk := range ancestors {
			paths[chunk.Path] = true
		}
		assert.True(t, paths["root"])
		assert.True(t, paths["root.section1"])
	})

	t.Run("Get ancestors of root", func(t *testing.T) {
		ancestors, err := chunksDbHandler.SelectAllChunksByPathAncestor("root")
		assert.NoError(t, err)
		assert.Len(t, ancestors, 1, "Expected 1 node (root itself)")
		assert.Equal(t, "root", ancestors[0].Path)
	})

	// Cleanup
	for _, chunk := range chunks {
		chunksDbHandler.DeleteChunk(chunk.ID)
	}
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestSelectChunksBySimilarityWithContext(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	chunksDbHandler, err := NewChunksDBHandler(database, nil, 384, true)
	require.NoError(t, err)

	// Create a document
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create test embedding
	embedding := make([]float32, 384)
	for i := range embedding {
		embedding[i] = 0.5
	}

	// Create chunks with hierarchy
	chunks := []*model.Chunk{
		{
			DocumentID: doc.ID,
			Content:    "Parent content",
			Path:       "root",
			Metadata:   map[string]interface{}{},
			Embedding:  embedding,
		},
		{
			DocumentID: doc.ID,
			Content:    "Child content",
			Path:       "root.section1",
			Metadata:   map[string]interface{}{},
			Embedding:  embedding,
		},
	}

	for _, chunk := range chunks {
		err = chunksDbHandler.InsertChunk(chunk)
		require.NoError(t, err)
	}

	t.Run("Search with context", func(t *testing.T) {
		queryEmbedding := make([]float32, 384)
		for i := range queryEmbedding {
			queryEmbedding[i] = 0.5
		}

		results, err := chunksDbHandler.SelectChunksBySimilarityWithContext(queryEmbedding, 10, true, true, 0.0, nil)
		assert.NoError(t, err)
		assert.NotEmpty(t, results)
	})

	// Cleanup
	for _, chunk := range chunks {
		chunksDbHandler.DeleteChunk(chunk.ID)
	}
	documentsDbHandler.DeleteDocument(doc.RID)
}
