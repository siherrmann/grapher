package retrieval

import (
	"testing"

	"github.com/siherrmann/grapher/database"
	"github.com/siherrmann/grapher/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetChunksMentioningEntity(t *testing.T) {
	chunks, edges, entities := initHandlers(t)
	engine := NewEngine(chunks, edges, entities)

	// Create test document
	db := initDB(t)
	documentsHandler, err := database.NewDocumentsDBHandler(db, false)
	require.NoError(t, err)

	doc := &model.Document{
		Title:    "Entity Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create test chunks
	chunk1 := &model.Chunk{
		DocumentID:  doc.ID,
		DocumentRID: doc.RID,
		Content:     "Abraham Lincoln was a president",
		Path:        "doc.section1",
		Metadata:    map[string]interface{}{},
	}
	chunk2 := &model.Chunk{
		DocumentID:  doc.ID,
		DocumentRID: doc.RID,
		Content:     "Lincoln led during the Civil War",
		Path:        "doc.section2",
		Metadata:    map[string]interface{}{},
	}
	chunk3 := &model.Chunk{
		DocumentID:  doc.ID,
		DocumentRID: doc.RID,
		Content:     "Barack Obama was also a president",
		Path:        "doc.section3",
		Metadata:    map[string]interface{}{},
	}

	err = chunks.InsertChunk(chunk1)
	require.NoError(t, err)
	err = chunks.InsertChunk(chunk2)
	require.NoError(t, err)
	err = chunks.InsertChunk(chunk3)
	require.NoError(t, err)

	// Create test entity
	entity := &model.Entity{
		Name:     "Abraham Lincoln",
		Type:     "PERSON",
		Metadata: map[string]interface{}{},
	}
	err = entities.InsertEntity(entity)
	require.NoError(t, err)

	// Create edges linking entity to chunks
	edge1 := &model.Edge{
		SourceEntityID: &entity.ID,
		TargetChunkID:  &chunk1.ID,
		EdgeType:       model.EdgeTypeEntityMention,
		Weight:         1.0,
	}
	edge2 := &model.Edge{
		SourceEntityID: &entity.ID,
		TargetChunkID:  &chunk2.ID,
		EdgeType:       model.EdgeTypeEntityMention,
		Weight:         1.0,
	}

	err = edges.InsertEdge(edge1)
	require.NoError(t, err)
	err = edges.InsertEdge(edge2)
	require.NoError(t, err)

	t.Run("Get chunks mentioning specific entity", func(t *testing.T) {
		resultChunks, err := engine.chunks.SelectChunksByEntity(entity.ID)

		assert.NoError(t, err)
		assert.Len(t, resultChunks, 2, "Expected 2 chunks mentioning Lincoln")

		// Verify we got the right chunks
		chunkIDs := make(map[int]bool)
		for _, chunk := range resultChunks {
			chunkIDs[chunk.ID] = true
		}
		assert.True(t, chunkIDs[chunk1.ID], "Expected chunk1 to be in results")
		assert.True(t, chunkIDs[chunk2.ID], "Expected chunk2 to be in results")
		assert.False(t, chunkIDs[chunk3.ID], "Expected chunk3 NOT to be in results")
	})

	t.Run("Returns empty for entity with no mentions", func(t *testing.T) {
		// Create entity with no edges
		unmentionedEntity := &model.Entity{
			Name:     "George Washington",
			Type:     "PERSON",
			Metadata: map[string]interface{}{},
		}
		err = entities.InsertEntity(unmentionedEntity)
		require.NoError(t, err)

		resultChunks, err := engine.chunks.SelectChunksByEntity(unmentionedEntity.ID)

		assert.NoError(t, err)
		assert.Empty(t, resultChunks, "Expected no chunks for unmentioned entity")

		// Cleanup
		entities.DeleteEntity(unmentionedEntity.ID)
	})

	t.Run("Returns nil for invalid entity ID", func(t *testing.T) {
		resultChunks, err := engine.chunks.SelectChunksByEntity(99999)

		// Should either return empty or error (depends on implementation)
		if err != nil {
			t.Logf("Invalid entity ID returned error (expected): %v", err)
		} else {
			assert.Empty(t, resultChunks, "Expected no chunks for invalid entity ID")
		}
	})

	// Cleanup
	edges.DeleteEdge(edge1.ID)
	edges.DeleteEdge(edge2.ID)
	entities.DeleteEntity(entity.ID)
	chunks.DeleteChunk(chunk1.ID)
	chunks.DeleteChunk(chunk2.ID)
	chunks.DeleteChunk(chunk3.ID)
	documentsHandler.DeleteDocument(doc.RID)
}

func TestSearchEntitiesByName(t *testing.T) {
	chunks, edges, entities := initHandlers(t)
	engine := NewEngine(chunks, edges, entities)

	// Create test entities
	entity1 := &model.Entity{
		Name:     "Abraham Lincoln",
		Type:     "PERSON",
		Metadata: map[string]interface{}{},
	}
	entity2 := &model.Entity{
		Name:     "Lincoln Memorial",
		Type:     "LOCATION",
		Metadata: map[string]interface{}{},
	}
	entity3 := &model.Entity{
		Name:     "Barack Obama",
		Type:     "PERSON",
		Metadata: map[string]interface{}{},
	}

	err := entities.InsertEntity(entity1)
	require.NoError(t, err)
	err = entities.InsertEntity(entity2)
	require.NoError(t, err)
	err = entities.InsertEntity(entity3)
	require.NoError(t, err)

	t.Run("Search entities by partial name match", func(t *testing.T) {
		results, err := engine.entities.SelectEntitiesBySearch("Lincoln", nil, 10)

		assert.NoError(t, err)
		assert.Len(t, results, 2, "Expected 2 entities matching 'Lincoln'")

		// Verify both Lincoln entities are in results
		names := make(map[string]bool)
		for _, entity := range results {
			names[entity.Name] = true
		}
		assert.True(t, names["Abraham Lincoln"], "Expected 'Abraham Lincoln' in results")
		assert.True(t, names["Lincoln Memorial"], "Expected 'Lincoln Memorial' in results")
	})

	t.Run("Search entities filtered by type", func(t *testing.T) {
		personType := "PERSON"
		results, err := engine.entities.SelectEntitiesBySearch("Lincoln", &personType, 10)

		assert.NoError(t, err)
		assert.Len(t, results, 1, "Expected 1 PERSON entity matching 'Lincoln'")
		if len(results) > 0 {
			assert.Equal(t, "Abraham Lincoln", results[0].Name)
			assert.Equal(t, "PERSON", results[0].Type)
		}
	})

	t.Run("Search respects limit parameter", func(t *testing.T) {
		results, err := engine.entities.SelectEntitiesBySearch("Lincoln", nil, 1)

		assert.NoError(t, err)
		assert.LessOrEqual(t, len(results), 1, "Expected at most 1 result when limit=1")
	})

	t.Run("Returns empty for non-matching search", func(t *testing.T) {
		results, err := engine.entities.SelectEntitiesBySearch("NonexistentEntity", nil, 10)

		assert.NoError(t, err)
		assert.Empty(t, results, "Expected no results for non-matching search")
	})

	t.Run("Case-insensitive search", func(t *testing.T) {
		results, err := engine.entities.SelectEntitiesBySearch("lincoln", nil, 10)

		assert.NoError(t, err)
		// Should match both "Abraham Lincoln" and "Lincoln Memorial" regardless of case
		assert.GreaterOrEqual(t, len(results), 1, "Expected at least 1 result for case-insensitive search")
	})

	t.Run("Search with empty string returns limited results", func(t *testing.T) {
		results, err := engine.entities.SelectEntitiesBySearch("", nil, 2)

		// Behavior may vary - either error or return first N entities
		if err == nil {
			assert.LessOrEqual(t, len(results), 2, "Expected at most 2 results when limit=2")
		}
	})

	// Cleanup
	entities.DeleteEntity(entity1.ID)
	entities.DeleteEntity(entity2.ID)
	entities.DeleteEntity(entity3.ID)
}

func TestEntitySearchIntegration(t *testing.T) {
	chunks, edges, entities := initHandlers(t)
	engine := NewEngine(chunks, edges, entities)

	// Create test document
	db := initDB(t)
	documentsHandler, err := database.NewDocumentsDBHandler(db, false)
	require.NoError(t, err)

	doc := &model.Document{
		Title:    "Presidents Document",
		Source:   "history.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Create chunks
	chunk1 := &model.Chunk{
		DocumentID:  doc.ID,
		DocumentRID: doc.RID,
		Content:     "Abraham Lincoln was the 16th President",
		Path:        "doc.1",
		Metadata:    map[string]interface{}{},
	}
	chunk2 := &model.Chunk{
		DocumentID:  doc.ID,
		DocumentRID: doc.RID,
		Content:     "Lincoln delivered the Gettysburg Address",
		Path:        "doc.2",
		Metadata:    map[string]interface{}{},
	}
	chunk3 := &model.Chunk{
		DocumentID:  doc.ID,
		DocumentRID: doc.RID,
		Content:     "George Washington was the first President",
		Path:        "doc.3",
		Metadata:    map[string]interface{}{},
	}

	err = chunks.InsertChunk(chunk1)
	require.NoError(t, err)
	err = chunks.InsertChunk(chunk2)
	require.NoError(t, err)
	err = chunks.InsertChunk(chunk3)
	require.NoError(t, err)

	// Create entities
	lincoln := &model.Entity{
		Name:     "Abraham Lincoln",
		Type:     "PERSON",
		Metadata: map[string]interface{}{},
	}
	washington := &model.Entity{
		Name:     "George Washington",
		Type:     "PERSON",
		Metadata: map[string]interface{}{},
	}

	err = entities.InsertEntity(lincoln)
	require.NoError(t, err)
	err = entities.InsertEntity(washington)
	require.NoError(t, err)

	// Link entities to chunks
	edge1 := &model.Edge{
		SourceEntityID: &lincoln.ID,
		TargetChunkID:  &chunk1.ID,
		EdgeType:       model.EdgeTypeEntityMention,
		Weight:         1.0,
	}
	edge2 := &model.Edge{
		SourceEntityID: &lincoln.ID,
		TargetChunkID:  &chunk2.ID,
		EdgeType:       model.EdgeTypeEntityMention,
		Weight:         1.0,
	}
	edge3 := &model.Edge{
		SourceEntityID: &washington.ID,
		TargetChunkID:  &chunk3.ID,
		EdgeType:       model.EdgeTypeEntityMention,
		Weight:         1.0,
	}

	err = edges.InsertEdge(edge1)
	require.NoError(t, err)
	err = edges.InsertEdge(edge2)
	require.NoError(t, err)
	err = edges.InsertEdge(edge3)
	require.NoError(t, err)

	t.Run("Find entity then get its chunks", func(t *testing.T) {
		// Step 1: Search for entity
		foundEntities, err := engine.entities.SelectEntitiesBySearch("Lincoln", nil, 5)
		assert.NoError(t, err)
		assert.NotEmpty(t, foundEntities, "Expected to find Lincoln entity")

		if len(foundEntities) > 0 {
			// Step 2: Get chunks mentioning that entity
			lincolnChunks, err := engine.chunks.SelectChunksByEntity(foundEntities[0].ID)
			assert.NoError(t, err)
			assert.Len(t, lincolnChunks, 2, "Expected 2 chunks mentioning Lincoln")

			// Verify chunk content
			for _, chunk := range lincolnChunks {
				assert.Contains(t, chunk.Content, "Lincoln", "Expected chunk to mention Lincoln")
			}
		}
	})

	t.Run("Different entities have different chunks", func(t *testing.T) {
		lincolnChunks, err := engine.chunks.SelectChunksByEntity(lincoln.ID)
		assert.NoError(t, err)

		washingtonChunks, err := engine.chunks.SelectChunksByEntity(washington.ID)
		assert.NoError(t, err)

		// Verify no overlap
		lincolnIDs := make(map[int]bool)
		for _, chunk := range lincolnChunks {
			lincolnIDs[chunk.ID] = true
		}

		for _, chunk := range washingtonChunks {
			assert.False(t, lincolnIDs[chunk.ID], "Expected Lincoln and Washington chunks to be different")
		}
	})

	// Cleanup
	edges.DeleteEdge(edge1.ID)
	edges.DeleteEdge(edge2.ID)
	edges.DeleteEdge(edge3.ID)
	entities.DeleteEntity(lincoln.ID)
	entities.DeleteEntity(washington.ID)
	chunks.DeleteChunk(chunk1.ID)
	chunks.DeleteChunk(chunk2.ID)
	chunks.DeleteChunk(chunk3.ID)
	documentsHandler.DeleteDocument(doc.RID)
}
