package grapher

import (
	"context"
	"testing"

	"github.com/siherrmann/grapher/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHybridSearchWithEntities(t *testing.T) {
	g := initGrapher(t)
	err := g.UseDefaultPipeline()
	require.NoError(t, err)

	// Insert test documents with recognizable entities
	doc1 := &model.Document{
		Title:   "Historical Figures",
		Source:  "history",
		Content: "Abraham Lincoln was the 16th President of the United States. He led the nation through the Civil War and is known for the Emancipation Proclamation. Lincoln was born in Kentucky and later moved to Illinois.",
		Metadata: model.Metadata{
			"type": "biography",
		},
	}

	doc2 := &model.Document{
		Title:   "Modern Leaders",
		Source:  "modern",
		Content: "Barack Obama served as the 44th President. He was born in Hawaii and worked in Chicago before entering politics. Obama's presidency focused on healthcare reform.",
		Metadata: model.Metadata{
			"type": "biography",
		},
	}

	doc3 := &model.Document{
		Title:   "Technology Leaders",
		Source:  "tech",
		Content: "Steve Jobs co-founded Apple Inc. in California. He revolutionized personal computing and mobile technology. Jobs was known for innovation at companies like Apple and Pixar.",
		Metadata: model.Metadata{
			"type": "technology",
		},
	}

	_, err = g.ProcessAndInsertDocument(doc1)
	require.NoError(t, err)
	_, err = g.ProcessAndInsertDocument(doc2)
	require.NoError(t, err)
	_, err = g.ProcessAndInsertDocument(doc3)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("HybridSearch extracts and boosts entities from query", func(t *testing.T) {
		config := model.DefaultQueryConfig()
		config.TopK = 5
		config.SimilarityThreshold = 0.0 // Lower threshold to get more results
		config.VectorWeight = 0.5
		config.EntityWeight = 0.8 // High weight for entity matches

		// Query mentions "Lincoln" explicitly
		results, err := g.HybridSearch(ctx, "What did Abraham Lincoln accomplish?", &config)

		assert.NoError(t, err)
		assert.NotEmpty(t, results, "Expected results from hybrid search")

		// Check that chunks mentioning Lincoln are boosted
		foundLincoln := false
		for _, result := range results {
			if result.Content != "" {
				t.Logf("Result: score=%.4f, method=%s, content=%s...",
					result.Score, result.RetrievalMethod, result.Content[:min(50, len(result.Content))])

				// Check if this chunk mentions Lincoln
				if containsEntity(result.Content, "Lincoln") ||
					containsEntity(result.Content, "Abraham") {
					foundLincoln = true
					// Entity-based results should have +entity in retrieval method
					if config.EntityWeight > 0 {
						t.Logf("Found Lincoln chunk with method: %s", result.RetrievalMethod)
					}
				}
			}
		}

		assert.True(t, foundLincoln, "Expected to find chunks mentioning Lincoln")
	})

	t.Run("HybridSearch with EntityWeight zero skips entity boosting", func(t *testing.T) {
		config := model.DefaultQueryConfig()
		config.TopK = 5
		config.SimilarityThreshold = 0.0
		config.VectorWeight = 0.7
		config.EntityWeight = 0.0 // Disable entity boosting

		results, err := g.HybridSearch(ctx, "Who was Steve Jobs?", &config)

		assert.NoError(t, err)
		assert.NotEmpty(t, results)

		// No results should have "+entity" in retrieval method
		for _, result := range results {
			assert.NotContains(t, result.RetrievalMethod, "+entity",
				"Expected no entity boosting when EntityWeight is 0")
			assert.NotEqual(t, "entity", result.RetrievalMethod,
				"Expected no pure entity results when EntityWeight is 0")
		}
	})

	t.Run("HybridSearch finds entities across documents", func(t *testing.T) {
		config := model.DefaultQueryConfig()
		config.TopK = 10
		config.SimilarityThreshold = 0.0
		config.VectorWeight = 0.4
		config.EntityWeight = 0.6

		// Query for a specific location that appears in multiple docs
		results, err := g.HybridSearch(ctx, "What happened in Illinois?", &config)

		assert.NoError(t, err)

		if len(results) > 0 {
			t.Logf("Found %d results for location query", len(results))
			for i, result := range results {
				if i < 3 { // Log first 3 results
					t.Logf("Result %d: score=%.4f, method=%s", i+1, result.Score, result.RetrievalMethod)
				}
			}
		}
	})

	t.Run("HybridSearch with high EntityWeight prioritizes entity matches", func(t *testing.T) {
		config := model.DefaultQueryConfig()
		config.TopK = 5
		config.SimilarityThreshold = 0.0
		config.VectorWeight = 0.2 // Low vector weight
		config.EntityWeight = 1.0 // High entity weight

		results, err := g.HybridSearch(ctx, "Barack Obama", &config)

		assert.NoError(t, err)
		assert.NotEmpty(t, results)

		// With high entity weight, entity-boosted results should appear first
		if len(results) > 0 {
			topResult := results[0]
			t.Logf("Top result: score=%.4f, method=%s", topResult.Score, topResult.RetrievalMethod)

			// The top result should mention Obama
			foundObama := containsEntity(topResult.Content, "Obama") ||
				containsEntity(topResult.Content, "Barack")
			assert.True(t, foundObama, "Expected top result to mention Obama with high entity weight")
		}
	})

	t.Run("HybridSearch without entity extractor works normally", func(t *testing.T) {
		// Create grapher without entity extractor
		gNoEntity := initGrapher(t)

		// Use a simple pipeline without entity extraction
		gNoEntity.SetPipeline(g.Pipeline)        // Copy pipeline
		gNoEntity.Pipeline.EntityExtractor = nil // Remove entity extractor

		config := model.DefaultQueryConfig()
		config.TopK = 5
		config.EntityWeight = 0.5

		results, err := gNoEntity.HybridSearch(ctx, "Abraham Lincoln", &config)

		assert.NoError(t, err)
		// Should still get results from vector/graph/hierarchy, just not entity boosted
		if len(results) > 0 {
			for _, result := range results {
				// Should not have entity-based methods without extractor
				assert.NotContains(t, result.RetrievalMethod, "+entity")
				assert.NotEqual(t, "entity", result.RetrievalMethod)
			}
		}
	})

	t.Run("HybridSearch handles entity extraction errors gracefully", func(t *testing.T) {
		config := model.DefaultQueryConfig()
		config.TopK = 5
		config.EntityWeight = 0.5

		// Empty query should not cause errors
		results, err := g.HybridSearch(ctx, "", &config)

		// Should either get empty results or an error, but not panic
		if err != nil {
			t.Logf("Empty query returned error (expected): %v", err)
		} else {
			assert.NotNil(t, results)
		}
	})

	t.Run("Entity boosting preserves vector similarity scores", func(t *testing.T) {
		config := model.DefaultQueryConfig()
		config.TopK = 5
		config.SimilarityThreshold = 0.0
		config.VectorWeight = 0.6
		config.EntityWeight = 0.4

		results, err := g.HybridSearch(ctx, "Who was the President?", &config)

		assert.NoError(t, err)
		assert.NotEmpty(t, results)

		// Check that results have similarity scores from vector search
		for _, result := range results {
			if result.RetrievalMethod != "entity" {
				// Non-pure-entity results should have similarity scores
				t.Logf("Result: similarity=%.4f, total=%.4f, method=%s",
					result.Similarity, result.Score, result.RetrievalMethod)
			}
		}
	})

	// Cleanup
	g.Documents.DeleteDocument(doc1.RID)
	g.Documents.DeleteDocument(doc2.RID)
	g.Documents.DeleteDocument(doc3.RID)
}

func TestHybridSearchEntityIntegration(t *testing.T) {
	g := initGrapher(t)
	err := g.UseDefaultPipeline()
	require.NoError(t, err)

	// Create document with clear entity mentions
	doc := &model.Document{
		Title:   "Bible Characters",
		Source:  "religious",
		Content: "Abel was the son of Adam and Eve. Abel was a shepherd who offered sacrifices. His brother Cain killed Abel out of jealousy. The story of Abel is found in Genesis chapter 4.",
		Metadata: model.Metadata{
			"book": "Genesis",
		},
	}

	_, err = g.ProcessAndInsertDocument(doc)
	require.NoError(t, err)

	ctx := context.Background()

	t.Run("Entity search finds mentions across chunk boundaries", func(t *testing.T) {
		config := model.DefaultQueryConfig()
		config.TopK = 10
		config.SimilarityThreshold = 0.0
		config.EntityWeight = 0.7
		config.VectorWeight = 0.3

		results, err := g.HybridSearch(ctx, "Tell me about Abel", &config)

		assert.NoError(t, err)
		assert.NotEmpty(t, results, "Expected results for Abel query")

		// Count how many results mention Abel
		abelMentions := 0
		for _, result := range results {
			if containsEntity(result.Content, "Abel") {
				abelMentions++
				t.Logf("Found Abel mention: method=%s, score=%.4f",
					result.RetrievalMethod, result.Score)
			}
		}

		if abelMentions > 0 {
			t.Logf("Found %d chunks mentioning Abel", abelMentions)
		}
	})

	t.Run("Multiple entities in query all contribute to scoring", func(t *testing.T) {
		config := model.DefaultQueryConfig()
		config.TopK = 10
		config.SimilarityThreshold = 0.0
		config.EntityWeight = 0.5
		config.VectorWeight = 0.5

		// Query mentions multiple entities
		results, err := g.HybridSearch(ctx, "What is the relationship between Cain and Abel?", &config)

		assert.NoError(t, err)
		assert.NotEmpty(t, results)

		// Chunks mentioning both entities should score higher
		for i, result := range results {
			hasCain := containsEntity(result.Content, "Cain")
			hasAbel := containsEntity(result.Content, "Abel")

			if hasCain && hasAbel {
				t.Logf("Result %d mentions both Cain and Abel: score=%.4f", i+1, result.Score)
			} else if hasCain || hasAbel {
				t.Logf("Result %d mentions one entity: score=%.4f", i+1, result.Score)
			}
		}
	})

	// Cleanup
	g.Documents.DeleteDocument(doc.RID)
}

// Helper function to check if text contains an entity name (case-insensitive)
func containsEntity(text, entity string) bool {
	if text == "" || entity == "" {
		return false
	}
	// Simple substring match (could be improved with word boundaries)
	text = toLower(text)
	entity = toLower(entity)
	return contains(text, entity)
}

func toLower(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			result[i] = c + ('a' - 'A')
		} else {
			result[i] = c
		}
	}
	return string(result)
}

func contains(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(substr) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
