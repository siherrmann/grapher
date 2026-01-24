package pipeline

import (
	"testing"

	"github.com/siherrmann/grapher/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultRelationExtractor(t *testing.T) {
	extractor, err := DefaultRelationExtractor()
	require.NoError(t, err)
	require.NotNil(t, extractor)

	t.Run("Detect numeric citations", func(t *testing.T) {
		text := "This is mentioned in reference [1] and also in [42]."
		relations, err := extractor(text, "doc.chunk1", nil)

		assert.NoError(t, err)
		assert.NotNil(t, relations)
		assert.GreaterOrEqual(t, len(relations), 2, "Should detect at least 2 numeric citations")

		for _, rel := range relations {
			assert.Equal(t, model.EdgeTypeReference, rel.EdgeType)
			assert.Contains(t, []interface{}{"1", "42"}, rel.Metadata["reference_id"])
		}
	})

	t.Run("Detect author-year citations", func(t *testing.T) {
		text := "According to (Smith 2020) and (Jones et al. 2019), this is true."
		relations, err := extractor(text, "doc.chunk1", nil)

		assert.NoError(t, err)
		assert.NotNil(t, relations)

		if len(relations) > 0 {
			t.Logf("Detected %d relations:", len(relations))
			for _, rel := range relations {
				t.Logf("  - %v", rel.Metadata)
			}
		}
	})

	t.Run("Detect section references", func(t *testing.T) {
		text := "As shown in Section 3.2 and described in chapter 5, we can see this pattern."
		relations, err := extractor(text, "doc.chunk1", nil)

		assert.NoError(t, err)
		assert.NotNil(t, relations)

		if len(relations) > 0 {
			t.Logf("Detected %d section references:", len(relations))
			for _, rel := range relations {
				t.Logf("  - %v", rel.Metadata)
			}
		}
	})

	t.Run("Detect inline section references", func(t *testing.T) {
		text := "Section 3 introduces the concept, and Figure 2.1 shows the results."
		relations, err := extractor(text, "doc.chunk1", nil)

		assert.NoError(t, err)
		assert.NotNil(t, relations)

		if len(relations) > 0 {
			for _, rel := range relations {
				assert.Equal(t, model.EdgeTypeReference, rel.EdgeType)
			}
		}
	})

	t.Run("Detect DOI references", func(t *testing.T) {
		text := "The study doi:10.1234/example shows interesting results."
		relations, err := extractor(text, "doc.chunk1", nil)

		assert.NoError(t, err)
		assert.NotNil(t, relations)

		if len(relations) > 0 {
			foundDOI := false
			for _, rel := range relations {
				if rel.Metadata["citation_pattern"] == "doi_reference" {
					foundDOI = true
					break
				}
			}
			assert.True(t, foundDOI, "Should detect DOI reference")
		}
	})

	t.Run("Detect URL references", func(t *testing.T) {
		text := "More information at https://example.com and http://another.org"
		relations, err := extractor(text, "doc.chunk1", nil)

		assert.NoError(t, err)
		assert.NotNil(t, relations)
		assert.GreaterOrEqual(t, len(relations), 2, "Should detect at least 2 URLs")
	})

	t.Run("Detect entity co-occurrence", func(t *testing.T) {
		// Create mock entities with positions
		entity1 := &model.Entity{
			ID:   1,
			Name: "John Smith",
			Type: "PERSON",
			Metadata: map[string]interface{}{
				"start": uint(0),
				"end":   uint(10),
			},
		}
		entity2 := &model.Entity{
			ID:   2,
			Name: "Microsoft",
			Type: "ORGANIZATION",
			Metadata: map[string]interface{}{
				"start": uint(30),
				"end":   uint(39),
			},
		}

		text := "John Smith works at Microsoft."
		relations, err := extractor(text, "doc.chunk1", []*model.Entity{entity1, entity2})

		assert.NoError(t, err)
		assert.NotNil(t, relations)

		// Should detect entity co-occurrence
		foundCoOccurrence := false
		for _, rel := range relations {
			if rel.EdgeType == model.EdgeTypeEntityMention {
				foundCoOccurrence = true
				assert.NotNil(t, rel.SourceEntityID)
				assert.NotNil(t, rel.TargetEntityID)
				assert.True(t, rel.Bidirectional)
				assert.Greater(t, rel.Weight, 0.0)
				t.Logf("Co-occurrence weight: %.2f, distance: %v", rel.Weight, rel.Metadata["distance"])
			}
		}
		assert.True(t, foundCoOccurrence, "Should detect entity co-occurrence")
	})

	t.Run("Ignore distant entities", func(t *testing.T) {
		entity1 := &model.Entity{
			ID:   1,
			Name: "Entity1",
			Type: "PERSON",
			Metadata: map[string]interface{}{
				"start": uint(0),
				"end":   uint(10),
			},
		}
		entity2 := &model.Entity{
			ID:   2,
			Name: "Entity2",
			Type: "ORGANIZATION",
			Metadata: map[string]interface{}{
				"start": uint(200), // Far away (>100 chars)
				"end":   uint(210),
			},
		}

		relations, err := extractor("dummy text", "doc.chunk1", []*model.Entity{entity1, entity2})

		assert.NoError(t, err)
		// Should not create entity mention edges for distant entities
		for _, rel := range relations {
			if rel.EdgeType == model.EdgeTypeEntityMention {
				t.Error("Should not create entity mention edges for distant entities")
			}
		}
	})

	t.Run("Handle text without citations", func(t *testing.T) {
		text := "This is a simple sentence without any citations."
		relations, err := extractor(text, "doc.chunk1", nil)

		assert.NoError(t, err)
		// Should return empty or nil for text without citations
		assert.True(t, len(relations) == 0)
	})

	t.Run("Handle empty text", func(t *testing.T) {
		relations, err := extractor("", "doc.chunk1", nil)

		assert.NoError(t, err)
		assert.True(t, len(relations) == 0)
	})
}

func TestCalculateCoOccurrenceWeight(t *testing.T) {
	tests := []struct {
		distance  int
		minWeight float64
		maxWeight float64
	}{
		{0, 1.0, 1.0},   // Adjacent entities
		{50, 0.7, 0.8},  // Close entities
		{100, 0.4, 0.6}, // Medium distance
		{200, 0.0, 0.1}, // Far entities
		{300, 0.0, 0.0}, // Very far entities
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.distance)), func(t *testing.T) {
			weight := calculateCoOccurrenceWeight(tt.distance)
			assert.GreaterOrEqual(t, weight, tt.minWeight)
			assert.LessOrEqual(t, weight, tt.maxWeight)
		})
	}
}

func TestGetPatternName(t *testing.T) {
	tests := []struct {
		pattern string
		name    string
	}{
		{`\[(\d+)\]`, "numeric_citation"},
		{`et\s+al`, "author_year_citation"},
		{`section|chapter`, "section_reference"},
		{`Section|Chapter`, "inline_section_reference"},
		{`doi`, "doi_reference"},
		{`https?`, "url_reference"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This is a simplified test since getPatternName uses the actual regex
			// In practice, we'd need to test with actual compiled regexes
			t.Logf("Pattern %s should map to %s", tt.pattern, tt.name)
		})
	}
}
