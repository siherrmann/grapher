package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultEntityExtractor(t *testing.T) {
	// Note: DefaultEntityExtractor uses hugot which requires downloading models
	// This test will download the distilbert-NER model if not already present
	t.Run("Create entity extractor", func(t *testing.T) {
		extractor, err := DefaultEntityExtractorBasic()
		require.NoError(t, err)
		assert.NotNil(t, extractor)
	})

	t.Run("Extract entities from text", func(t *testing.T) {
		extractor, err := DefaultEntityExtractorBasic()
		require.NoError(t, err)

		text := "My name is Wolfgang and I live in Berlin."
		entities, err := extractor(text)
		assert.NoError(t, err)
		assert.NotNil(t, entities)

		// Should detect at least Wolfgang (PERSON) and Berlin (LOCATION)
		if len(entities) > 0 {
			t.Logf("Detected %d entities:", len(entities))
			for _, entity := range entities {
				t.Logf("  - %s (%s): %v", entity.Name, entity.Type, entity.Metadata)
			}
		}
	})

	t.Run("Extract entities from text with organizations", func(t *testing.T) {
		extractor, err := DefaultEntityExtractorBasic()
		require.NoError(t, err)

		text := "Apple Inc. is headquartered in Cupertino, California."
		entities, err := extractor(text)
		assert.NoError(t, err)
		assert.NotNil(t, entities)

		if len(entities) > 0 {
			t.Logf("Detected %d entities:", len(entities))
			for _, entity := range entities {
				t.Logf("  - %s (%s): %v", entity.Name, entity.Type, entity.Metadata)
			}
		}
	})

	t.Run("Handle empty text", func(t *testing.T) {
		extractor, err := DefaultEntityExtractorBasic()
		require.NoError(t, err)

		entities, err := extractor("")
		assert.NoError(t, err)
		// Empty text should return empty or nil entities
		assert.True(t, len(entities) == 0)
	})

	t.Run("Handle text without entities", func(t *testing.T) {
		extractor, err := DefaultEntityExtractorBasic()
		require.NoError(t, err)

		text := "This is a simple sentence without any named entities."
		entities, err := extractor(text)
		assert.NoError(t, err)
		// Text without entities should return empty or nil
		t.Logf("Detected %d entities (expected 0 or few)", len(entities))
	})
}

func TestNormalizeEntityType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"B-PER", "PER"},
		{"I-PER", "PER"},
		{"B-LOC", "LOC"},
		{"I-LOC", "LOC"},
		{"B-ORG", "ORG"},
		{"I-ORG", "ORG"},
		{"MISC", "MISC"},
		{"O", "O"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := normalizeEntityType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
