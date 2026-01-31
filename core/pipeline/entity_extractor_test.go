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

func TestDefaultEntityExtractorAdvanced(t *testing.T) {
	// Note: DefaultEntityExtractorAdvanced uses NuNER model from local files
	// This test requires the nuner_zero_model.onnx file in core/pipeline/models/nuner/
	t.Run("Create advanced entity extractor", func(t *testing.T) {
		extractor, err := DefaultEntityExtractorAdvanced()
		if err != nil {
			t.Skipf("Skipping advanced extractor test - model not available: %v", err)
			return
		}
		assert.NotNil(t, extractor)
	})

	t.Run("Extract entities with advanced model", func(t *testing.T) {
		extractor, err := DefaultEntityExtractorAdvanced()
		if err != nil {
			t.Skipf("Skipping advanced extractor test - model not available: %v", err)
			return
		}

		text := "My name is Wolfgang and I live in Berlin. I work at Google as a software engineer."
		entities, err := extractor(text)
		assert.NoError(t, err)
		assert.NotNil(t, entities)

		// Should detect entities like Wolfgang (person), Berlin (location/gpe), Google (organization)
		if len(entities) > 0 {
			t.Logf("Detected %d entities with advanced model:", len(entities))
			for _, entity := range entities {
				t.Logf("  - %s (%s): %v", entity.Name, entity.Type, entity.Metadata)
			}
		}
	})

	t.Run("Extract diverse entity types", func(t *testing.T) {
		extractor, err := DefaultEntityExtractorAdvanced()
		if err != nil {
			t.Skipf("Skipping advanced extractor test - model not available: %v", err)
			return
		}

		text := "The iPhone 15 was released by Apple Inc. on September 15, 2023 for $799. " +
			"Steve Jobs founded the company in Cupertino, California."
		entities, err := extractor(text)
		assert.NoError(t, err)
		assert.NotNil(t, entities)

		if len(entities) > 0 {
			t.Logf("Detected %d diverse entities:", len(entities))
			for _, entity := range entities {
				t.Logf("  - %s (%s): confidence=%.2f",
					entity.Name,
					entity.Type,
					entity.Metadata["confidence"].(float64))
			}
			// With NuNER we expect: iPhone 15 (product), Apple Inc. (organization),
			// September 15, 2023 (date), $799 (monetary value),
			// Steve Jobs (person), Cupertino (gpe/location), California (gpe/location)
		}
	})

	t.Run("Handle empty text with advanced model", func(t *testing.T) {
		extractor, err := DefaultEntityExtractorAdvanced()
		if err != nil {
			t.Skipf("Skipping advanced extractor test - model not available: %v", err)
			return
		}

		entities, err := extractor("")
		assert.NoError(t, err)
		assert.True(t, len(entities) == 0)
	})

	t.Run("Compare basic vs advanced extractors", func(t *testing.T) {
		basicExtractor, err := DefaultEntityExtractorBasic()
		require.NoError(t, err)

		advancedExtractor, err := DefaultEntityExtractorAdvanced()
		if err != nil {
			t.Skipf("Skipping comparison - advanced model not available: %v", err)
			return
		}

		text := "On January 15, 2024, John Smith sent an email to support@example.com " +
			"about a medical condition requiring $1,500 in treatment."

		basicEntities, err := basicExtractor(text)
		assert.NoError(t, err)

		advancedEntities, err := advancedExtractor(text)
		assert.NoError(t, err)

		t.Logf("Basic extractor found %d entities:", len(basicEntities))
		for _, e := range basicEntities {
			t.Logf("  - %s (%s)", e.Name, e.Type)
		}

		t.Logf("Advanced extractor found %d entities:", len(advancedEntities))
		for _, e := range advancedEntities {
			t.Logf("  - %s (%s)", e.Name, e.Type)
		}

		// Advanced should detect more entity types:
		// - date (January 15, 2024)
		// - email (support@example.com)
		// - monetary value ($1,500)
		// - medical condition
		// Basic model typically only detects PERSON, ORG, LOC, MISC
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
