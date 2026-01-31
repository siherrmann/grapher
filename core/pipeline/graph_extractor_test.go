package pipeline

import (
	"testing"

	"github.com/siherrmann/grapher/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockEmbedder returns a simple mock embedding for testing
func mockEmbedder(text string) ([]float32, error) {
	// Return a simple fixed-size embedding based on text length
	embedding := make([]float32, 384)
	for i := range embedding {
		embedding[i] = float32(len(text)) / 100.0
	}
	return embedding, nil
}

func TestDefaultGraphExtractor(t *testing.T) {
	t.Run("Create graph extractor", func(t *testing.T) {
		extractor, err := DefaultGraphExtractor(mockEmbedder)
		if err != nil {
			t.Skipf("Skipping graph extractor test - model not available: %v", err)
			return
		}
		assert.NotNil(t, extractor)
	})

	t.Run("Extract entities and relations together", func(t *testing.T) {
		extractor, err := DefaultGraphExtractor(mockEmbedder)
		if err != nil {
			t.Skipf("Skipping graph extractor test - model not available: %v", err)
			return
		}

		text := "Punta Cana is a resort town in the municipality of Higüey, in La Altagracia Province, the easternmost province of the Dominican Republic."
		entities, edges, err := extractor(text)
		assert.NoError(t, err)
		assert.NotNil(t, entities)
		assert.NotNil(t, edges)

		if len(entities) > 0 {
			t.Logf("Detected %d entities:", len(entities))
			for _, entity := range entities {
				t.Logf("  - %s (%s)", entity.Name, entity.Type)
			}
		}

		if len(edges) > 0 {
			t.Logf("Detected %d relations:", len(edges))
			for _, edge := range edges {
				source, _ := edge.Metadata["source_entity"].(string)
				target, _ := edge.Metadata["target_entity"].(string)
				t.Logf("  - %s -[%s]-> %s", source, edge.EdgeType, target)
			}
		}

		// REBEL should detect relations like:
		// Punta Cana -> located_in -> Higüey
		// La Altagracia Province -> country -> Dominican Republic
	})

	t.Run("Handle empty text", func(t *testing.T) {
		extractor, err := DefaultGraphExtractor(mockEmbedder)
		if err != nil {
			t.Skipf("Skipping graph extractor test - model not available: %v", err)
			return
		}

		entities, edges, err := extractor("")
		assert.NoError(t, err)
		assert.True(t, len(entities) == 0)
		assert.True(t, len(edges) == 0)
	})
}

func TestParseREBELOutput(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Triplet
	}{
		{
			name:  "Single triplet",
			input: "<triplet> Punta Cana <subj> located in <obj> Higüey",
			expected: []Triplet{
				{Head: "Punta Cana", Relation: "located in", Tail: "Higüey"},
			},
		},
		{
			name:  "Multiple triplets",
			input: "<triplet> Punta Cana <subj> located in <obj> Higüey <triplet> La Altagracia Province <subj> country <obj> Dominican Republic",
			expected: []Triplet{
				{Head: "Punta Cana", Relation: "located in", Tail: "Higüey"},
				{Head: "La Altagracia Province", Relation: "country", Tail: "Dominican Republic"},
			},
		},
		{
			name:     "Empty input",
			input:    "",
			expected: []Triplet{},
		},
		{
			name:  "With extra whitespace",
			input: "<triplet>  Apple Inc.  <subj>  founded by  <obj>  Steve Jobs  ",
			expected: []Triplet{
				{Head: "Apple Inc.", Relation: "founded by", Tail: "Steve Jobs"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseREBELOutput(tt.input)
			assert.Equal(t, len(tt.expected), len(result))
			for i, expected := range tt.expected {
				if i < len(result) {
					assert.Equal(t, expected.Head, result[i].Head)
					assert.Equal(t, expected.Relation, result[i].Relation)
					assert.Equal(t, expected.Tail, result[i].Tail)
				}
			}
		})
	}
}

func TestNormalizeRelationType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"located in", "located_in"},
		{"Located In", "located_in"},
		{"FOUNDED BY", "founded_by"},
		{"capital-of", "capital_of"},
		{"part of", "part_of"},
		{"member of", "member_of"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := normalizeRelationType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGraphExtractorAdapters(t *testing.T) {
	t.Run("GraphExtractorToEntityExtractor", func(t *testing.T) {
		graphExtract, err := DefaultGraphExtractor(mockEmbedder)
		if err != nil {
			t.Skipf("Skipping adapter test - model not available: %v", err)
			return
		}

		entityExtract := GraphExtractorToEntityExtractor(graphExtract)
		assert.NotNil(t, entityExtract)

		text := "Apple Inc. was founded by Steve Jobs in Cupertino, California."
		entities, err := entityExtract(text)
		assert.NoError(t, err)
		assert.NotNil(t, entities)

		if len(entities) > 0 {
			t.Logf("Extracted %d entities via adapter:", len(entities))
			for _, e := range entities {
				t.Logf("  - %s (%s)", e.Name, e.Type)
			}
		}
	})

	t.Run("GraphExtractorToRelationExtractor", func(t *testing.T) {
		graphExtract, err := DefaultGraphExtractor(mockEmbedder)
		if err != nil {
			t.Skipf("Skipping adapter test - model not available: %v", err)
			return
		}

		relationExtract := GraphExtractorToRelationExtractor(graphExtract)
		assert.NotNil(t, relationExtract)

		text := "Apple Inc. was founded by Steve Jobs in Cupertino, California."
		edges, err := relationExtract(text, "chunk1", nil)
		assert.NoError(t, err)
		assert.NotNil(t, edges)

		if len(edges) > 0 {
			t.Logf("Extracted %d relations via adapter:", len(edges))
			for _, e := range edges {
				source, _ := e.Metadata["source_entity"].(string)
				target, _ := e.Metadata["target_entity"].(string)
				t.Logf("  - %s -[%s]-> %s", source, e.EdgeType, target)
			}
		}
	})

	t.Run("Adapter with entity filtering", func(t *testing.T) {
		graphExtract, err := DefaultGraphExtractor(mockEmbedder)
		if err != nil {
			t.Skipf("Skipping adapter test - model not available: %v", err)
			return
		}

		relationExtract := GraphExtractorToRelationExtractor(graphExtract)

		text := "Apple Inc. was founded by Steve Jobs. Microsoft was founded by Bill Gates."

		// First get all entities
		entities, _, err := graphExtract(text)
		require.NoError(t, err)

		// Filter to only Apple-related entities
		appleEntities := []*model.Entity{}
		for _, e := range entities {
			if e.Name == "Apple Inc." || e.Name == "Steve Jobs" {
				appleEntities = append(appleEntities, e)
			}
		}

		// Extract relations - should only return Apple-related ones
		edges, err := relationExtract(text, "chunk1", appleEntities)
		assert.NoError(t, err)

		if len(edges) > 0 {
			t.Logf("Filtered relations (Apple only):")
			for _, e := range edges {
				source, _ := e.Metadata["source_entity"].(string)
				target, _ := e.Metadata["target_entity"].(string)
				t.Logf("  - %s -[%s]-> %s", source, e.EdgeType, target)
			}
		}
	})
}

func TestDefaultExtractorsFromGraph(t *testing.T) {
	t.Run("DefaultEntityExtractorFromGraph", func(t *testing.T) {
		extractor, err := DefaultEntityExtractorFromGraph(mockEmbedder)
		if err != nil {
			t.Skipf("Skipping test - model not available: %v", err)
			return
		}

		text := "Berlin is the capital of Germany."
		entities, err := extractor(text)
		assert.NoError(t, err)
		assert.NotNil(t, entities)

		if len(entities) > 0 {
			t.Logf("Entities via convenience function:")
			for _, e := range entities {
				t.Logf("  - %s (%s)", e.Name, e.Type)
			}
		}
	})

	t.Run("DefaultRelationExtractorFromGraph", func(t *testing.T) {
		extractor, err := DefaultRelationExtractorFromGraph(mockEmbedder)
		if err != nil {
			t.Skipf("Skipping test - model not available: %v", err)
			return
		}

		text := "Berlin is the capital of Germany."
		edges, err := extractor(text, "chunk1", nil)
		assert.NoError(t, err)
		assert.NotNil(t, edges)

		if len(edges) > 0 {
			t.Logf("Relations via convenience function:")
			for _, e := range edges {
				source, _ := e.Metadata["source_entity"].(string)
				target, _ := e.Metadata["target_entity"].(string)
				t.Logf("  - %s -[%s]-> %s", source, e.EdgeType, target)
			}
		}
	})
}
