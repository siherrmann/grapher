package pipeline

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/knights-analytics/hugot"
	"github.com/siherrmann/grapher/model"
)

// Triplet represents a relation triplet extracted by REBEL
type Triplet struct {
	Head     string
	Relation string
	Tail     string
}

// DefaultGraphExtractor creates a graph extractor using REBEL model
// REBEL extracts both entities and relationships in a single pass
// More efficient than separate entity and relation extraction
// Accepts an embedder to generate embeddings for entity names, enabling similarity search
func DefaultGraphExtractor(embedder EmbedFunc) (GraphExtractFunc, error) {
	// Download REBEL model from HuggingFace
	// modelName := "mrebel"
	// modelPath, err := helper.PrepareModel(modelName, "mrebel_base_model.onnx")
	// if err != nil {
	// 	return nil, err
	// }

	// Initialize hugot session with generation support
	session, err := hugot.NewGoSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create hugot session: %w", err)
	}

	// Create generation pipeline for REBEL
	config := hugot.TextGenerationConfig{
		ModelPath: "./models/mrebel/mrebel_base_model.onnx",
		Name:      "rebel-pipeline",
	}
	generationPipeline, err := hugot.NewPipeline(session, config)
	if err != nil {
		if destroyErr := session.Destroy(); destroyErr != nil {
			return nil, fmt.Errorf("failed to create REBEL pipeline: %w (cleanup error: %v)", err, destroyErr)
		}
		return nil, fmt.Errorf("failed to create REBEL pipeline: %w", err)
	}

	return func(text string) ([]*model.Entity, []*model.Edge, error) {
		// Generate triplets using REBEL
		output, err := generationPipeline.RunPipeline(context.Background(), []string{text})
		if err != nil {
			return nil, nil, fmt.Errorf("failed to generate with REBEL: %w", err)
		}

		if len(output.Responses) == 0 || output.Responses[0] == "" {
			return nil, nil, nil
		}

		// Parse REBEL output into triplets
		triplets := parseREBELOutput(output.Responses[0])

		// Extract unique entities and create edges
		entityMap := make(map[string]*model.Entity)
		var edges []*model.Edge

		for _, triplet := range triplets {
			// Create or get head entity
			if _, exists := entityMap[triplet.Head]; !exists {
				embedding, err := embedder(triplet.Head)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to generate embedding for entity %q: %w", triplet.Head, err)
				}
				entityMap[triplet.Head] = &model.Entity{
					Name: triplet.Head,
					Type: "entity",
					Metadata: map[string]interface{}{
						"source":    "rebel",
						"embedding": embedding,
					},
				}
			}

			// Create or get tail entity
			if _, exists := entityMap[triplet.Tail]; !exists {
				embedding, err := embedder(triplet.Tail)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to generate embedding for entity %q: %w", triplet.Tail, err)
				}
				entityMap[triplet.Tail] = &model.Entity{
					Name: triplet.Tail,
					Type: "entity",
					Metadata: map[string]interface{}{
						"source":    "rebel",
						"embedding": embedding,
					},
				}
			}

			// Create edge
			edges = append(edges, &model.Edge{
				EdgeType: model.EdgeType(normalizeRelationType(triplet.Relation)),
				Weight:   1.0,
				Metadata: model.Metadata{
					"source":        "rebel",
					"relation":      triplet.Relation,
					"source_entity": triplet.Head,
					"target_entity": triplet.Tail,
				},
			})
		}

		// Convert entity map to slice
		entities := make([]*model.Entity, 0, len(entityMap))
		for _, entity := range entityMap {
			entities = append(entities, entity)
		}

		return entities, edges, nil
	}, nil
}

// parseREBELOutput parses REBEL model output into triplets
// REBEL outputs format: "<triplet> head <subj> relation <obj> tail <triplet> ..."
func parseREBELOutput(generated string) []Triplet {
	var triplets []Triplet

	// Split by <triplet> marker
	tripletPattern := regexp.MustCompile(`<triplet>([^<]+)<subj>([^<]+)<obj>([^<]+)`)
	matches := tripletPattern.FindAllStringSubmatch(generated, -1)

	for _, match := range matches {
		if len(match) == 4 {
			triplets = append(triplets, Triplet{
				Head:     strings.TrimSpace(match[1]),
				Relation: strings.TrimSpace(match[2]),
				Tail:     strings.TrimSpace(match[3]),
			})
		}
	}

	return triplets
}

// normalizeRelationType normalizes relation names for consistency
func normalizeRelationType(relation string) string {
	// Convert to lowercase and replace spaces with underscores
	normalized := strings.ToLower(relation)
	normalized = strings.ReplaceAll(normalized, " ", "_")
	normalized = strings.ReplaceAll(normalized, "-", "_")
	return normalized
}

// GraphExtractorToEntityExtractor adapts a GraphExtractFunc to EntityExtractFunc
// Useful when you have a graph extractor but need to use it in entity-only context
func GraphExtractorToEntityExtractor(graphExtract GraphExtractFunc) EntityExtractFunc {
	return func(text string) ([]*model.Entity, error) {
		entities, _, err := graphExtract(text)
		return entities, err
	}
}

// GraphExtractorToRelationExtractor adapts a GraphExtractFunc to RelationExtractFunc
// Useful when you have a graph extractor but need to use it in relation-only context
// Note: This ignores the chunkID and entities parameters since REBEL extracts everything together
func GraphExtractorToRelationExtractor(graphExtract GraphExtractFunc) RelationExtractFunc {
	return func(text string, chunkID string, entities []*model.Entity) ([]*model.Edge, error) {
		_, edges, err := graphExtract(text)
		if err != nil {
			return nil, err
		}

		// Optionally filter edges to only include provided entities
		if len(entities) > 0 {
			entityNames := make(map[string]bool)
			for _, e := range entities {
				entityNames[e.Name] = true
			}

			var filteredEdges []*model.Edge
			for _, edge := range edges {
				sourceEntity, _ := edge.Metadata["source_entity"].(string)
				targetEntity, _ := edge.Metadata["target_entity"].(string)
				if entityNames[sourceEntity] && entityNames[targetEntity] {
					filteredEdges = append(filteredEdges, edge)
				}
			}
			return filteredEdges, nil
		}

		return edges, nil
	}
}

// DefaultEntityExtractorFromGraph creates an entity extractor using REBEL
// This is a convenience function that wraps the graph extractor
func DefaultEntityExtractorFromGraph(embedder EmbedFunc) (EntityExtractFunc, error) {
	graphExtract, err := DefaultGraphExtractor(embedder)
	if err != nil {
		return nil, err
	}
	return GraphExtractorToEntityExtractor(graphExtract), nil
}

// DefaultRelationExtractorFromGraph creates a relation extractor using REBEL
// This is a convenience function that wraps the graph extractor
func DefaultRelationExtractorFromGraph(embedder EmbedFunc) (RelationExtractFunc, error) {
	graphExtract, err := DefaultGraphExtractor(embedder)
	if err != nil {
		return nil, err
	}
	return GraphExtractorToRelationExtractor(graphExtract), nil
}
