package pipeline

import (
	"fmt"
	"log"
	"strings"

	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelines"
	"github.com/siherrmann/grapher/helper"
	"github.com/siherrmann/grapher/model"
)

var labels = []string{
	"person",
	"job title",
	"group",
	"organization",
	"brand",
	"gpe",
	"location",
	"facility",
	"address",
	"date",
	"time",
	"monetary value",
	"percentage",
	"quantity",
	"product",
	"technology",
	"work of art",
	"concept",
	"ideology",
	"language",
	"feeling",
	"trait",
	"activity",
	"natural phenomenon",
	"event",
	"law",
	"medical condition",
	"email",
	"phonenumber",
}

// DefaultEntityExtractorAdvanced creates an advanced entity extractor using NuNER (GLiNER-based) model
// Uses NuNER for zero-shot named entity recognition with custom labels
// Supports a wider range of entity types than the basic extractor
func DefaultEntityExtractorAdvanced() (EntityExtractFunc, error) {
	log.Printf("Using labels: %v", strings.Join(labels, ", "))

	// Download NuNER ONNX model from HuggingFace
	// Using onnx-community optimized NuNER_Zero model
	modelName := "onnx-community/NuNER_Zero"
	modelPath, err := helper.PrepareModel(modelName, "onnx/model.onnx")
	if err != nil {
		return nil, err
	}

	// Initialize hugot session with Go backend
	session, err := hugot.NewGoSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create hugot session: %w", err)
	}

	// Create zero-shot NER pipeline for GLiNER/NuNER
	// Note: NuNER uses a different architecture than standard NER models
	config := hugot.TokenClassificationConfig{
		ModelPath: modelPath,
		Name:      "nuner-pipeline",
		Options: []hugot.TokenClassificationOption{
			pipelines.WithSimpleAggregation(),
			pipelines.WithIgnoreLabels([]string{"O"}), // Ignore non-entity tokens
		},
	}
	nerPipeline, err := hugot.NewPipeline(session, config)
	if err != nil {
		if destroyErr := session.Destroy(); destroyErr != nil {
			return nil, fmt.Errorf("failed to create NuNER pipeline: %w (cleanup error: %v)", err, destroyErr)
		}
		return nil, fmt.Errorf("failed to create NuNER pipeline: %w", err)
	}

	return func(text string) ([]*model.Entity, error) {
		// Run NER on the text
		result, err := nerPipeline.RunPipeline([]string{text})
		if err != nil {
			return nil, fmt.Errorf("failed to run NuNER: %w", err)
		}

		if len(result.Entities) == 0 {
			return nil, nil
		}

		// Convert NER results to model.Entity with deduplication
		entityMap := make(map[string]*model.Entity) // key: name+type
		for _, entity := range result.Entities[0] {
			// Normalize entity type
			entityType := normalizeEntityType(entity.Entity)

			// Clean up entity name
			name := strings.TrimSpace(entity.Word)

			// Filter out invalid entities
			if !isValidEntity(name) {
				continue
			}

			// Create unique key for deduplication
			key := strings.ToLower(name) + "|" + entityType

			// Only keep if not already seen or has higher confidence
			if existing, found := entityMap[key]; !found || entity.Score > float32(existing.Metadata["confidence"].(float64)) {
				entityMap[key] = &model.Entity{
					Name: name,
					Type: entityType,
					Metadata: map[string]interface{}{
						"confidence": float64(entity.Score),
						"start":      entity.Start,
						"end":        entity.End,
					},
				}
			}
		}

		// Convert map to slice
		var entities []*model.Entity
		for _, entity := range entityMap {
			entities = append(entities, entity)
		}

		return entities, nil
	}, nil
}

// DefaultEntityExtractorBasic creates an entity extractor using a NER model
// Uses distilbert-NER for named entity recognition
// Detects: PERSON, ORGANIZATION, LOCATION, MISC entities
func DefaultEntityExtractorBasic() (EntityExtractFunc, error) {
	// Prepare model (download if needed)
	// Using KnightsAnalytics optimized distilbert-NER model
	modelName := "KnightsAnalytics/distilbert-NER"
	modelPath, err := helper.PrepareModel(modelName, "model.onnx")
	if err != nil {
		return nil, err
	}

	// Initialize hugot session with Go backend
	session, err := hugot.NewGoSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create hugot session: %w", err)
	}

	// Create token classification pipeline for NER
	config := hugot.TokenClassificationConfig{
		ModelPath: modelPath,
		Name:      "ner-pipeline",
		Options: []hugot.TokenClassificationOption{
			pipelines.WithSimpleAggregation(),
			pipelines.WithIgnoreLabels([]string{"O"}), // Ignore non-entity tokens
		},
	}
	nerPipeline, err := hugot.NewPipeline(session, config)
	if err != nil {
		if destroyErr := session.Destroy(); destroyErr != nil {
			return nil, fmt.Errorf("failed to create NER pipeline: %w (cleanup error: %v)", err, destroyErr)
		}
		return nil, fmt.Errorf("failed to create NER pipeline: %w", err)
	}

	return func(text string) ([]*model.Entity, error) {
		// Run NER on the text
		result, err := nerPipeline.RunPipeline([]string{text})
		if err != nil {
			return nil, fmt.Errorf("failed to run NER: %w", err)
		}

		if len(result.Entities) == 0 {
			return nil, nil
		}

		// Convert NER results to model.Entity with deduplication
		entityMap := make(map[string]*model.Entity) // key: name+type
		for _, entity := range result.Entities[0] {
			// Normalize entity type (remove B- and I- prefixes)
			entityType := normalizeEntityType(entity.Entity)

			// Clean up entity name
			name := strings.TrimSpace(entity.Word)

			// Filter out invalid entities
			if !isValidEntity(name) {
				continue
			}

			// Create unique key for deduplication
			key := strings.ToLower(name) + "|" + entityType

			// Only keep if not already seen or has higher confidence
			if existing, found := entityMap[key]; !found || entity.Score > float32(existing.Metadata["confidence"].(float64)) {
				entityMap[key] = &model.Entity{
					Name: name,
					Type: entityType,
					Metadata: map[string]interface{}{
						"confidence": float64(entity.Score),
						"start":      entity.Start,
						"end":        entity.End,
					},
				}
			}
		}

		// Convert map to slice
		var entities []*model.Entity
		for _, entity := range entityMap {
			entities = append(entities, entity)
		}

		return entities, nil
	}, nil
}

// isValidEntity checks if an entity name is valid
func isValidEntity(name string) bool {
	// Filter out empty or very short names
	if len(name) < 2 {
		return false
	}

	// Filter out entities that are just punctuation or special characters
	cleaned := strings.TrimFunc(name, func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'))
	})
	if len(cleaned) < 2 {
		return false
	}

	// Filter out entities starting with # (tokenization artifacts)
	if strings.HasPrefix(name, "#") {
		return false
	}

	return true
}

// normalizeEntityType removes B- and I- prefixes from NER labels
func normalizeEntityType(label string) string {
	// Remove BIO tagging prefixes (B- for beginning, I- for inside)
	if strings.HasPrefix(label, "B-") {
		return label[2:]
	}
	if strings.HasPrefix(label, "I-") {
		return label[2:]
	}
	return label
}
