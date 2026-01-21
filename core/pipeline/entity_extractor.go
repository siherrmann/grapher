package pipeline

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelines"
	"github.com/siherrmann/grapher/helper"
	"github.com/siherrmann/grapher/model"
)

// DefaultEntityExtractor creates an entity extractor using a NER model
// Uses distilbert-NER for named entity recognition
// Detects: PERSON, ORGANIZATION, LOCATION, MISC entities
func DefaultEntityExtractor() (EntityExtractFunc, error) {
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

		// Convert NER results to model.Entity
		var entities []*model.Entity
		for _, entity := range result.Entities[0] {
			// Normalize entity type (remove B- and I- prefixes)
			entityType := normalizeEntityType(entity.Entity)

			entities = append(entities, &model.Entity{
				ID:   uuid.New(),
				Name: strings.TrimSpace(entity.Word),
				Type: entityType,
				Metadata: map[string]interface{}{
					"confidence": entity.Score,
					"start":      entity.Start,
					"end":        entity.End,
				},
			})
		}

		return entities, nil
	}, nil
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
