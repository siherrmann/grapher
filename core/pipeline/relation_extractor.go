package pipeline

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/knights-analytics/hugot"
	"github.com/knights-analytics/hugot/pipelines"
	"github.com/siherrmann/grapher/helper"
	"github.com/siherrmann/grapher/model"
)

// DefaultRelationExtractor creates a relation extractor using NER models
// Uses token classification to detect citation-related entities and references
// Detects: Citations, references, and relationships between entities
func DefaultRelationExtractor() (RelationExtractFunc, error) {
	// Prepare citation detection model (using NER to detect citation entities)
	modelName := "KnightsAnalytics/distilbert-NER"
	modelPath, err := helper.PrepareModel(modelName)
	if err != nil {
		return nil, err
	}

	// Initialize hugot session with Go backend
	session, err := hugot.NewGoSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create hugot session: %w", err)
	}

	// Create token classification pipeline for citation detection
	config := hugot.TokenClassificationConfig{
		ModelPath: modelPath,
		Name:      "citation-pipeline",
		Options: []hugot.TokenClassificationOption{
			pipelines.WithSimpleAggregation(),
			pipelines.WithIgnoreLabels([]string{"O"}),
		},
	}
	citationPipeline, err := hugot.NewPipeline(session, config)
	if err != nil {
		if destroyErr := session.Destroy(); destroyErr != nil {
			return nil, fmt.Errorf("failed to create citation pipeline: %w (cleanup error: %v)", err, destroyErr)
		}
		return nil, fmt.Errorf("failed to create citation pipeline: %w", err)
	}

	// Fallback patterns for specific citation formats
	citationPatterns := []*regexp.Regexp{
		regexp.MustCompile(`\[(\d+)\]`),                                              // [1], [2]
		regexp.MustCompile(`\(([A-Z][a-z]+(?:\s+et\s+al\.)?)\s+(\d{4})\)`),           // (Smith 2020)
		regexp.MustCompile(`(?i)\b(?:section|chapter)\s+(\d+(?:\.\d+)*)\b`),          // section 3.2, chapter 5
		regexp.MustCompile(`\b(?:Section|Chapter|Figure|Table)\s+(\d+(?:\.\d+)*)\b`), // Section 3, Figure 2.1
		regexp.MustCompile(`doi:\s*(\S+)`),                                           // DOI
		regexp.MustCompile(`https?://\S+`),                                           // URLs
	}

	return func(text string, chunkPath string, entities []*model.Entity) ([]*model.Edge, error) {
		var edges []*model.Edge

		// Use NER model to detect citation-related entities
		// The model can detect MISC (miscellaneous) entities which often include citations
		result, err := citationPipeline.RunPipeline([]string{text})
		if err == nil && len(result.Entities) > 0 {
			for _, entity := range result.Entities[0] {
				// Look for entities that might be citations
				// MISC entities from NER often capture numbers, dates, and references
				entityType := strings.TrimPrefix(strings.TrimPrefix(entity.Entity, "B-"), "I-")

				// Create reference edges for detected citation-like entities
				if entityType == "MISC" || entityType == "PER" {
					edge := &model.Edge{
						EdgeType:      model.EdgeTypeReference,
						Weight:        float64(entity.Score),
						Bidirectional: false,
						Metadata: map[string]interface{}{
							"citation_text":  entity.Word,
							"detection_type": "ner_model",
							"entity_type":    entityType,
							"confidence":     entity.Score,
							"extracted_from": chunkPath,
							"start":          entity.Start,
							"end":            entity.End,
						},
					}
					edges = append(edges, edge)
				}
			}
		}

		// Use pattern matching as supplementary detection for structured citations
		for _, pattern := range citationPatterns {
			matches := pattern.FindAllStringSubmatch(text, -1)
			for _, match := range matches {
				edge := &model.Edge{
					EdgeType:      model.EdgeTypeReference,
					Weight:        0.7, // Slightly lower weight for pattern-based
					Bidirectional: false,
					Metadata: map[string]interface{}{
						"citation_text":    match[0],
						"detection_type":   "pattern_supplement",
						"citation_pattern": getCitationPatternType(pattern),
						"extracted_from":   chunkPath,
					},
				}

				if len(match) > 1 {
					edge.Metadata["reference_id"] = match[1]
					if len(match) > 2 {
						edge.Metadata["reference_year"] = match[2]
					}
				}

				edges = append(edges, edge)
			}
		}

		// Detect co-occurrence relationships between entities
		if len(entities) > 1 {
			for i := 0; i < len(entities); i++ {
				for j := i + 1; j < len(entities); j++ {
					entity1 := entities[i]
					entity2 := entities[j]

					// Get positions from metadata
					start1, ok1 := entity1.Metadata["start"].(uint)
					start2, ok2 := entity2.Metadata["start"].(uint)

					if ok1 && ok2 {
						distance := int(start2) - int(start1)
						if distance < 0 {
							distance = -distance
						}

						// If entities are within 100 characters, create an entity mention edge
						if distance < 100 {
							edge := &model.Edge{
								SourceEntityID: &entity1.ID,
								TargetEntityID: &entity2.ID,
								EdgeType:       model.EdgeTypeEntityMention,
								Weight:         calculateCoOccurrenceWeight(distance),
								Bidirectional:  true,
								Metadata: map[string]interface{}{
									"distance":     distance,
									"context":      chunkPath,
									"entity1_type": entity1.Type,
									"entity2_type": entity2.Type,
									"entity1_name": entity1.Name,
									"entity2_name": entity2.Name,
								},
							}
							edges = append(edges, edge)
						}
					}
				}
			}
		}

		return edges, nil
	}, nil
}

// getCitationPatternType returns the type of citation pattern
func getCitationPatternType(pattern *regexp.Regexp) string {
	patternStr := pattern.String()
	switch {
	case strings.Contains(patternStr, `\[(\d+)\]`):
		return "numeric_citation"
	case strings.Contains(patternStr, "et\\s+al"):
		return "author_year_citation"
	case strings.Contains(patternStr, "section|chapter") && strings.Contains(patternStr, "(?i)"):
		return "section_reference"
	case strings.Contains(patternStr, "Section|Chapter|Figure|Table"):
		return "inline_section_reference"
	case strings.Contains(patternStr, "doi"):
		return "doi_reference"
	case strings.Contains(patternStr, "https?"):
		return "url_reference"
	default:
		return "other"
	}
}

// calculateCoOccurrenceWeight calculates edge weight based on entity proximity
// Closer entities get higher weights (stronger relationship)
func calculateCoOccurrenceWeight(distance int) float64 {
	// Max weight is 1.0 for adjacent entities, decreasing with distance
	// Formula: 1.0 - (distance / 200)
	// At distance 0: weight = 1.0
	// At distance 100: weight = 0.5
	// At distance 200+: weight = 0.0
	weight := 1.0 - (float64(distance) / 200.0)
	if weight < 0 {
		return 0.0
	}
	return weight
}
