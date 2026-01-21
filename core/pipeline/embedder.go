package pipeline

import (
	"fmt"

	"github.com/knights-analytics/hugot"
	"github.com/siherrmann/grapher/helper"
)

// DefaultEmbedder creates an embedder using a real sentence transformer model
// Uses the all-MiniLM-L6-v2 model which produces 384-dimensional embeddings
func DefaultEmbedder() (EmbedFunc, error) {
	// Prepare model (download if needed)
	modelName := "sentence-transformers/all-MiniLM-L6-v2"
	modelPath, err := helper.PrepareModel(modelName)
	if err != nil {
		return nil, err
	}

	// Initialize hugot session with Go backend
	session, err := hugot.NewGoSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create hugot session: %w", err)
	}

	// Create sentence transformers pipeline configuration
	config := hugot.FeatureExtractionConfig{
		ModelPath: modelPath,
		Name:      "embedder-pipeline",
	}
	sentencePipeline, err := hugot.NewPipeline(session, config)
	if err != nil {
		if destroyErr := session.Destroy(); destroyErr != nil {
			return nil, fmt.Errorf("failed to create sentence pipeline: %w (cleanup error: %v)", err, destroyErr)
		}
		return nil, fmt.Errorf("failed to create sentence pipeline: %w", err)
	}

	return func(text string) ([]float32, error) {
		// Generate embedding for the text
		result, err := sentencePipeline.RunPipeline([]string{text})
		if err != nil {
			return nil, fmt.Errorf("failed to generate embedding: %w", err)
		}

		if len(result.Embeddings) == 0 {
			return nil, fmt.Errorf("no embedding generated")
		}

		// Extract the first (and only) embedding
		embedding := result.Embeddings[0]
		return embedding, nil
	}, nil
}
