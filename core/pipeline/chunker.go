package pipeline

import (
	"fmt"
	"math"
	"strings"

	"github.com/knights-analytics/hugot"
	"github.com/siherrmann/grapher/helper"
)

// SentenceChunker creates a chunker that splits by sentences
func SentenceChunker(maxSentencesPerChunk int) ChunkFunc {
	return func(text string, basePath string) ([]ChunkWithPath, error) {
		if maxSentencesPerChunk <= 0 {
			return nil, fmt.Errorf("max sentences per chunk must be positive")
		}

		// Handle empty or whitespace-only text
		if strings.TrimSpace(text) == "" {
			return []ChunkWithPath{}, nil
		}

		text = strings.ReplaceAll(text, "! ", "!|")
		text = strings.ReplaceAll(text, "? ", "?|")
		text = strings.ReplaceAll(text, ". ", ".|")

		sentences := strings.Split(text, "|")
		var result []string
		for _, s := range sentences {
			s = strings.TrimSpace(s)
			if s != "" {
				result = append(result, s)
			}
		}

		var chunks []ChunkWithPath
		var currentChunk []string
		chunkIdx := 0
		pos := 0

		for _, sentence := range sentences {
			currentChunk = append(currentChunk, sentence)

			if len(currentChunk) >= maxSentencesPerChunk {
				content := strings.Join(currentChunk, " ")
				startPos := pos
				endPos := pos + len(content)

				path := fmt.Sprintf("%s.chunk%d", basePath, chunkIdx)

				chunks = append(chunks, ChunkWithPath{
					Content:    content,
					Path:       path,
					StartPos:   &startPos,
					EndPos:     &endPos,
					ChunkIndex: &chunkIdx,
					Metadata:   make(map[string]interface{}),
				})

				pos = endPos
				currentChunk = nil
				chunkIdx++
			}
		}

		// Add remaining sentences
		if len(currentChunk) > 0 {
			content := strings.Join(currentChunk, " ")
			startPos := pos
			endPos := pos + len(content)

			path := fmt.Sprintf("%s.chunk%d", basePath, chunkIdx)

			chunks = append(chunks, ChunkWithPath{
				Content:    content,
				Path:       path,
				StartPos:   &startPos,
				EndPos:     &endPos,
				ChunkIndex: &chunkIdx,
				Metadata:   make(map[string]interface{}),
			})
		}

		return chunks, nil
	}
}

// ParagraphChunker creates a chunker that splits by paragraphs
func ParagraphChunker() ChunkFunc {
	return func(text string, basePath string) ([]ChunkWithPath, error) {
		paragraphs := strings.Split(text, "\n\n")

		var chunks []ChunkWithPath
		pos := 0

		for i, para := range paragraphs {
			para = strings.TrimSpace(para)
			if para == "" {
				continue
			}

			startPos := pos
			endPos := pos + len(para)

			path := fmt.Sprintf("%s.para%d", basePath, i)

			chunks = append(chunks, ChunkWithPath{
				Content:    para,
				Path:       path,
				StartPos:   &startPos,
				EndPos:     &endPos,
				ChunkIndex: &i,
				Metadata:   make(map[string]interface{}),
			})

			pos = endPos + 2 // Account for "\n\n"
		}

		return chunks, nil
	}
}

// cosineSimilarity calculates the cosine similarity between two embedding vectors
func cosineSimilarity(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}

	var dotProduct, normA, normB float32
	for i := range a {
		dotProduct += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProduct / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
}

// DefaultChunker creates a semantic chunker that uses embeddings to identify natural boundaries
// It analyzes semantic similarity between sentences and creates chunks at points where similarity drops
func DefaultChunker(maxChunkSize int, similarityThreshold float32) ChunkFunc {
	return func(text string, basePath string) ([]ChunkWithPath, error) {
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
		defer session.Destroy()

		// Create sentence transformers pipeline configuration
		config := hugot.FeatureExtractionConfig{
			ModelPath: modelPath,
			Name:      "semantic-chunker-pipeline",
		}
		sentencePipeline, err := hugot.NewPipeline(session, config)
		if err != nil {
			return nil, fmt.Errorf("failed to create sentence pipeline: %w", err)
		}

		// Split text into sentences
		text = strings.ReplaceAll(text, "! ", "!|")
		text = strings.ReplaceAll(text, "? ", "?|")
		text = strings.ReplaceAll(text, ". ", ".|")
		sentences := strings.Split(text, "|")

		var cleanSentences []string
		for _, s := range sentences {
			s = strings.TrimSpace(s)
			if s != "" {
				cleanSentences = append(cleanSentences, s)
			}
		}

		if len(cleanSentences) == 0 {
			return nil, fmt.Errorf("no sentences found in text")
		}

		// Get embeddings for all sentences
		embeddingResult, err := sentencePipeline.RunPipeline(cleanSentences)
		if err != nil {
			return nil, fmt.Errorf("failed to generate embeddings: %w", err)
		}

		embeddings := embeddingResult.Embeddings
		if len(embeddings) != len(cleanSentences) {
			return nil, fmt.Errorf("embedding count mismatch: got %d embeddings for %d sentences", len(embeddings), len(cleanSentences))
		}

		// Group sentences based on semantic similarity
		var chunks []ChunkWithPath
		var currentChunk []string
		var currentEmbeddings [][]float32
		var currentLength int
		chunkIdx := 0
		pos := 0

		for i, sentence := range cleanSentences {
			sentenceLen := len(sentence)
			shouldBreak := false

			// Check if we should create a chunk boundary
			if len(currentChunk) > 0 {
				// Calculate average embedding of current chunk
				avgEmbedding := make([]float32, len(currentEmbeddings[0]))
				for _, emb := range currentEmbeddings {
					for j := range emb {
						avgEmbedding[j] += emb[j]
					}
				}
				for j := range avgEmbedding {
					avgEmbedding[j] /= float32(len(currentEmbeddings))
				}

				// Calculate similarity between current chunk and new sentence
				similarity := cosineSimilarity(avgEmbedding, embeddings[i])

				// Break if similarity drops below threshold or size limit exceeded
				if similarity < similarityThreshold || currentLength+sentenceLen > maxChunkSize {
					shouldBreak = true
				}
			}

			if shouldBreak {
				// Create chunk
				content := strings.Join(currentChunk, " ")
				startPos := pos
				endPos := pos + len(content)

				path := fmt.Sprintf("%s.chunk%d", basePath, chunkIdx)

				chunks = append(chunks, ChunkWithPath{
					Content:    content,
					Path:       path,
					StartPos:   &startPos,
					EndPos:     &endPos,
					ChunkIndex: &chunkIdx,
					Metadata: map[string]interface{}{
						"embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
						"num_sentences":   len(currentChunk),
						"chunking_method": "semantic",
					},
				})

				pos = endPos
				currentChunk = nil
				currentEmbeddings = nil
				currentLength = 0
				chunkIdx++
			}

			currentChunk = append(currentChunk, sentence)
			currentEmbeddings = append(currentEmbeddings, embeddings[i])
			currentLength += sentenceLen

			// For the last sentence, create the final chunk
			if i == len(cleanSentences)-1 && len(currentChunk) > 0 {
				content := strings.Join(currentChunk, " ")
				startPos := pos
				endPos := pos + len(content)

				path := fmt.Sprintf("%s.chunk%d", basePath, chunkIdx)

				chunks = append(chunks, ChunkWithPath{
					Content:    content,
					Path:       path,
					StartPos:   &startPos,
					EndPos:     &endPos,
					ChunkIndex: &chunkIdx,
					Metadata: map[string]interface{}{
						"embedding_model": "sentence-transformers/all-MiniLM-L6-v2",
						"num_sentences":   len(currentChunk),
						"chunking_method": "semantic",
					},
				})
			}
		}

		return chunks, nil
	}
}
