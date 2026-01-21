package pipeline

import (
	"errors"
	"testing"

	"github.com/siherrmann/grapher/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock ChunkFunc for testing
func mockChunkFunc(text string, basePath string) ([]ChunkWithPath, error) {
	if text == "" {
		return nil, errors.New("empty text")
	}

	chunks := []ChunkWithPath{
		{
			Content:  "Chunk 1",
			Path:     basePath + ".chunk1",
			StartPos: intPtr(0),
			EndPos:   intPtr(7),
			Metadata: model.Metadata{"index": 0},
		},
		{
			Content:  "Chunk 2",
			Path:     basePath + ".chunk2",
			StartPos: intPtr(8),
			EndPos:   intPtr(15),
			Metadata: model.Metadata{"index": 1},
		},
	}
	return chunks, nil
}

// Mock EmbedFunc for testing
func mockEmbedFunc(text string) ([]float32, error) {
	if text == "" {
		return nil, errors.New("empty text")
	}

	// Return a simple embedding
	return []float32{0.1, 0.2, 0.3, 0.4}, nil
}

// Mock EmbedFunc that returns an error
func mockEmbedFuncError(text string) ([]float32, error) {
	return nil, errors.New("embedding error")
}

// Helper function
func intPtr(i int) *int {
	return &i
}

func TestNewPipeline(t *testing.T) {
	t.Run("Create new pipeline", func(t *testing.T) {
		pipeline := NewPipeline(mockChunkFunc, mockEmbedFunc)

		require.NotNil(t, pipeline, "Expected NewPipeline to return a non-nil instance")
		assert.NotNil(t, pipeline.Chunker, "Expected pipeline to have a chunker function")
		assert.NotNil(t, pipeline.Embedder, "Expected pipeline to have an embedder function")
	})

	t.Run("Create pipeline with nil functions", func(t *testing.T) {
		pipeline := NewPipeline(nil, nil)

		require.NotNil(t, pipeline, "Expected NewPipeline to return a non-nil instance")
		assert.Nil(t, pipeline.Chunker, "Expected chunker to be nil")
		assert.Nil(t, pipeline.Embedder, "Expected embedder to be nil")
	})
}

func TestPipelineProcess(t *testing.T) {
	t.Run("Process text successfully", func(t *testing.T) {
		pipeline := NewPipeline(mockChunkFunc, mockEmbedFunc)

		chunks, err := pipeline.Process("Test text", "doc")

		assert.NoError(t, err, "Expected Process to not return an error")
		require.Len(t, chunks, 2, "Expected 2 chunks")

		// Verify first chunk
		assert.Equal(t, "Chunk 1", chunks[0].Content, "Expected correct content")
		assert.Equal(t, "doc.chunk1", chunks[0].Path, "Expected correct path")
		assert.NotNil(t, chunks[0].Embedding, "Expected embedding to be set")
		assert.Len(t, chunks[0].Embedding, 4, "Expected embedding to have 4 dimensions")
		assert.Equal(t, intPtr(0), chunks[0].StartPos, "Expected correct start position")
		assert.Equal(t, intPtr(7), chunks[0].EndPos, "Expected correct end position")
		assert.NotNil(t, chunks[0].Metadata, "Expected metadata to be set")

		// Verify second chunk
		assert.Equal(t, "Chunk 2", chunks[1].Content, "Expected correct content")
		assert.Equal(t, "doc.chunk2", chunks[1].Path, "Expected correct path")
		assert.NotNil(t, chunks[1].Embedding, "Expected embedding to be set")
	})

	t.Run("Process with empty text", func(t *testing.T) {
		pipeline := NewPipeline(mockChunkFunc, mockEmbedFunc)

		chunks, err := pipeline.Process("", "doc")

		assert.Error(t, err, "Expected Process to return an error for empty text")
		assert.Nil(t, chunks, "Expected chunks to be nil on error")
		assert.Contains(t, err.Error(), "empty text", "Expected specific error message")
	})

	t.Run("Process with embedding error", func(t *testing.T) {
		pipeline := NewPipeline(mockChunkFunc, mockEmbedFuncError)

		chunks, err := pipeline.Process("Test text", "doc")

		assert.Error(t, err, "Expected Process to return an error from embedder")
		assert.Nil(t, chunks, "Expected chunks to be nil on error")
		assert.Contains(t, err.Error(), "embedding error", "Expected embedding error message")
	})

	t.Run("Process with different base path", func(t *testing.T) {
		pipeline := NewPipeline(mockChunkFunc, mockEmbedFunc)

		chunks, err := pipeline.Process("Test text", "document.chapter1")

		assert.NoError(t, err, "Expected Process to not return an error")
		require.Len(t, chunks, 2, "Expected 2 chunks")
		assert.Equal(t, "document.chapter1.chunk1", chunks[0].Path, "Expected correct path with base")
		assert.Equal(t, "document.chapter1.chunk2", chunks[1].Path, "Expected correct path with base")
	})

	t.Run("Process preserves chunk metadata", func(t *testing.T) {
		pipeline := NewPipeline(mockChunkFunc, mockEmbedFunc)

		chunks, err := pipeline.Process("Test text", "doc")

		assert.NoError(t, err, "Expected Process to not return an error")
		require.Len(t, chunks, 2, "Expected 2 chunks")

		// Verify metadata is preserved
		assert.Equal(t, 0, chunks[0].Metadata["index"], "Expected metadata index 0")
		assert.Equal(t, 1, chunks[1].Metadata["index"], "Expected metadata index 1")
	})

	t.Run("Process with custom chunker returning different count", func(t *testing.T) {
		customChunker := func(text string, basePath string) ([]ChunkWithPath, error) {
			return []ChunkWithPath{
				{Content: "Single chunk", Path: basePath + ".single", Metadata: model.Metadata{}},
			}, nil
		}

		pipeline := NewPipeline(customChunker, mockEmbedFunc)

		chunks, err := pipeline.Process("Test text", "doc")

		assert.NoError(t, err, "Expected Process to not return an error")
		require.Len(t, chunks, 1, "Expected 1 chunk from custom chunker")
		assert.Equal(t, "Single chunk", chunks[0].Content, "Expected correct content")
	})

	t.Run("Process with custom embedder returning different dimensions", func(t *testing.T) {
		customEmbedder := func(text string) ([]float32, error) {
			return []float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8}, nil
		}

		pipeline := NewPipeline(mockChunkFunc, customEmbedder)

		chunks, err := pipeline.Process("Test text", "doc")

		assert.NoError(t, err, "Expected Process to not return an error")
		require.Len(t, chunks, 2, "Expected 2 chunks")
		assert.Len(t, chunks[0].Embedding, 8, "Expected embedding to have 8 dimensions")
		assert.Len(t, chunks[1].Embedding, 8, "Expected embedding to have 8 dimensions")
	})
}

func TestChunkWithPath(t *testing.T) {
	t.Run("Create ChunkWithPath with all fields", func(t *testing.T) {
		startPos := 10
		endPos := 20
		chunkIndex := 5

		cwp := ChunkWithPath{
			Content:    "Test content",
			Path:       "doc.chapter1.section2",
			StartPos:   &startPos,
			EndPos:     &endPos,
			ChunkIndex: &chunkIndex,
			Metadata:   model.Metadata{"type": "paragraph"},
		}

		assert.Equal(t, "Test content", cwp.Content, "Expected correct content")
		assert.Equal(t, "doc.chapter1.section2", cwp.Path, "Expected correct path")
		assert.Equal(t, 10, *cwp.StartPos, "Expected correct start position")
		assert.Equal(t, 20, *cwp.EndPos, "Expected correct end position")
		assert.Equal(t, 5, *cwp.ChunkIndex, "Expected correct chunk index")
		assert.Equal(t, "paragraph", cwp.Metadata["type"], "Expected correct metadata")
	})

	t.Run("Create ChunkWithPath with minimal fields", func(t *testing.T) {
		cwp := ChunkWithPath{
			Content: "Minimal chunk",
			Path:    "doc.chunk",
		}

		assert.Equal(t, "Minimal chunk", cwp.Content, "Expected correct content")
		assert.Equal(t, "doc.chunk", cwp.Path, "Expected correct path")
		assert.Nil(t, cwp.StartPos, "Expected nil start position")
		assert.Nil(t, cwp.EndPos, "Expected nil end position")
		assert.Nil(t, cwp.ChunkIndex, "Expected nil chunk index")
		assert.Nil(t, cwp.Metadata, "Expected nil metadata")
	})
}
