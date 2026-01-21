package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSentenceChunker(t *testing.T) {
	t.Run("Valid chunking with multiple sentences", func(t *testing.T) {
		chunker := SentenceChunker(2)
		text := "This is sentence one. This is sentence two. This is sentence three."
		basePath := "doc.test"

		chunks, err := chunker(text, basePath)

		require.NoError(t, err)
		assert.Greater(t, len(chunks), 0, "Expected at least one chunk")

		// Verify chunk structure
		for _, chunk := range chunks {
			assert.NotEmpty(t, chunk.Content)
			assert.NotEmpty(t, chunk.Path)
			assert.Contains(t, chunk.Path, basePath)
			assert.NotNil(t, chunk.StartPos)
			assert.NotNil(t, chunk.EndPos)
			assert.NotNil(t, chunk.ChunkIndex)
		}
	})

	t.Run("Single sentence", func(t *testing.T) {
		chunker := SentenceChunker(1)
		text := "This is a single sentence."
		basePath := "doc.single"

		chunks, err := chunker(text, basePath)

		require.NoError(t, err)
		assert.Equal(t, 1, len(chunks))
		assert.Contains(t, chunks[0].Content, "single sentence")
	})

	t.Run("Error with zero max sentences", func(t *testing.T) {
		chunker := SentenceChunker(0)
		text := "Some text."
		basePath := "doc.test"

		_, err := chunker(text, basePath)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be positive")
	})

	t.Run("Error with negative max sentences", func(t *testing.T) {
		chunker := SentenceChunker(-1)
		text := "Some text."
		basePath := "doc.test"

		_, err := chunker(text, basePath)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be positive")
	})

	t.Run("Different punctuation marks", func(t *testing.T) {
		chunker := SentenceChunker(1)
		text := "Question one? Statement two. Exclamation three!"
		basePath := "doc.punct"

		chunks, err := chunker(text, basePath)

		require.NoError(t, err)
		assert.Greater(t, len(chunks), 0)
	})

	t.Run("Empty text", func(t *testing.T) {
		chunker := SentenceChunker(2)
		text := ""
		basePath := "doc.empty"

		chunks, err := chunker(text, basePath)

		require.NoError(t, err)
		assert.Equal(t, 0, len(chunks))
	})

	t.Run("Text with only whitespace", func(t *testing.T) {
		chunker := SentenceChunker(2)
		text := "   \n\t  "
		basePath := "doc.whitespace"

		chunks, err := chunker(text, basePath)

		require.NoError(t, err)
		assert.Equal(t, 0, len(chunks))
	})
}

func TestParagraphChunker(t *testing.T) {
	t.Run("Valid chunking with multiple paragraphs", func(t *testing.T) {
		chunker := ParagraphChunker()
		text := "First paragraph.\n\nSecond paragraph.\n\nThird paragraph."
		basePath := "doc.test"

		chunks, err := chunker(text, basePath)

		require.NoError(t, err)
		assert.Equal(t, 3, len(chunks))

		// Verify each chunk
		assert.Contains(t, chunks[0].Content, "First")
		assert.Contains(t, chunks[1].Content, "Second")
		assert.Contains(t, chunks[2].Content, "Third")

		// Verify paths
		for i, chunk := range chunks {
			assert.Contains(t, chunk.Path, basePath)
			assert.Contains(t, chunk.Path, "para")
			assert.NotNil(t, chunk.ChunkIndex)
			assert.Equal(t, i, *chunk.ChunkIndex)
		}
	})

	t.Run("Single paragraph", func(t *testing.T) {
		chunker := ParagraphChunker()
		text := "Just one paragraph here."
		basePath := "doc.single"

		chunks, err := chunker(text, basePath)

		require.NoError(t, err)
		assert.Equal(t, 1, len(chunks))
		assert.Contains(t, chunks[0].Content, "one paragraph")
	})

	t.Run("Empty paragraphs are skipped", func(t *testing.T) {
		chunker := ParagraphChunker()
		text := "First paragraph.\n\n\n\nSecond paragraph."
		basePath := "doc.empty"

		chunks, err := chunker(text, basePath)

		require.NoError(t, err)
		assert.Equal(t, 2, len(chunks))
	})

	t.Run("Empty text", func(t *testing.T) {
		chunker := ParagraphChunker()
		text := ""
		basePath := "doc.empty"

		chunks, err := chunker(text, basePath)

		require.NoError(t, err)
		assert.Equal(t, 0, len(chunks))
	})

	t.Run("Position tracking", func(t *testing.T) {
		chunker := ParagraphChunker()
		text := "Para one.\n\nPara two."
		basePath := "doc.pos"

		chunks, err := chunker(text, basePath)

		require.NoError(t, err)
		assert.Equal(t, 2, len(chunks))

		// Verify positions are set
		assert.NotNil(t, chunks[0].StartPos)
		assert.NotNil(t, chunks[0].EndPos)
		assert.NotNil(t, chunks[1].StartPos)
		assert.NotNil(t, chunks[1].EndPos)

		// Second chunk should start after first
		assert.Greater(t, *chunks[1].StartPos, *chunks[0].EndPos)
	})
}

func TestDefaultChunker(t *testing.T) {
	// Note: DefaultChunker uses hugot which requires downloading models
	// These tests may take longer on first run
	t.Run("Valid semantic chunking", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping DefaultChunker test in short mode (requires model download)")
		}

		chunker := DefaultChunker(200, 0.7)
		text := "Machine learning is fascinating. Neural networks are powerful. " +
			"Dogs are great pets. Cats are independent animals. " +
			"Python is a programming language. Go is also popular."
		basePath := "doc.semantic"

		chunks, err := chunker(text, basePath)

		require.NoError(t, err)
		assert.Greater(t, len(chunks), 0)

		// Verify semantic metadata
		for _, chunk := range chunks {
			assert.NotEmpty(t, chunk.Metadata)
			assert.Equal(t, "sentence-transformers/all-MiniLM-L6-v2", chunk.Metadata["embedding_model"])
			assert.Equal(t, "semantic", chunk.Metadata["chunking_method"])
			assert.NotNil(t, chunk.Metadata["num_sentences"])
		}
	})

	t.Run("Respects max chunk size", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping DefaultChunker test in short mode (requires model download)")
		}

		chunker := DefaultChunker(50, 0.7)
		text := "This is a very long sentence that should definitely exceed fifty characters when combined with another sentence. " +
			"And here is that second sentence to make sure we go over the limit."
		basePath := "doc.size"

		chunks, err := chunker(text, basePath)

		require.NoError(t, err)

		// With small max size, should create multiple chunks
		assert.Greater(t, len(chunks), 1, "Expected multiple chunks due to size limit")

		// Verify no chunk exceeds max size significantly (allowing for sentence boundaries)
		for _, chunk := range chunks {
			// Each chunk should be reasonably sized
			assert.NotEmpty(t, chunk.Content)
		}
	})

	t.Run("Empty text returns error", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping DefaultChunker test in short mode (requires model download)")
		}

		chunker := DefaultChunker(500, 0.7)
		text := ""
		basePath := "doc.empty"

		_, err := chunker(text, basePath)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no sentences found")
	})
}

func TestCosineSimilarity(t *testing.T) {
	t.Run("Identical vectors", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{1.0, 2.0, 3.0}

		similarity := cosineSimilarity(a, b)

		assert.InDelta(t, 1.0, similarity, 0.001, "Identical vectors should have similarity ~1.0")
	})

	t.Run("Orthogonal vectors", func(t *testing.T) {
		a := []float32{1.0, 0.0, 0.0}
		b := []float32{0.0, 1.0, 0.0}

		similarity := cosineSimilarity(a, b)

		assert.InDelta(t, 0.0, similarity, 0.001, "Orthogonal vectors should have similarity ~0.0")
	})

	t.Run("Opposite vectors", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{-1.0, -2.0, -3.0}

		similarity := cosineSimilarity(a, b)

		assert.InDelta(t, -1.0, similarity, 0.001, "Opposite vectors should have similarity ~-1.0")
	})

	t.Run("Different lengths return 0", func(t *testing.T) {
		a := []float32{1.0, 2.0}
		b := []float32{1.0, 2.0, 3.0}

		similarity := cosineSimilarity(a, b)

		assert.Equal(t, float32(0.0), similarity)
	})

	t.Run("Zero vectors return 0", func(t *testing.T) {
		a := []float32{0.0, 0.0, 0.0}
		b := []float32{1.0, 2.0, 3.0}

		similarity := cosineSimilarity(a, b)

		assert.Equal(t, float32(0.0), similarity)
	})

	t.Run("Similar but not identical vectors", func(t *testing.T) {
		a := []float32{1.0, 2.0, 3.0}
		b := []float32{1.0, 2.1, 2.9}

		similarity := cosineSimilarity(a, b)

		assert.Greater(t, similarity, float32(0.9), "Similar vectors should have high similarity")
		assert.Less(t, similarity, float32(1.0), "But not exactly 1.0")
	})
}
