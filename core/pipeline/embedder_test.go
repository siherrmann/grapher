package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultEmbedder(t *testing.T) {
	// Note: DefaultEmbedder uses hugot which requires downloading models
	// These tests may take longer on first run

	t.Run("Create embedder successfully", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping DefaultEmbedder test in short mode (requires model download)")
		}

		embedder, err := DefaultEmbedder()

		require.NoError(t, err)
		assert.NotNil(t, embedder)
	})

	t.Run("Generate embedding for text", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping DefaultEmbedder test in short mode (requires model download)")
		}

		embedder, err := DefaultEmbedder()
		require.NoError(t, err)

		text := "This is a test sentence."
		embedding, err := embedder(text)

		require.NoError(t, err)
		assert.NotNil(t, embedding)
		assert.Equal(t, 384, len(embedding), "all-MiniLM-L6-v2 produces 384-dimensional embeddings")

		// Verify embedding contains non-zero values
		hasNonZero := false
		for _, val := range embedding {
			if val != 0 {
				hasNonZero = true
				break
			}
		}
		assert.True(t, hasNonZero, "Embedding should contain non-zero values")
	})

	t.Run("Same text produces same embedding", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping DefaultEmbedder test in short mode (requires model download)")
		}

		embedder, err := DefaultEmbedder()
		require.NoError(t, err)

		text := "Deterministic embedding test"
		embedding1, err1 := embedder(text)
		require.NoError(t, err1)

		embedding2, err2 := embedder(text)
		require.NoError(t, err2)

		assert.Equal(t, len(embedding1), len(embedding2))

		// Check that embeddings are identical (or very close due to floating point)
		for i := range embedding1 {
			assert.InDelta(t, embedding1[i], embedding2[i], 0.0001, "Same text should produce same embedding")
		}
	})

	t.Run("Different texts produce different embeddings", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping DefaultEmbedder test in short mode (requires model download)")
		}

		embedder, err := DefaultEmbedder()
		require.NoError(t, err)

		text1 := "Machine learning is fascinating"
		text2 := "The weather is nice today"

		embedding1, err1 := embedder(text1)
		require.NoError(t, err1)

		embedding2, err2 := embedder(text2)
		require.NoError(t, err2)

		assert.Equal(t, len(embedding1), len(embedding2))

		// Embeddings should be different
		isDifferent := false
		for i := range embedding1 {
			if embedding1[i] != embedding2[i] {
				isDifferent = true
				break
			}
		}
		assert.True(t, isDifferent, "Different texts should produce different embeddings")
	})

	t.Run("Similar texts have similar embeddings", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping DefaultEmbedder test in short mode (requires model download)")
		}

		embedder, err := DefaultEmbedder()
		require.NoError(t, err)

		text1 := "The dog is happy"
		text2 := "The puppy is joyful"
		text3 := "Quantum physics is complex"

		embedding1, err1 := embedder(text1)
		require.NoError(t, err1)

		embedding2, err2 := embedder(text2)
		require.NoError(t, err2)

		embedding3, err3 := embedder(text3)
		require.NoError(t, err3)

		// Calculate cosine similarity
		similarity12 := cosineSimilarity(embedding1, embedding2)
		similarity13 := cosineSimilarity(embedding1, embedding3)

		// Dog-puppy should be more similar than dog-physics
		assert.Greater(t, similarity12, similarity13,
			"Semantically similar texts should have higher similarity")
		assert.Greater(t, similarity12, float32(0.5),
			"Related texts should have reasonable similarity")
	})

	t.Run("Handle empty string", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping DefaultEmbedder test in short mode (requires model download)")
		}

		embedder, err := DefaultEmbedder()
		require.NoError(t, err)

		embedding, err := embedder("")

		// Should either return an embedding or an error, but not panic
		if err == nil {
			assert.NotNil(t, embedding)
			assert.Equal(t, 384, len(embedding))
		}
	})

	t.Run("Handle very long text", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping DefaultEmbedder test in short mode (requires model download)")
		}

		embedder, err := DefaultEmbedder()
		require.NoError(t, err)

		// Create a very long text
		longText := ""
		for i := 0; i < 100; i++ {
			longText += "This is a sentence that contributes to making the text very long. "
		}

		embedding, err := embedder(longText)

		require.NoError(t, err)
		assert.NotNil(t, embedding)
		assert.Equal(t, 384, len(embedding))
	})

	t.Run("Handle special characters", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping DefaultEmbedder test in short mode (requires model download)")
		}

		embedder, err := DefaultEmbedder()
		require.NoError(t, err)

		text := "Special chars: @#$%^&*()! 你好 🎉"
		embedding, err := embedder(text)

		require.NoError(t, err)
		assert.NotNil(t, embedding)
		assert.Equal(t, 384, len(embedding))
	})

	t.Run("Multiple embedder instances work independently", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping DefaultEmbedder test in short mode (requires model download)")
		}

		embedder1, err1 := DefaultEmbedder()
		require.NoError(t, err1)

		embedder2, err2 := DefaultEmbedder()
		require.NoError(t, err2)

		text := "Testing multiple instances"

		embedding1, err := embedder1(text)
		require.NoError(t, err)

		embedding2, err := embedder2(text)
		require.NoError(t, err)

		// Both should produce the same result for the same text
		assert.Equal(t, len(embedding1), len(embedding2))
		for i := range embedding1 {
			assert.InDelta(t, embedding1[i], embedding2[i], 0.0001)
		}
	})
}

func TestEmbedderDimensions(t *testing.T) {
	t.Run("Verify embedding dimensions", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping DefaultEmbedder test in short mode (requires model download)")
		}

		embedder, err := DefaultEmbedder()
		require.NoError(t, err)

		tests := []string{
			"Short",
			"This is a medium length sentence.",
			"This is a much longer sentence that contains more words and information to test how the embedder handles varying text lengths.",
		}

		for _, text := range tests {
			embedding, err := embedder(text)
			require.NoError(t, err, "Failed for text: %s", text)
			assert.Equal(t, 384, len(embedding),
				"All embeddings should be 384-dimensional regardless of input length. Failed for: %s", text)
		}
	})
}
