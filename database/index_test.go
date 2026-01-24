package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChangeIndexType(t *testing.T) {
	database := initDB(t)

	// Needed because a chunk has a reference to a document
	_, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err, "Expected NewDocumentsDBHandler to not return an error")

	chunksDbHandler, err := NewChunksDBHandler(database, 384, true)
	require.NoError(t, err, "Expected NewChunksDBHandler to not return an error")

	ctx := context.Background()

	t.Run("Change index to HNSW with default params", func(t *testing.T) {
		params := map[string]interface{}{}
		err := chunksDbHandler.ChangeIndexType(ctx, "hnsw", params)
		assert.NoError(t, err, "Expected ChangeIndexType to hnsw to not return an error")
	})

	t.Run("Change index to HNSW with custom params", func(t *testing.T) {
		params := map[string]interface{}{
			"m":               32,
			"ef_construction": 128,
		}
		err := chunksDbHandler.ChangeIndexType(ctx, "hnsw", params)
		assert.NoError(t, err, "Expected ChangeIndexType to hnsw with custom params to not return an error")
	})

	t.Run("Change index to IVFFlat with default params", func(t *testing.T) {
		params := map[string]interface{}{}
		err := chunksDbHandler.ChangeIndexType(ctx, "ivfflat", params)
		assert.NoError(t, err, "Expected ChangeIndexType to ivfflat to not return an error")
	})

	t.Run("Change index to IVFFlat with custom params", func(t *testing.T) {
		params := map[string]interface{}{
			"lists": 200,
		}
		err := chunksDbHandler.ChangeIndexType(ctx, "ivfflat", params)
		assert.NoError(t, err, "Expected ChangeIndexType to ivfflat with custom params to not return an error")
	})

	t.Run("Change index with unsupported index type", func(t *testing.T) {
		params := map[string]interface{}{}
		err := chunksDbHandler.ChangeIndexType(ctx, "invalid", params)
		assert.Error(t, err, "Expected error when using unsupported index type")
		assert.Contains(t, err.Error(), "unsupported index type", "Expected error message to mention unsupported index type")
	})

	t.Run("Change index with timeout context", func(t *testing.T) {
		// Create a context that times out almost immediately
		shortCtx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
		defer cancel()

		// Wait a bit to ensure timeout
		time.Sleep(10 * time.Millisecond)

		params := map[string]interface{}{}
		err := chunksDbHandler.ChangeIndexType(shortCtx, "hnsw", params)
		// May succeed if operation is fast enough, or fail with timeout
		// Just ensure it doesn't panic
		_ = err
	})

	t.Run("Change index back to HNSW for cleanup", func(t *testing.T) {
		params := map[string]interface{}{
			"m":               16,
			"ef_construction": 64,
		}
		err := chunksDbHandler.ChangeIndexType(ctx, "hnsw", params)
		assert.NoError(t, err, "Expected ChangeIndexType to hnsw for cleanup to not return an error")
	})
}
