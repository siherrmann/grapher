package model

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultQueryConfig(t *testing.T) {
	t.Run("Returns correct default values", func(t *testing.T) {
		config := DefaultQueryConfig()

		assert.Equal(t, 5, config.TopK, "Default TopK should be 5")
		assert.Equal(t, 0.7, config.SimilarityThreshold, "Default SimilarityThreshold should be 0.7")
		assert.Equal(t, 2, config.MaxHops, "Default MaxHops should be 2")
		assert.Nil(t, config.EdgeTypes, "Default EdgeTypes should be nil (all types)")
		assert.True(t, config.FollowBidirectional, "Default FollowBidirectional should be true")
		assert.False(t, config.IncludeAncestors, "Default IncludeAncestors should be false")
		assert.False(t, config.IncludeDescendants, "Default IncludeDescendants should be false")
		assert.True(t, config.IncludeSiblings, "Default IncludeSiblings should be true")
		assert.Equal(t, 0.6, config.VectorWeight, "Default VectorWeight should be 0.6")
		assert.Equal(t, 0.3, config.GraphWeight, "Default GraphWeight should be 0.3")
		assert.Equal(t, 0.1, config.HierarchyWeight, "Default HierarchyWeight should be 0.1")
	})

	t.Run("Default weights sum to 1.0", func(t *testing.T) {
		config := DefaultQueryConfig()

		sum := config.VectorWeight + config.GraphWeight + config.HierarchyWeight
		assert.InDelta(t, 1.0, sum, 0.001, "Default weights should sum to 1.0")
	})

	t.Run("Can be modified after creation", func(t *testing.T) {
		config := DefaultQueryConfig()

		config.TopK = 10
		config.SimilarityThreshold = 0.8
		config.MaxHops = 3
		config.VectorWeight = 0.5

		assert.Equal(t, 10, config.TopK)
		assert.Equal(t, 0.8, config.SimilarityThreshold)
		assert.Equal(t, 3, config.MaxHops)
		assert.Equal(t, 0.5, config.VectorWeight)
	})

	t.Run("Can set DocumentRIDs", func(t *testing.T) {
		config := DefaultQueryConfig()

		doc1 := uuid.New()
		doc2 := uuid.New()
		config.DocumentRIDs = []uuid.UUID{doc1, doc2}

		require.Len(t, config.DocumentRIDs, 2)
		assert.Equal(t, doc1, config.DocumentRIDs[0])
		assert.Equal(t, doc2, config.DocumentRIDs[1])
	})

	t.Run("Can set EdgeTypes filter", func(t *testing.T) {
		config := DefaultQueryConfig()

		config.EdgeTypes = []EdgeType{EdgeTypeSemantic, EdgeTypeReference}

		require.Len(t, config.EdgeTypes, 2)
		assert.Equal(t, EdgeTypeSemantic, config.EdgeTypes[0])
		assert.Equal(t, EdgeTypeReference, config.EdgeTypes[1])
	})
}
