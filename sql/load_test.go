package sql

import (
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInit(t *testing.T) {
	db := initDB(t)

	t.Run("Initialize database extensions", func(t *testing.T) {
		err := Init(db.Instance)
		assert.NoError(t, err)

		// Verify pgvector extension is created
		var exists bool
		err = db.Instance.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'vector');").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "pgvector extension should be created")

		// Verify ltree extension is created
		err = db.Instance.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'ltree');").Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "ltree extension should be created")
	})

	t.Run("Initialize database extensions is idempotent", func(t *testing.T) {
		// Running Init multiple times should not error
		err := Init(db.Instance)
		assert.NoError(t, err)

		err = Init(db.Instance)
		assert.NoError(t, err)
	})
}

func TestLoadChunksSql(t *testing.T) {
	db := initDB(t)
	defer db.Close()

	// Initialize extensions first
	err := Init(db.Instance)
	require.NoError(t, err)

	t.Run("Load chunks SQL functions", func(t *testing.T) {
		err := LoadChunksSql(db.Instance, false)
		assert.NoError(t, err)

		// Verify all functions exist
		for _, funcName := range ChunksFunctions {
			var exists bool
			err = db.Instance.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = $1);", funcName).Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists, "Function %s should exist", funcName)
		}
	})

	t.Run("Load chunks SQL is idempotent without force", func(t *testing.T) {
		// Loading again without force should be a no-op
		err := LoadChunksSql(db.Instance, false)
		assert.NoError(t, err)
	})

	t.Run("Load chunks SQL with force reloads", func(t *testing.T) {
		// Loading with force should reload
		err := LoadChunksSql(db.Instance, true)
		assert.NoError(t, err)

		// Verify functions still exist
		for _, funcName := range ChunksFunctions {
			var exists bool
			err = db.Instance.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = $1);", funcName).Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists, "Function %s should exist after force reload", funcName)
		}
	})
}

func TestLoadDocumentsSql(t *testing.T) {
	db := initDB(t)
	defer db.Close()

	// Initialize extensions first
	err := Init(db.Instance)
	require.NoError(t, err)

	t.Run("Load documents SQL functions", func(t *testing.T) {
		err := LoadDocumentsSql(db.Instance, false)
		assert.NoError(t, err)

		// Verify all functions exist
		for _, funcName := range DocumentsFunctions {
			var exists bool
			err = db.Instance.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = $1);", funcName).Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists, "Function %s should exist", funcName)
		}
	})

	t.Run("Load documents SQL is idempotent without force", func(t *testing.T) {
		err := LoadDocumentsSql(db.Instance, false)
		assert.NoError(t, err)
	})

	t.Run("Load documents SQL with force reloads", func(t *testing.T) {
		err := LoadDocumentsSql(db.Instance, true)
		assert.NoError(t, err)
	})
}

func TestLoadEdgesSql(t *testing.T) {
	db := initDB(t)
	defer db.Close()

	// Initialize extensions first
	err := Init(db.Instance)
	require.NoError(t, err)

	t.Run("Load edges SQL functions", func(t *testing.T) {
		err := LoadEdgesSql(db.Instance, false)
		assert.NoError(t, err)

		// Verify all functions exist
		for _, funcName := range EdgesFunctions {
			var exists bool
			err = db.Instance.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = $1);", funcName).Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists, "Function %s should exist", funcName)
		}
	})

	t.Run("Load edges SQL is idempotent without force", func(t *testing.T) {
		err := LoadEdgesSql(db.Instance, false)
		assert.NoError(t, err)
	})

	t.Run("Load edges SQL with force reloads", func(t *testing.T) {
		err := LoadEdgesSql(db.Instance, true)
		assert.NoError(t, err)
	})
}

func TestLoadEntitiesSql(t *testing.T) {
	db := initDB(t)
	defer db.Close()

	// Initialize extensions first
	err := Init(db.Instance)
	require.NoError(t, err)

	t.Run("Load entities SQL functions", func(t *testing.T) {
		err := LoadEntitiesSql(db.Instance, false)
		assert.NoError(t, err)

		// Verify all functions exist
		for _, funcName := range EntitiesFunctions {
			var exists bool
			err = db.Instance.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = $1);", funcName).Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists, "Function %s should exist", funcName)
		}
	})

	t.Run("Load entities SQL is idempotent without force", func(t *testing.T) {
		err := LoadEntitiesSql(db.Instance, false)
		assert.NoError(t, err)
	})

	t.Run("Load entities SQL with force reloads", func(t *testing.T) {
		err := LoadEntitiesSql(db.Instance, true)
		assert.NoError(t, err)
	})
}

func TestLoadAllSql(t *testing.T) {
	db := initDB(t)
	defer db.Close()

	// Initialize extensions first
	err := Init(db.Instance)
	require.NoError(t, err)

	t.Run("Load all SQL functions", func(t *testing.T) {
		err := LoadAllSql(db.Instance, false)
		assert.NoError(t, err)

		// Verify all chunks functions exist
		for _, funcName := range ChunksFunctions {
			var exists bool
			err = db.Instance.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = $1);", funcName).Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists, "Chunks function %s should exist", funcName)
		}

		// Verify all documents functions exist
		for _, funcName := range DocumentsFunctions {
			var exists bool
			err = db.Instance.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = $1);", funcName).Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists, "Documents function %s should exist", funcName)
		}

		// Verify all edges functions exist
		for _, funcName := range EdgesFunctions {
			var exists bool
			err = db.Instance.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = $1);", funcName).Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists, "Edges function %s should exist", funcName)
		}

		// Verify all entities functions exist
		for _, funcName := range EntitiesFunctions {
			var exists bool
			err = db.Instance.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = $1);", funcName).Scan(&exists)
			require.NoError(t, err)
			assert.True(t, exists, "Entities function %s should exist", funcName)
		}
	})

	t.Run("Load all SQL is idempotent without force", func(t *testing.T) {
		err := LoadAllSql(db.Instance, false)
		assert.NoError(t, err)
	})

	t.Run("Load all SQL with force reloads", func(t *testing.T) {
		err := LoadAllSql(db.Instance, true)
		assert.NoError(t, err)
	})
}

func TestCheckFunctions(t *testing.T) {
	db := initDB(t)
	defer db.Close()

	// Initialize extensions first
	err := Init(db.Instance)
	require.NoError(t, err)

	t.Run("Check functions returns false when functions don't exist", func(t *testing.T) {
		exists, err := checkFunctions(db.Instance, []string{"nonexistent_function"})
		assert.NoError(t, err)
		assert.False(t, exists, "Should return false for nonexistent function")
	})

	t.Run("Check functions returns true when all functions exist", func(t *testing.T) {
		// Load chunks SQL first
		err := LoadChunksSql(db.Instance, false)
		require.NoError(t, err)

		exists, err := checkFunctions(db.Instance, ChunksFunctions)
		assert.NoError(t, err)
		assert.True(t, exists, "Should return true when all functions exist")
	})

	t.Run("Check functions returns false when some functions don't exist", func(t *testing.T) {
		// Mix of existing and non-existing functions
		mixedFunctions := append([]string{"init_chunks"}, "nonexistent_function")
		exists, err := checkFunctions(db.Instance, mixedFunctions)
		assert.NoError(t, err)
		assert.False(t, exists, "Should return false when some functions don't exist")
	})

	t.Run("Check functions with empty list", func(t *testing.T) {
		exists, err := checkFunctions(db.Instance, []string{})
		assert.NoError(t, err)
		// With an empty list, the loop doesn't execute and allExist remains false
		// This is actually the correct behavior from the implementation
		assert.False(t, exists, "Should return false for empty function list")
	})
}

func TestFunctionLists(t *testing.T) {
	t.Run("ChunksFunctions list is not empty", func(t *testing.T) {
		assert.NotEmpty(t, ChunksFunctions, "ChunksFunctions should not be empty")
		assert.Greater(t, len(ChunksFunctions), 5, "Should have multiple chunk functions")
	})

	t.Run("DocumentsFunctions list is not empty", func(t *testing.T) {
		assert.NotEmpty(t, DocumentsFunctions, "DocumentsFunctions should not be empty")
		assert.Greater(t, len(DocumentsFunctions), 5, "Should have multiple document functions")
	})

	t.Run("EdgesFunctions list is not empty", func(t *testing.T) {
		assert.NotEmpty(t, EdgesFunctions, "EdgesFunctions should not be empty")
		assert.Greater(t, len(EdgesFunctions), 5, "Should have multiple edge functions")
	})

	t.Run("EntitiesFunctions list is not empty", func(t *testing.T) {
		assert.NotEmpty(t, EntitiesFunctions, "EntitiesFunctions should not be empty")
		assert.Greater(t, len(EntitiesFunctions), 5, "Should have multiple entity functions")
	})
}

func TestEmbeddedSQL(t *testing.T) {
	t.Run("Init SQL is embedded", func(t *testing.T) {
		assert.NotEmpty(t, initSQL, "initSQL should be embedded")
		assert.Contains(t, initSQL, "CREATE EXTENSION", "Should contain CREATE EXTENSION")
	})

	t.Run("Chunks SQL is embedded", func(t *testing.T) {
		assert.NotEmpty(t, chunksSQL, "chunksSQL should be embedded")
		assert.Contains(t, chunksSQL, "CREATE", "Should contain CREATE statements")
	})

	t.Run("Documents SQL is embedded", func(t *testing.T) {
		assert.NotEmpty(t, documentsSQL, "documentsSQL should be embedded")
		assert.Contains(t, documentsSQL, "CREATE", "Should contain CREATE statements")
	})

	t.Run("Edges SQL is embedded", func(t *testing.T) {
		assert.NotEmpty(t, edgesSQL, "edgesSQL should be embedded")
		assert.Contains(t, edgesSQL, "CREATE", "Should contain CREATE statements")
	})

	t.Run("Entities SQL is embedded", func(t *testing.T) {
		assert.NotEmpty(t, entitiesSQL, "entitiesSQL should be embedded")
		assert.Contains(t, entitiesSQL, "CREATE", "Should contain CREATE statements")
	})
}
