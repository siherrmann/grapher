package database

import (
	"context"
	"fmt"
	"time"

	"github.com/siherrmann/grapher/helper"
)

// ChangeIndexType changes the vector index type between HNSW and IVFFlat
// indexType: "hnsw" or "ivfflat"
// params: optional parameters for index creation
//   - For HNSW: "m" (int, default 16), "ef_construction" (int, default 64)
//   - For IVFFlat: "lists" (int, default 100)
func (h *ChunksDBHandler) ChangeIndexType(ctx context.Context, indexType string, params map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	// Drop existing index
	_, err := h.db.Instance.ExecContext(ctx, `DROP INDEX IF EXISTS idx_chunks_embedding;`)
	if err != nil {
		return helper.NewError("drop index", err)
	}

	h.db.Logger.Info("Dropped existing vector index")

	// Create new index based on type
	var createIndexSQL string

	switch indexType {
	case "hnsw":
		m := 16
		efConstruction := 64

		if mVal, ok := params["m"].(int); ok {
			m = mVal
		}
		if efVal, ok := params["ef_construction"].(int); ok {
			efConstruction = efVal
		}

		createIndexSQL = fmt.Sprintf(
			`CREATE INDEX idx_chunks_embedding ON chunks USING hnsw (embedding vector_cosine_ops) WITH (m = %d, ef_construction = %d);`,
			m, efConstruction,
		)

	case "ivfflat":
		lists := 100
		if listsVal, ok := params["lists"].(int); ok {
			lists = listsVal
		}

		createIndexSQL = fmt.Sprintf(
			`CREATE INDEX idx_chunks_embedding ON chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = %d);`,
			lists,
		)

	default:
		return helper.NewError("change index type", fmt.Errorf("unsupported index type: %s (use 'hnsw' or 'ivfflat')", indexType))
	}

	// Create the new index
	_, err = h.db.Instance.ExecContext(ctx, createIndexSQL)
	if err != nil {
		return helper.NewError("create index", err)
	}

	h.db.Logger.Info(fmt.Sprintf("Created %s index with params: %v", indexType, params))

	return nil
}
