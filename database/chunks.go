package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/pgvector/pgvector-go"
	"github.com/siherrmann/grapher/helper"
	"github.com/siherrmann/grapher/model"
	loadSql "github.com/siherrmann/grapher/sql"
)

// ChunksDBHandlerFunctions defines the interface for Chunks database operations.
type ChunksDBHandlerFunctions interface {
	InsertChunk(chunk *model.Chunk) error
	SelectChunk(id uuid.UUID) (*model.Chunk, error)
	SelectAllChunksByDocument(documentRID uuid.UUID) ([]*model.Chunk, error)
	SelectAllChunksByPathDescendant(path string) ([]*model.Chunk, error)
	SelectAllChunksByPathAncestor(path string) ([]*model.Chunk, error)
	SelectChunksBySimilarity(embedding []float32, limit int, threshold float64, documentRIDs []uuid.UUID) ([]*model.Chunk, error)
	SelectChunksBySimilarityWithContext(embedding []float32, limit int, includeAncestors bool, includeDescendants bool, threshold float64, documentRIDs []uuid.UUID) ([]*model.Chunk, error)
	DeleteChunk(id uuid.UUID) error
	UpdateChunkEmbedding(id uuid.UUID, embedding []float32) error
}

// ChunksDBHandler handles chunk-related database operations
type ChunksDBHandler struct {
	db           *helper.Database
	edgesHandler *EdgesDBHandler // For graph operations
}

// NewChunksDBHandler creates a new chunks database handler.
// It initializes the database connection and loads chunk-related SQL functions.
// If force is true, it will reload the SQL functions even if they already exist.
func NewChunksDBHandler(db *helper.Database, edgesHandler *EdgesDBHandler, embeddingDim int, force bool) (*ChunksDBHandler, error) {
	if db == nil {
		return nil, helper.NewError("database connection validation", fmt.Errorf("database connection is nil"))
	}

	chunksDbHandler := &ChunksDBHandler{
		db:           db,
		edgesHandler: edgesHandler,
	}

	err := loadSql.LoadChunksSql(chunksDbHandler.db.Instance, force)
	if err != nil {
		return nil, helper.NewError("load chunks sql", err)
	}

	err = chunksDbHandler.CreateTable(embeddingDim)
	if err != nil {
		return nil, helper.NewError("create table", err)
	}

	db.Logger.Info("Initialized ChunksDBHandler")

	return chunksDbHandler, nil
}

// CreateTable creates the 'chunks' table in the database.
// If the table already exists, it does not create it again.
// It also creates all necessary extensions, indexes, and triggers.
func (h *ChunksDBHandler) CreateTable(embeddingDim int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use the SQL init() function to create all tables, triggers, and indexes
	_, err := h.db.Instance.ExecContext(ctx, `SELECT init_chunks($1);`, embeddingDim)
	if err != nil {
		log.Panicf("error initializing chunks table: %#v", err)
	}

	h.db.Logger.Info("Checked/created table chunks")

	return nil
}

// InsertChunk inserts a new chunk
func (h *ChunksDBHandler) InsertChunk(chunk *model.Chunk) error {
	var embeddingParam interface{}
	if len(chunk.Embedding) > 0 {
		embeddingVector := pgvector.NewVector(chunk.Embedding)
		embeddingParam = &embeddingVector
	} else {
		embeddingParam = nil
	}

	row := h.db.Instance.QueryRow(
		`SELECT * FROM insert_chunk($1, $2, $3, $4, $5, $6, $7, $8)`,
		chunk.DocumentID,
		chunk.Content,
		chunk.Path,
		embeddingParam,
		chunk.StartPos,
		chunk.EndPos,
		chunk.ChunkIndex,
		chunk.Metadata,
	)

	var embeddingVec *pgvector.Vector
	err := row.Scan(
		&chunk.ID,
		&chunk.DocumentID,
		&chunk.DocumentRID,
		&chunk.Content,
		&chunk.Path,
		&embeddingVec,
		&chunk.StartPos,
		&chunk.EndPos,
		&chunk.ChunkIndex,
		&chunk.Metadata,
		&chunk.CreatedAt,
	)
	if err != nil {
		return helper.NewError("scan", err)
	}

	if embeddingVec != nil {
		chunk.Embedding = embeddingVec.Slice()
	}

	return nil
}

// SelectChunk retrieves a chunk by ID
func (h *ChunksDBHandler) SelectChunk(id uuid.UUID) (*model.Chunk, error) {
	row := h.db.Instance.QueryRow(
		`SELECT * FROM select_chunk($1)`,
		id,
	)

	chunk := &model.Chunk{}
	var embeddingVec *pgvector.Vector
	err := row.Scan(
		&chunk.ID,
		&chunk.DocumentID,
		&chunk.DocumentRID,
		&chunk.Content,
		&chunk.Path,
		&embeddingVec,
		&chunk.StartPos,
		&chunk.EndPos,
		&chunk.ChunkIndex,
		&chunk.Metadata,
		&chunk.CreatedAt,
	)
	if err != nil {
		return nil, helper.NewError("scan", err)
	}

	if embeddingVec != nil {
		chunk.Embedding = embeddingVec.Slice()
	}

	return chunk, nil
}

// SelectAllChunksByDocument retrieves all chunks for a document
func (h *ChunksDBHandler) SelectAllChunksByDocument(documentRID uuid.UUID) ([]*model.Chunk, error) {
	rows, err := h.db.Instance.Query(
		`SELECT * FROM select_chunks_by_document($1)`,
		documentRID,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var chunks []*model.Chunk
	for rows.Next() {
		chunk := &model.Chunk{}
		var embeddingVec *pgvector.Vector
		var metadataJSON []byte
		err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.DocumentRID,
			&chunk.Content,
			&chunk.Path,
			&embeddingVec,
			&chunk.StartPos,
			&chunk.EndPos,
			&chunk.ChunkIndex,
			&metadataJSON,
			&chunk.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		if embeddingVec != nil {
			chunk.Embedding = embeddingVec.Slice()
		}
		if err := json.Unmarshal(metadataJSON, &chunk.Metadata); err != nil {
			return nil, helper.NewError("unmarshaling metadata", err)
		}

		chunks = append(chunks, chunk)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return chunks, nil
}

// SelectAllChunksByPathDescendant retrieves chunks that are descendants of the given path
func (h *ChunksDBHandler) SelectAllChunksByPathDescendant(path string) ([]*model.Chunk, error) {
	rows, err := h.db.Instance.Query(
		`SELECT * FROM select_chunks_by_path_descendant($1)`,
		path,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var chunks []*model.Chunk
	for rows.Next() {
		chunk := &model.Chunk{}
		var embeddingVec *pgvector.Vector
		err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.DocumentRID,
			&chunk.Content,
			&chunk.Path,
			&embeddingVec,
			&chunk.StartPos,
			&chunk.EndPos,
			&chunk.ChunkIndex,
			&chunk.Metadata,
			&chunk.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		if embeddingVec != nil {
			chunk.Embedding = embeddingVec.Slice()
		}

		chunks = append(chunks, chunk)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return chunks, nil
}

// SelectAllChunksByPathAncestor retrieves chunks that are ancestors of the given path
func (h *ChunksDBHandler) SelectAllChunksByPathAncestor(path string) ([]*model.Chunk, error) {
	rows, err := h.db.Instance.Query(
		`SELECT * FROM select_chunks_by_path_ancestor($1)`,
		path,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var chunks []*model.Chunk
	for rows.Next() {
		chunk := &model.Chunk{}
		var embeddingVec *pgvector.Vector
		err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.DocumentRID,
			&chunk.Content,
			&chunk.Path,
			&embeddingVec,
			&chunk.StartPos,
			&chunk.EndPos,
			&chunk.ChunkIndex,
			&chunk.Metadata,
			&chunk.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		if embeddingVec != nil {
			chunk.Embedding = embeddingVec.Slice()
		}

		chunks = append(chunks, chunk)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return chunks, nil
}

// SelectSiblingChunks retrieves chunks that are siblings of the given path (same parent, same level)
func (h *ChunksDBHandler) SelectSiblingChunks(path string) ([]*model.Chunk, error) {
	rows, err := h.db.Instance.Query(
		`SELECT * FROM select_sibling_chunks($1)`,
		path,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var chunks []*model.Chunk
	for rows.Next() {
		chunk := &model.Chunk{}
		var embeddingVec *pgvector.Vector
		err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.DocumentRID,
			&chunk.Content,
			&chunk.Path,
			&embeddingVec,
			&chunk.StartPos,
			&chunk.EndPos,
			&chunk.ChunkIndex,
			&chunk.Metadata,
			&chunk.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		if embeddingVec != nil {
			chunk.Embedding = embeddingVec.Slice()
		}

		chunks = append(chunks, chunk)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return chunks, nil
}

// SelectChunksBySimilarity performs vector similarity search
// If documentRIDs is nil or empty, searches across all documents
func (h *ChunksDBHandler) SelectChunksBySimilarity(embedding []float32, limit int, threshold float64, documentRIDs []uuid.UUID) ([]*model.Chunk, error) {
	embeddingVector := pgvector.NewVector(embedding)

	// Convert documentRIDs to PostgreSQL UUID array format
	var documentRIDsParam interface{}
	if len(documentRIDs) > 0 {
		documentRIDsParam = pq.Array(documentRIDs)
	} else {
		documentRIDsParam = nil
	}

	rows, err := h.db.Instance.Query(
		`SELECT * FROM select_chunks_by_similarity($1, $2, $3, $4)`,
		embeddingVector,
		limit,
		threshold,
		documentRIDsParam,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var results []*model.Chunk
	for rows.Next() {
		chunk := &model.Chunk{}
		var embeddingVec *pgvector.Vector
		var similarity sql.NullFloat64
		err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.DocumentRID,
			&chunk.Content,
			&chunk.Path,
			&embeddingVec,
			&chunk.StartPos,
			&chunk.EndPos,
			&chunk.ChunkIndex,
			&chunk.Metadata,
			&chunk.CreatedAt,
			&similarity,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		if embeddingVec != nil {
			chunk.Embedding = embeddingVec.Slice()
		}

		// Ensure similarity is always set
		if similarity.Valid {
			chunk.Similarity = &similarity.Float64
		} else {
			zero := 0.0
			chunk.Similarity = &zero
		}

		results = append(results, chunk)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return results, nil
}

// SelectChunksBySimilarityWithContext performs vector similarity search with hierarchical context
// If documentRIDs is nil or empty, searches across all documents
func (h *ChunksDBHandler) SelectChunksBySimilarityWithContext(
	embedding []float32,
	limit int,
	includeAncestors bool,
	includeDescendants bool,
	threshold float64,
	documentRIDs []uuid.UUID,
) ([]*model.Chunk, error) {
	embeddingVector := pgvector.NewVector(embedding)

	// Convert documentRIDs to PostgreSQL UUID array format
	var documentRIDsParam interface{}
	if len(documentRIDs) > 0 {
		documentRIDsParam = pq.Array(documentRIDs)
	} else {
		documentRIDsParam = nil
	}

	rows, err := h.db.Instance.Query(
		`SELECT * FROM select_chunks_by_similarity_with_context($1, $2, $3, $4, $5, $6)`,
		embeddingVector,
		limit,
		includeAncestors,
		includeDescendants,
		threshold,
		documentRIDsParam,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var results []*model.Chunk
	for rows.Next() {
		chunk := &model.Chunk{}
		var similarity sql.NullFloat64
		var embeddingVec *pgvector.Vector
		err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.DocumentRID,
			&chunk.Content,
			&chunk.Path,
			&embeddingVec,
			&chunk.StartPos,
			&chunk.EndPos,
			&chunk.ChunkIndex,
			&chunk.Metadata,
			&chunk.CreatedAt,
			&similarity,
			&chunk.IsMatch,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		if embeddingVec != nil {
			chunk.Embedding = embeddingVec.Slice()
		}

		if similarity.Valid {
			chunk.Similarity = &similarity.Float64
		}

		results = append(results, chunk)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return results, nil
}

// DeleteChunk deletes a chunk by ID
func (h *ChunksDBHandler) DeleteChunk(id uuid.UUID) error {
	_, err := h.db.Instance.Exec(
		`SELECT delete_chunk($1)`,
		id,
	)
	if err != nil {
		return helper.NewError("exec", err)
	}
	return nil
}

// UpdateChunkEmbedding updates the embedding of a chunk
func (h *ChunksDBHandler) UpdateChunkEmbedding(id uuid.UUID, embedding []float32) error {
	embeddingVector := pgvector.NewVector(embedding)
	_, err := h.db.Instance.Exec(
		`SELECT * FROM update_chunk_embedding($1, $2)`,
		id,
		embeddingVector,
	)
	if err != nil {
		return helper.NewError("exec", err)
	}
	return nil
}
