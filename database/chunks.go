package database

import (
	"context"
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
	UpdateChunk(chunk *model.Chunk) error
	DeleteChunk(id int) error
	SelectChunk(id int) (*model.Chunk, error)
	SelectChunksByDocument(documentRID uuid.UUID) ([]*model.Chunk, error)
	SelectChunksByPathDescendant(path string) ([]*model.Chunk, error)
	SelectChunksByPathAncestor(path string) ([]*model.Chunk, error)
	SelectChunksBySimilarity(embedding []float32, limit int, threshold float64, documentRIDs []uuid.UUID) ([]*model.Chunk, error)
	SelectChunksBySimilarityWithContext(embedding []float32, limit int, includeAncestors bool, includeDescendants bool, threshold float64, documentRIDs []uuid.UUID) ([]*model.Chunk, error)
}

// ChunksDBHandler handles chunk-related database operations
type ChunksDBHandler struct {
	db *helper.Database
}

// NewChunksDBHandler creates a new chunks database handler.
// It initializes the database connection and loads chunk-related SQL functions.
// If force is true, it will reload the SQL functions even if they already exist.
func NewChunksDBHandler(db *helper.Database, embeddingDim int, force bool) (*ChunksDBHandler, error) {
	if db == nil {
		return nil, helper.NewError("database connection validation", fmt.Errorf("database connection is nil"))
	}

	chunksDbHandler := &ChunksDBHandler{
		db: db,
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
	row := h.db.Instance.QueryRow(
		`SELECT * FROM insert_chunk($1, $2, $3, $4, $5, $6, $7, $8)`,
		chunk.DocumentID,
		chunk.Content,
		chunk.Path,
		pq.Array(chunk.Embedding),
		chunk.StartPos,
		chunk.EndPos,
		chunk.ChunkIndex,
		chunk.Metadata,
	)

	err := row.Scan(
		&chunk.ID,
		&chunk.DocumentID,
		&chunk.DocumentRID,
		&chunk.Content,
		&chunk.Path,
		pq.Array(&chunk.Embedding),
		&chunk.StartPos,
		&chunk.EndPos,
		&chunk.ChunkIndex,
		&chunk.Metadata,
		&chunk.CreatedAt,
	)
	if err != nil {
		return helper.NewError("scan", err)
	}

	return nil
}

// UpdateChunk updates the embedding of a chunk
func (h *ChunksDBHandler) UpdateChunk(chunk *model.Chunk) error {
	embeddingVector := pgvector.NewVector(chunk.Embedding)
	row := h.db.Instance.QueryRow(
		`SELECT * FROM update_chunk($1, $2)`,
		chunk.ID,
		embeddingVector,
	)

	err := row.Scan(
		&chunk.ID,
		&chunk.DocumentID,
		&chunk.DocumentRID,
		&chunk.Content,
		&chunk.Path,
		pq.Array(&chunk.Embedding),
		&chunk.StartPos,
		&chunk.EndPos,
		&chunk.ChunkIndex,
		&chunk.Metadata,
		&chunk.CreatedAt,
	)
	if err != nil {
		return helper.NewError("scan", err)
	}

	return nil
}

// DeleteChunk deletes a chunk by ID
func (h *ChunksDBHandler) DeleteChunk(id int) error {
	_, err := h.db.Instance.Exec(
		`SELECT delete_chunk($1)`,
		id,
	)
	if err != nil {
		return helper.NewError("exec", err)
	}
	return nil
}

// SelectChunk retrieves a chunk by ID
func (h *ChunksDBHandler) SelectChunk(id int) (*model.Chunk, error) {
	row := h.db.Instance.QueryRow(
		`SELECT * FROM select_chunk($1)`,
		id,
	)

	chunk := &model.Chunk{}
	err := row.Scan(
		&chunk.ID,
		&chunk.DocumentID,
		&chunk.DocumentRID,
		&chunk.Content,
		&chunk.Path,
		pq.Array(&chunk.Embedding),
		&chunk.StartPos,
		&chunk.EndPos,
		&chunk.ChunkIndex,
		&chunk.Metadata,
		&chunk.CreatedAt,
	)
	if err != nil {
		return nil, helper.NewError("scan", err)
	}

	return chunk, nil
}

// SelectChunksByDocument retrieves all chunks for a document
func (h *ChunksDBHandler) SelectChunksByDocument(documentRID uuid.UUID) ([]*model.Chunk, error) {
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

		var metadataJSON []byte
		err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.DocumentRID,
			&chunk.Content,
			&chunk.Path,
			pq.Array(&chunk.Embedding),
			&chunk.StartPos,
			&chunk.EndPos,
			&chunk.ChunkIndex,
			&metadataJSON,
			&chunk.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
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

// SelectChunksByEntity retrieves all chunks connected to a given entity
func (h *ChunksDBHandler) SelectChunksByEntity(entityID int) ([]*model.Chunk, error) {
	rows, err := h.db.Instance.Query(
		`SELECT * FROM select_chunks_by_entity($1)`,
		entityID,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var chunks []*model.Chunk
	for rows.Next() {
		chunk := &model.Chunk{}
		err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.DocumentRID,
			&chunk.Content,
			&chunk.Path,
			pq.Array(&chunk.Embedding),
			&chunk.StartPos,
			&chunk.EndPos,
			&chunk.ChunkIndex,
			&chunk.Metadata,
			&chunk.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		chunks = append(chunks, chunk)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return chunks, nil
}

// SelectChunksByPathDescendant retrieves chunks that are descendants of the given path
func (h *ChunksDBHandler) SelectChunksByPathDescendant(path string) ([]*model.Chunk, error) {
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

		err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.DocumentRID,
			&chunk.Content,
			&chunk.Path,
			pq.Array(&chunk.Embedding),
			&chunk.StartPos,
			&chunk.EndPos,
			&chunk.ChunkIndex,
			&chunk.Metadata,
			&chunk.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		chunks = append(chunks, chunk)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return chunks, nil
}

// SelectChunksByPathAncestor retrieves chunks that are ancestors of the given path
func (h *ChunksDBHandler) SelectChunksByPathAncestor(path string) ([]*model.Chunk, error) {
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

		err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.DocumentRID,
			&chunk.Content,
			&chunk.Path,
			pq.Array(&chunk.Embedding),
			&chunk.StartPos,
			&chunk.EndPos,
			&chunk.ChunkIndex,
			&chunk.Metadata,
			&chunk.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
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

		err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.DocumentRID,
			&chunk.Content,
			&chunk.Path,
			pq.Array(&chunk.Embedding),
			&chunk.StartPos,
			&chunk.EndPos,
			&chunk.ChunkIndex,
			&chunk.Metadata,
			&chunk.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
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
		err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.DocumentRID,
			&chunk.Content,
			&chunk.Path,
			pq.Array(&chunk.Embedding),
			&chunk.StartPos,
			&chunk.EndPos,
			&chunk.ChunkIndex,
			&chunk.Metadata,
			&chunk.CreatedAt,
			&chunk.Similarity,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
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
		err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.DocumentRID,
			&chunk.Content,
			&chunk.Path,
			pq.Array(&chunk.Embedding),
			&chunk.StartPos,
			&chunk.EndPos,
			&chunk.ChunkIndex,
			&chunk.Metadata,
			&chunk.CreatedAt,
			&chunk.Similarity,
			&chunk.IsMatch,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		results = append(results, chunk)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return results, nil
}

func (h *ChunksDBHandler) SelectChunksByBFS(sourceID int, maxHops int, edgeTypes []model.EdgeType, followBidirectional bool) ([]*model.Chunk, error) {
	rows, err := h.db.Instance.Query(
		`SELECT * FROM select_chunks_by_bfs($1, $2, $3, $4)`,
		sourceID,
		maxHops,
		pq.Array(edgeTypes),
		followBidirectional,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var results []*model.Chunk
	for rows.Next() {
		chunk := &model.Chunk{}
		err := rows.Scan(
			&chunk.ID,
			&chunk.DocumentID,
			&chunk.DocumentRID,
			&chunk.Content,
			&chunk.Path,
			pq.Array(&chunk.Embedding),
			&chunk.StartPos,
			&chunk.EndPos,
			&chunk.ChunkIndex,
			&chunk.Metadata,
			&chunk.CreatedAt,
			&chunk.Distance,
			pq.Array(&chunk.PathFromSource),
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		results = append(results, chunk)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return results, nil
}
