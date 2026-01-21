package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/siherrmann/grapher/helper"
	"github.com/siherrmann/grapher/model"
	"github.com/siherrmann/grapher/sql"
)

// EntitiesDBHandlerFunctions defines the interface for Entities database operations.
type EntitiesDBHandlerFunctions interface {
	InsertEntity(entity *model.Entity) error
	SelectEntity(id uuid.UUID) (*model.Entity, error)
	SelectEntityByName(name string, entityType string) (*model.Entity, error)
	SelectEntitiesBySearch(searchTerm string, entityType *string, limit int) ([]*model.Entity, error)
	SelectEntitiesByType(entityType string, limit int) ([]*model.Entity, error)
	DeleteEntity(id uuid.UUID) error
	UpdateEntityMetadata(id uuid.UUID, metadata map[string]interface{}) error
	SelectChunksMentioningEntity(entityID uuid.UUID) ([]*model.ChunkMention, error)
}

// EntitiesDBHandler handles entity-related database operations
type EntitiesDBHandler struct {
	db *helper.Database
}

// NewEntitiesDBHandler creates a new entities database handler.
// It initializes the database connection and loads entity-related SQL functions.
// If force is true, it will reload the SQL functions even if they already exist.
func NewEntitiesDBHandler(db *helper.Database, force bool) (*EntitiesDBHandler, error) {
	if db == nil {
		return nil, helper.NewError("database connection validation", fmt.Errorf("database connection is nil"))
	}

	entitiesDbHandler := &EntitiesDBHandler{
		db: db,
	}

	err := sql.LoadEntitiesSql(entitiesDbHandler.db.Instance, force)
	if err != nil {
		return nil, helper.NewError("load entities sql", err)
	}

	err = entitiesDbHandler.CreateTable()
	if err != nil {
		return nil, helper.NewError("create table", err)
	}

	db.Logger.Info("Initialized EntitiesDBHandler")

	return entitiesDbHandler, nil
}

// CreateTable creates the 'entities' table in the database.
// If the table already exists, it does not create it again.
// It also creates all necessary indexes.
func (h *EntitiesDBHandler) CreateTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use the SQL init() function to create all tables, triggers, and indexes
	_, err := h.db.Instance.ExecContext(ctx, `SELECT init_entities();`)
	if err != nil {
		log.Panicf("error initializing entities table: %#v", err)
	}

	h.db.Logger.Info("Checked/created table entities")

	return nil
}

// InsertEntity inserts a new entity (or updates if exists)
func (h *EntitiesDBHandler) InsertEntity(entity *model.Entity) error {
	row := h.db.Instance.QueryRow(
		`SELECT * FROM insert_entity($1, $2, $3)`,
		entity.Name,
		entity.Type,
		entity.Metadata,
	)

	err := row.Scan(
		&entity.ID,
		&entity.Name,
		&entity.Type,
		&entity.Metadata,
		&entity.CreatedAt,
	)
	if err != nil {
		return helper.NewError("scan", err)
	}

	return nil
}

// SelectEntity retrieves an entity by ID
func (h *EntitiesDBHandler) SelectEntity(id uuid.UUID) (*model.Entity, error) {
	entity := &model.Entity{}
	row := h.db.Instance.QueryRow(
		`SELECT * FROM select_entity($1)`,
		id,
	)

	err := row.Scan(
		&entity.ID,
		&entity.Name,
		&entity.Type,
		&entity.Metadata,
		&entity.CreatedAt,
	)
	if err != nil {
		return nil, helper.NewError("scan", err)
	}

	return entity, nil
}

// SelectEntityByName retrieves an entity by name and type
func (h *EntitiesDBHandler) SelectEntityByName(name string, entityType string) (*model.Entity, error) {
	entity := &model.Entity{}
	row := h.db.Instance.QueryRow(
		`SELECT * FROM select_entity_by_name($1, $2)`,
		name,
		entityType,
	)

	err := row.Scan(
		&entity.ID,
		&entity.Name,
		&entity.Type,
		&entity.Metadata,
		&entity.CreatedAt,
	)
	if err != nil {
		return nil, helper.NewError("scan", err)
	}

	return entity, nil
}

// SelectEntitiesBySearch searches entities by name pattern
func (h *EntitiesDBHandler) SelectEntitiesBySearch(searchTerm string, entityType *string, limit int) ([]*model.Entity, error) {
	rows, err := h.db.Instance.Query(
		`SELECT * FROM search_entities($1, $2, $3)`,
		searchTerm,
		entityType,
		limit,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var entities []*model.Entity
	for rows.Next() {
		entity := &model.Entity{}
		err := rows.Scan(
			&entity.ID,
			&entity.Name,
			&entity.Type,
			&entity.Metadata,
			&entity.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		entities = append(entities, entity)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return entities, nil
}

// SelectEntitiesByType retrieves entities by type
func (h *EntitiesDBHandler) SelectEntitiesByType(entityType string, limit int) ([]*model.Entity, error) {
	rows, err := h.db.Instance.Query(
		`SELECT * FROM select_entities_by_type($1, $2)`,
		entityType,
		limit,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var entities []*model.Entity
	for rows.Next() {
		entity := &model.Entity{}
		err := rows.Scan(
			&entity.ID,
			&entity.Name,
			&entity.Type,
			&entity.Metadata,
			&entity.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		entities = append(entities, entity)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return entities, nil
}

// DeleteEntity deletes an entity by ID
func (h *EntitiesDBHandler) DeleteEntity(id uuid.UUID) error {
	_, err := h.db.Instance.Exec(
		`SELECT delete_entity($1)`,
		id,
	)
	if err != nil {
		return helper.NewError("exec", err)
	}
	return nil
}

// UpdateEntityMetadata updates the metadata of an entity
func (h *EntitiesDBHandler) UpdateEntityMetadata(id uuid.UUID, metadata model.Metadata) error {
	_, err := h.db.Instance.Exec(
		`SELECT * FROM update_entity_metadata($1, $2)`,
		id,
		metadata,
	)
	if err != nil {
		return helper.NewError("exec", err)
	}
	return nil
}

// SelectChunksMentioningEntity retrieves chunks that mention an entity
func (h *EntitiesDBHandler) SelectChunksMentioningEntity(entityID uuid.UUID) ([]*model.ChunkMention, error) {
	rows, err := h.db.Instance.Query(
		`SELECT * FROM select_chunks_mentioning_entity($1)`,
		entityID,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var mentions []*model.ChunkMention
	for rows.Next() {
		mention := &model.ChunkMention{}
		err := rows.Scan(
			&mention.ChunkID,
			&mention.EdgeID,
			&mention.EdgeMetadata,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		mentions = append(mentions, mention)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return mentions, nil
}

// GetEntity retrieves an entity by ID (alias for SelectEntity for interface compatibility)
func (h *EntitiesDBHandler) GetEntity(ctx context.Context, id string) (*model.Entity, error) {
	entityID, err := uuid.Parse(id)
	if err != nil {
		return nil, helper.NewError("parse uuid", err)
	}
	return h.SelectEntity(entityID)
}

// GetChunksForEntity retrieves all chunks related to an entity
func (h *EntitiesDBHandler) GetChunksForEntity(ctx context.Context, entityID string) ([]*model.Chunk, error) {
	entityUUID, err := uuid.Parse(entityID)
	if err != nil {
		return nil, helper.NewError("parse uuid", err)
	}

	// Get edges that connect this entity to chunks
	rows, err := h.db.Instance.QueryContext(ctx,
		`SELECT DISTINCT c.id, c.document_id, d.rid, c.content, c.path, c.embedding, 
		        c.start_pos, c.end_pos, c.chunk_index, c.metadata, c.created_at
		 FROM chunks c
		 LEFT JOIN documents d ON c.document_id = d.id
		 INNER JOIN edges e ON (
		     (e.source_entity_id = $1 AND e.target_chunk_id = c.id)
		     OR (e.target_entity_id = $1 AND e.source_chunk_id = c.id)
		 )
		 ORDER BY c.created_at`,
		entityUUID,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var chunks []*model.Chunk
	for rows.Next() {
		chunk := &model.Chunk{}
		var embeddingVec interface{}
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

		chunks = append(chunks, chunk)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return chunks, nil
}
