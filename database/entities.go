package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/siherrmann/grapher/helper"
	"github.com/siherrmann/grapher/model"
	"github.com/siherrmann/grapher/sql"
)

// EntitiesDBHandlerFunctions defines the interface for Entities database operations.
type EntitiesDBHandlerFunctions interface {
	InsertEntity(entity *model.Entity) error
	UpdateEntityMetadata(id int, metadata map[string]interface{}) error
	DeleteEntity(id int) error
	SelectEntity(id int) (*model.Entity, error)
	SelectEntityByName(name string, entityType string) (*model.Entity, error)
	SelectEntitiesBySearch(searchTerm string, entityType *string, limit int) ([]*model.Entity, error)
	SelectEntitiesByType(entityType string, limit int) ([]*model.Entity, error)
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

// UpdateEntityMetadata updates the metadata of an entity
func (h *EntitiesDBHandler) UpdateEntityMetadata(id int, metadata model.Metadata) error {
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

// DeleteEntity deletes an entity by ID
func (h *EntitiesDBHandler) DeleteEntity(id int) error {
	_, err := h.db.Instance.Exec(
		`SELECT delete_entity($1)`,
		id,
	)
	if err != nil {
		return helper.NewError("exec", err)
	}
	return nil
}

// SelectEntity retrieves an entity by ID
func (h *EntitiesDBHandler) SelectEntity(id int) (*model.Entity, error) {
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
		`SELECT * FROM select_entities_by_search($1, $2, $3)`,
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
