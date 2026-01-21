package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/siherrmann/grapher/helper"
	"github.com/siherrmann/grapher/model"
	loadSql "github.com/siherrmann/grapher/sql"
)

// EdgesDBHandlerFunctions defines the interface for Edges database operations.
type EdgesDBHandlerFunctions interface {
	InsertEdge(edge *model.Edge) error
	SelectEdge(id uuid.UUID) (*model.Edge, error)
	SelectEdgesFromChunk(chunkID uuid.UUID, edgeType *model.EdgeType) ([]*model.Edge, error)
	SelectEdgesToChunk(chunkID uuid.UUID, edgeType *model.EdgeType) ([]*model.Edge, error)
	SelectEdgesConnectedToChunk(chunkID uuid.UUID, edgeType *model.EdgeType) ([]*model.EdgeConnection, error)
	SelectEdgesFromEntity(entityID uuid.UUID, edgeType *model.EdgeType) ([]*model.Edge, error)
	SelectEdgesToEntity(entityID uuid.UUID, edgeType *model.EdgeType) ([]*model.Edge, error)
	DeleteEdge(id uuid.UUID) error
	UpdateEdgeWeight(id uuid.UUID, weight float64) error
	TraverseBFSFromChunk(startChunkID uuid.UUID, maxDepth int, edgeType *model.EdgeType) ([]*model.TraversalNode, error)
}

// EdgesDBHandler handles edge-related database operations
type EdgesDBHandler struct {
	db *helper.Database
}

// NewEdgesDBHandler creates a new edges database handler.
// It initializes the database connection and loads edge-related SQL functions.
// If force is true, it will reload the SQL functions even if they already exist.
func NewEdgesDBHandler(db *helper.Database, force bool) (*EdgesDBHandler, error) {
	if db == nil {
		return nil, helper.NewError("database connection validation", fmt.Errorf("database connection is nil"))
	}

	edgesDbHandler := &EdgesDBHandler{
		db: db,
	}

	err := loadSql.LoadEdgesSql(edgesDbHandler.db.Instance, force)
	if err != nil {
		return nil, helper.NewError("load edges sql", err)
	}

	err = edgesDbHandler.CreateTable()
	if err != nil {
		return nil, helper.NewError("create table", err)
	}

	db.Logger.Info("Initialized EdgesDBHandler")

	return edgesDbHandler, nil
}

// CreateTable creates the 'edges' table in the database.
// If the table already exists, it does not create it again.
// It also creates the edge_type enum and all necessary indexes.
func (h *EdgesDBHandler) CreateTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use the SQL init() function to create all tables, triggers, and indexes
	_, err := h.db.Instance.ExecContext(ctx, `SELECT init_edges();`)
	if err != nil {
		log.Panicf("error initializing edges table: %#v", err)
	}

	h.db.Logger.Info("Checked/created table edges")

	return nil
}

// InsertEdge inserts a new edge
func (h *EdgesDBHandler) InsertEdge(edge *model.Edge) error {
	row := h.db.Instance.QueryRow(
		`SELECT * FROM insert_edge($1, $2, $3, $4, $5, $6, $7, $8)`,
		edge.SourceChunkID,
		edge.TargetChunkID,
		edge.SourceEntityID,
		edge.TargetEntityID,
		edge.EdgeType,
		edge.Weight,
		edge.Bidirectional,
		edge.Metadata,
	)

	err := row.Scan(
		&edge.ID,
		&edge.SourceChunkID,
		&edge.TargetChunkID,
		&edge.SourceEntityID,
		&edge.TargetEntityID,
		&edge.EdgeType,
		&edge.Weight,
		&edge.Bidirectional,
		&edge.Metadata,
		&edge.CreatedAt,
	)
	if err != nil {
		return helper.NewError("scan", err)
	}

	return nil
}

// SelectEdge retrieves an edge by ID
func (h *EdgesDBHandler) SelectEdge(id uuid.UUID) (*model.Edge, error) {
	row := h.db.Instance.QueryRow(
		`SELECT * FROM select_edge($1)`,
		id,
	)

	edge := &model.Edge{}

	err := row.Scan(
		&edge.ID,
		&edge.SourceChunkID,
		&edge.TargetChunkID,
		&edge.SourceEntityID,
		&edge.TargetEntityID,
		&edge.EdgeType,
		&edge.Weight,
		&edge.Bidirectional,
		&edge.Metadata,
		&edge.CreatedAt,
	)
	if err != nil {
		return nil, helper.NewError("scan", err)
	}

	return edge, nil
}

// SelectEdgesFromChunk retrieves edges originating from a chunk
func (h *EdgesDBHandler) SelectEdgesFromChunk(chunkID uuid.UUID, edgeType *model.EdgeType) ([]*model.Edge, error) {
	var rows *sql.Rows
	var err error

	if edgeType != nil {
		rows, err = h.db.Instance.Query(
			`SELECT * FROM select_edges_from_chunk($1, $2)`,
			chunkID,
			*edgeType,
		)
	} else {
		rows, err = h.db.Instance.Query(
			`SELECT * FROM select_edges_from_chunk($1, NULL)`,
			chunkID,
		)
	}

	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var edges []*model.Edge
	for rows.Next() {
		edge := &model.Edge{}

		err := rows.Scan(
			&edge.ID,
			&edge.SourceChunkID,
			&edge.TargetChunkID,
			&edge.SourceEntityID,
			&edge.TargetEntityID,
			&edge.EdgeType,
			&edge.Weight,
			&edge.Bidirectional,
			&edge.Metadata,
			&edge.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		edges = append(edges, edge)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return edges, nil
}

// SelectEdgesToChunk retrieves edges targeting a chunk
func (h *EdgesDBHandler) SelectEdgesToChunk(chunkID uuid.UUID, edgeType *model.EdgeType) ([]*model.Edge, error) {
	var rows *sql.Rows
	var err error

	if edgeType != nil {
		rows, err = h.db.Instance.Query(
			`SELECT * FROM select_edges_to_chunk($1, $2)`,
			chunkID,
			*edgeType,
		)
	} else {
		rows, err = h.db.Instance.Query(
			`SELECT * FROM select_edges_to_chunk($1, NULL)`,
			chunkID,
		)
	}

	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var edges []*model.Edge
	for rows.Next() {
		edge := &model.Edge{}
		err := rows.Scan(
			&edge.ID,
			&edge.SourceChunkID,
			&edge.TargetChunkID,
			&edge.SourceEntityID,
			&edge.TargetEntityID,
			&edge.EdgeType,
			&edge.Weight,
			&edge.Bidirectional,
			&edge.Metadata,
			&edge.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		edges = append(edges, edge)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return edges, nil
}

// SelectEdgesConnectedToChunk retrieves all edges connected to a chunk (both directions)
func (h *EdgesDBHandler) SelectEdgesConnectedToChunk(chunkID uuid.UUID, edgeType *model.EdgeType) ([]*model.EdgeConnection, error) {
	var rows *sql.Rows
	var err error

	if edgeType != nil {
		rows, err = h.db.Instance.Query(
			`SELECT * FROM select_edges_connected_to_chunk($1, $2)`,
			chunkID,
			*edgeType,
		)
	} else {
		rows, err = h.db.Instance.Query(
			`SELECT * FROM select_edges_connected_to_chunk($1, NULL)`,
			chunkID,
		)
	}

	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var connections []*model.EdgeConnection
	for rows.Next() {
		edge := &model.Edge{}
		var isOutgoing bool
		err := rows.Scan(
			&edge.ID,
			&edge.SourceChunkID,
			&edge.TargetChunkID,
			&edge.SourceEntityID,
			&edge.TargetEntityID,
			&edge.EdgeType,
			&edge.Weight,
			&edge.Bidirectional,
			&edge.Metadata,
			&edge.CreatedAt,
			&isOutgoing,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		connections = append(connections, &model.EdgeConnection{
			Edge:       edge,
			IsOutgoing: isOutgoing,
		})
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return connections, nil
}

// SelectEdgesFromEntity retrieves edges originating from an entity
func (h *EdgesDBHandler) SelectEdgesFromEntity(entityID uuid.UUID, edgeType *model.EdgeType) ([]*model.Edge, error) {
	var rows *sql.Rows
	var err error

	if edgeType != nil {
		rows, err = h.db.Instance.Query(
			`SELECT * FROM select_edges_from_entity($1, $2)`,
			entityID,
			*edgeType,
		)
	} else {
		rows, err = h.db.Instance.Query(
			`SELECT * FROM select_edges_from_entity($1, NULL)`,
			entityID,
		)
	}

	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var edges []*model.Edge
	for rows.Next() {
		edge := &model.Edge{}
		err := rows.Scan(
			&edge.ID,
			&edge.SourceChunkID,
			&edge.TargetChunkID,
			&edge.SourceEntityID,
			&edge.TargetEntityID,
			&edge.EdgeType,
			&edge.Weight,
			&edge.Bidirectional,
			&edge.Metadata,
			&edge.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		edges = append(edges, edge)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return edges, nil
}

// SelectEdgesToEntity retrieves edges targeting an entity
func (h *EdgesDBHandler) SelectEdgesToEntity(entityID uuid.UUID, edgeType *model.EdgeType) ([]*model.Edge, error) {
	var rows *sql.Rows
	var err error

	if edgeType != nil {
		rows, err = h.db.Instance.Query(
			`SELECT * FROM select_edges_to_entity($1, $2)`,
			entityID,
			*edgeType,
		)
	} else {
		rows, err = h.db.Instance.Query(
			`SELECT * FROM select_edges_to_entity($1, NULL)`,
			entityID,
		)
	}

	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var edges []*model.Edge
	for rows.Next() {
		edge := &model.Edge{}
		err := rows.Scan(
			&edge.ID,
			&edge.SourceChunkID,
			&edge.TargetChunkID,
			&edge.SourceEntityID,
			&edge.TargetEntityID,
			&edge.EdgeType,
			&edge.Weight,
			&edge.Bidirectional,
			&edge.Metadata,
			&edge.CreatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		edges = append(edges, edge)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return edges, nil
}

// DeleteEdge deletes an edge by ID
func (h *EdgesDBHandler) DeleteEdge(id uuid.UUID) error {
	_, err := h.db.Instance.Exec(
		`SELECT delete_edge($1)`,
		id,
	)
	if err != nil {
		return helper.NewError("exec", err)
	}
	return nil
}

// UpdateEdgeWeight updates the weight of an edge
func (h *EdgesDBHandler) UpdateEdgeWeight(id uuid.UUID, weight float64) error {
	_, err := h.db.Instance.Exec(
		`SELECT * FROM update_edge_weight($1, $2)`,
		id,
		weight,
	)
	if err != nil {
		return helper.NewError("exec", err)
	}
	return nil
}

// TraverseBFSFromChunk performs breadth-first search from a starting chunk
func (h *EdgesDBHandler) TraverseBFSFromChunk(startChunkID uuid.UUID, maxDepth int, edgeType *model.EdgeType) ([]*model.TraversalNode, error) {
	var rows *sql.Rows
	var err error

	if edgeType != nil {
		rows, err = h.db.Instance.Query(
			`SELECT * FROM traverse_bfs_from_chunk($1, $2, $3)`,
			startChunkID,
			maxDepth,
			*edgeType,
		)
	} else {
		rows, err = h.db.Instance.Query(
			`SELECT * FROM traverse_bfs_from_chunk($1, $2, NULL)`,
			startChunkID,
			maxDepth,
		)
	}

	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var nodes []*model.TraversalNode
	for rows.Next() {
		node := &model.TraversalNode{}
		var pathArray []byte
		err := rows.Scan(
			&node.ChunkID,
			&node.Depth,
			&pathArray,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		// Parse PostgreSQL UUID array
		// Format: {uuid1,uuid2,uuid3}
		if err := parseUUIDArray(pathArray, &node.Path); err != nil {
			return nil, helper.NewError("parsing path array", err)
		}

		nodes = append(nodes, node)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return nodes, nil
}

// parseUUIDArray parses PostgreSQL UUID array format
func parseUUIDArray(data []byte, result *[]uuid.UUID) error {
	// PostgreSQL array format: {uuid1,uuid2,uuid3}
	str := string(data)
	if len(str) < 2 || str[0] != '{' || str[len(str)-1] != '}' {
		return helper.NewError("invalid array format", fmt.Errorf("%s", str))
	}

	// Remove braces
	str = str[1 : len(str)-1]
	if str == "" {
		*result = []uuid.UUID{}
		return nil
	}

	// Split by comma
	parts := []string{}
	current := ""
	for _, ch := range str {
		if ch == ',' {
			parts = append(parts, current)
			current = ""
		} else {
			current += string(ch)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}

	// Parse each UUID
	*result = make([]uuid.UUID, 0, len(parts))
	for _, part := range parts {
		id, err := uuid.Parse(part)
		if err != nil {
			return helper.NewError(fmt.Sprintf("parsing UUID %s", part), err)
		}
		*result = append(*result, id)
	}

	return nil
}
