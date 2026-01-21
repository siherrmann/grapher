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

// DocumentsDBHandlerFunctions defines the interface for Documents database operations.
type DocumentsDBHandlerFunctions interface {
	InsertDocument(doc *model.Document) error
	SelectDocument(rid uuid.UUID) (*model.Document, error)
	SelectAllDocuments(lastCreatedAt *time.Time, limit int) ([]*model.Document, error)
	SelectDocumentsBySearch(searchTerm string, limit int) ([]*model.Document, error)
	UpdateDocument(doc *model.Document) error
	DeleteDocument(rid uuid.UUID) error
}

// DocumentsDBHandler handles document-related database operations
type DocumentsDBHandler struct {
	db *helper.Database
}

// NewDocumentsDBHandler creates a new documents database handler.
// It initializes the database connection and loads document-related SQL functions.
// If force is true, it will reload the SQL functions even if they already exist.
func NewDocumentsDBHandler(db *helper.Database, force bool) (*DocumentsDBHandler, error) {
	if db == nil {
		return nil, helper.NewError("database connection validation", fmt.Errorf("database connection is nil"))
	}

	documentsDbHandler := &DocumentsDBHandler{
		db: db,
	}

	err := sql.LoadDocumentsSql(documentsDbHandler.db.Instance, force)
	if err != nil {
		return nil, helper.NewError("load documents sql", err)
	}

	err = documentsDbHandler.CreateTable()
	if err != nil {
		return nil, helper.NewError("create table", err)
	}

	db.Logger.Info("Initialized DocumentsDBHandler")

	return documentsDbHandler, nil
}

// CreateTable creates the 'documents' table in the database.
// If the table already exists, it does not create it again.
// It also creates all necessary indexes and triggers.
func (h *DocumentsDBHandler) CreateTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use the SQL init() function to create all tables, triggers, and indexes
	_, err := h.db.Instance.ExecContext(ctx, `SELECT init_documents();`)
	if err != nil {
		log.Panicf("error initializing documents table: %#v", err)
	}

	h.db.Logger.Info("Checked/created table documents")

	return nil
}

// InsertDocument inserts a new document
func (h *DocumentsDBHandler) InsertDocument(doc *model.Document) error {
	row := h.db.Instance.QueryRow(
		`SELECT * FROM insert_document($1, $2, $3)`,
		doc.Title,
		doc.Source,
		doc.Metadata,
	)

	err := row.Scan(
		&doc.ID,
		&doc.RID,
		&doc.Title,
		&doc.Source,
		&doc.Metadata,
		&doc.CreatedAt,
		&doc.UpdatedAt,
	)
	if err != nil {
		return helper.NewError("scan", err)
	}

	return nil
}

// SelectDocument retrieves a document by RID
func (h *DocumentsDBHandler) SelectDocument(rid uuid.UUID) (*model.Document, error) {
	doc := &model.Document{}
	row := h.db.Instance.QueryRow(
		`SELECT * FROM select_document($1)`,
		rid,
	)

	err := row.Scan(
		&doc.ID,
		&doc.RID,
		&doc.Title,
		&doc.Source,
		&doc.Metadata,
		&doc.CreatedAt,
		&doc.UpdatedAt,
	)
	if err != nil {
		return nil, helper.NewError("scan", err)
	}

	return doc, nil
}

// SelectAllDocuments retrieves all documents with pagination
func (h *DocumentsDBHandler) SelectAllDocuments(lastCreatedAt *time.Time, limit int) ([]*model.Document, error) {
	rows, err := h.db.Instance.Query(
		`SELECT * FROM select_all_documents($1, $2)`,
		lastCreatedAt,
		limit,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var documents []*model.Document
	for rows.Next() {
		doc := &model.Document{}
		err := rows.Scan(
			&doc.ID,
			&doc.RID,
			&doc.Title,
			&doc.Source,
			&doc.Metadata,
			&doc.CreatedAt,
			&doc.UpdatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		documents = append(documents, doc)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return documents, nil
}

// SelectDocumentsBySearch searches documents by title or source
func (h *DocumentsDBHandler) SelectDocumentsBySearch(searchTerm string, limit int) ([]*model.Document, error) {
	rows, err := h.db.Instance.Query(
		`SELECT * FROM search_documents($1, $2)`,
		searchTerm,
		limit,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
	}
	defer rows.Close()

	var documents []*model.Document
	for rows.Next() {
		doc := &model.Document{}
		err := rows.Scan(
			&doc.ID,
			&doc.RID,
			&doc.Title,
			&doc.Source,
			&doc.Metadata,
			&doc.CreatedAt,
			&doc.UpdatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		documents = append(documents, doc)
	}

	err = rows.Err()
	if err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return documents, nil
}

// UpdateDocument updates a document
func (h *DocumentsDBHandler) UpdateDocument(doc *model.Document) error {
	row := h.db.Instance.QueryRow(
		`SELECT * FROM update_document($1, $2, $3, $4)`,
		doc.RID,
		doc.Title,
		doc.Source,
		doc.Metadata,
	)

	err := row.Scan(
		&doc.ID,
		&doc.RID,
		&doc.Title,
		&doc.Source,
		&doc.Metadata,
		&doc.CreatedAt,
		&doc.UpdatedAt,
	)
	if err != nil {
		return helper.NewError("scan", err)
	}

	return nil
}

// DeleteDocument deletes a document by RID
func (h *DocumentsDBHandler) DeleteDocument(rid uuid.UUID) error {
	_, err := h.db.Instance.Exec(
		`SELECT delete_document($1)`,
		rid,
	)
	if err != nil {
		return helper.NewError("exec", err)
	}
	return nil
}
