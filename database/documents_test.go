package database

import (
	"testing"
	"time"

	"github.com/siherrmann/grapher/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocumentsNewDocumentsDBHandler(t *testing.T) {
	database := initDB(t)

	t.Run("Valid call NewDocumentsDBHandler", func(t *testing.T) {
		documentsDbHandler, err := NewDocumentsDBHandler(database, true)
		assert.NoError(t, err, "Expected NewDocumentsDBHandler to not return an error")
		require.NotNil(t, documentsDbHandler, "Expected NewDocumentsDBHandler to return a non-nil instance")
		require.NotNil(t, documentsDbHandler.db, "Expected NewDocumentsDBHandler to have a non-nil database instance")
		require.NotNil(t, documentsDbHandler.db.Instance, "Expected NewDocumentsDBHandler to have a non-nil database connection instance")
	})

	t.Run("Invalid call NewDocumentsDBHandler with nil database", func(t *testing.T) {
		_, err := NewDocumentsDBHandler(nil, false)
		assert.Error(t, err, "Expected error when creating DocumentsDBHandler with nil database")
		assert.Contains(t, err.Error(), "database connection is nil", "Expected specific error message for nil database connection")
	})
}

func TestDocumentsInsert(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err, "Expected NewDocumentsDBHandler to not return an error")

	t.Run("Insert document", func(t *testing.T) {
		doc := &model.Document{
			Title:    "Test Document",
			Source:   "test_source.txt",
			Metadata: map[string]interface{}{"author": "Test Author", "year": 2024},
		}

		err := documentsDbHandler.InsertDocument(doc)
		assert.NoError(t, err, "Expected Insert to not return an error")
		assert.NotEmpty(t, doc.RID, "Expected inserted document to have a RID")
		assert.WithinDuration(t, doc.CreatedAt, time.Now(), 2*time.Second, "Expected CreatedAt to be set")
		assert.WithinDuration(t, doc.UpdatedAt, time.Now(), 2*time.Second, "Expected UpdatedAt to be set")
		assert.Equal(t, "Test Document", doc.Title, "Expected title to match")

		// Cleanup
		documentsDbHandler.DeleteDocument(doc.RID)
	})
}

func TestDocumentsGet(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	// Create a document
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{"key": "value"},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Test Get
	retrievedDoc, err := documentsDbHandler.SelectDocument(doc.RID)
	assert.NoError(t, err, "Expected Get to not return an error")
	assert.NotNil(t, retrievedDoc, "Expected Get to return a non-nil document")
	assert.Equal(t, doc.RID, retrievedDoc.RID, "Expected document RIDs to match")
	assert.Equal(t, doc.Title, retrievedDoc.Title, "Expected titles to match")
	assert.Equal(t, doc.Source, retrievedDoc.Source, "Expected sources to match")

	// Cleanup
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestDocumentsGetAll(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	// Create multiple documents
	docCount := 5
	docs := make([]*model.Document, docCount)
	for i := 0; i < docCount; i++ {
		docs[i] = &model.Document{
			Title:    "Test Document " + string(rune('A'+i)),
			Source:   "test.txt",
			Metadata: map[string]interface{}{},
		}
		err = documentsDbHandler.InsertDocument(docs[i])
		require.NoError(t, err)
	}

	// Test SelectAllDocuments
	retrievedDocs, err := documentsDbHandler.SelectAllDocuments(nil, 10)
	assert.NoError(t, err, "Expected SelectAllDocuments to not return an error")
	assert.GreaterOrEqual(t, len(retrievedDocs), docCount, "Expected to retrieve at least the inserted documents")

	// Test pagination
	pageLength := 3
	paginatedDocs, err := documentsDbHandler.SelectAllDocuments(nil, pageLength)
	assert.NoError(t, err, "Expected SelectAllDocuments to not return an error")
	assert.LessOrEqual(t, len(paginatedDocs), pageLength, "Expected at most pageLength documents")

	// Cleanup
	for _, doc := range docs {
		documentsDbHandler.DeleteDocument(doc.RID)
	}
}

func TestDocumentsSearch(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	// Create documents with different titles
	searchTerm := "UniqueSearchTerm"
	matchingDocs := 3
	otherDocs := 2

	docs := []*model.Document{}

	for i := 0; i < matchingDocs; i++ {
		doc := &model.Document{
			Title:    searchTerm + " Document " + string(rune('A'+i)),
			Source:   "test.txt",
			Metadata: map[string]interface{}{},
		}
		err = documentsDbHandler.InsertDocument(doc)
		require.NoError(t, err)
		docs = append(docs, doc)
	}

	for i := 0; i < otherDocs; i++ {
		doc := &model.Document{
			Title:    "Other Document " + string(rune('A'+i)),
			Source:   "test.txt",
			Metadata: map[string]interface{}{},
		}
		err = documentsDbHandler.InsertDocument(doc)
		require.NoError(t, err)
		docs = append(docs, doc)
	}

	// Test Search
	results, err := documentsDbHandler.SelectDocumentsBySearch(searchTerm, 10)
	assert.NoError(t, err, "Expected SelectDocumentsBySearch to not return an error")
	assert.Len(t, results, matchingDocs, "Expected to find only matching documents")

	// Cleanup
	for _, doc := range docs {
		documentsDbHandler.DeleteDocument(doc.RID)
	}
}

func TestDocumentsUpdate(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	// Create a document
	doc := &model.Document{
		Title:    "Original Title",
		Source:   "original.txt",
		Metadata: map[string]interface{}{"version": 1},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Update the document
	doc.Title = "Updated Title"
	doc.Source = "updated.txt"
	doc.Metadata = map[string]interface{}{"version": 2}

	err = documentsDbHandler.UpdateDocument(doc)
	assert.NoError(t, err, "Expected UpdateDocument to not return an error")

	// Verify update
	retrievedDoc, err := documentsDbHandler.SelectDocument(doc.RID)
	require.NoError(t, err)
	assert.Equal(t, "Updated Title", retrievedDoc.Title, "Expected title to be updated")
	assert.Equal(t, "updated.txt", retrievedDoc.Source, "Expected source to be updated")
	assert.Equal(t, float64(2), retrievedDoc.Metadata["version"], "Expected metadata to be updated")

	// Cleanup
	documentsDbHandler.DeleteDocument(doc.RID)
}

func TestDocumentsDelete(t *testing.T) {
	database := initDB(t)

	documentsDbHandler, err := NewDocumentsDBHandler(database, true)
	require.NoError(t, err)

	// Create a document
	doc := &model.Document{
		Title:    "Test Document",
		Source:   "test.txt",
		Metadata: map[string]interface{}{},
	}
	err = documentsDbHandler.InsertDocument(doc)
	require.NoError(t, err)

	// Delete the document
	err = documentsDbHandler.DeleteDocument(doc.RID)
	assert.NoError(t, err, "Expected Delete to not return an error")

	// Verify deletion
	_, err = documentsDbHandler.SelectDocument(doc.RID)
	assert.Error(t, err, "Expected Get to return an error for deleted document")
}
