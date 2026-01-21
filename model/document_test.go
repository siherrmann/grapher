package model

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDocumentFromFile(t *testing.T) {
	t.Run("Successfully reads file and creates document", func(t *testing.T) {
		// Create temporary file
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "test.txt")
		content := "This is test content"
		err := os.WriteFile(filePath, []byte(content), 0644)
		require.NoError(t, err)

		// Create document from file
		metadata := Metadata{"author": "test"}
		doc, err := NewDocumentFromFile(filePath, metadata)

		require.NoError(t, err)
		assert.Equal(t, "test", doc.Title, "Title should be filename without extension")
		assert.Equal(t, filePath, doc.Source, "Source should be file path")
		assert.Equal(t, content, doc.Content, "Content should match file content")
		assert.Equal(t, "test", doc.Metadata["author"])
	})

	t.Run("Returns error for non-existent file", func(t *testing.T) {
		doc, err := NewDocumentFromFile("/non/existent/file.txt", nil)

		require.Error(t, err)
		assert.Nil(t, doc)
	})

	t.Run("Handles empty file", func(t *testing.T) {
		// Create empty temporary file
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "empty.txt")
		err := os.WriteFile(filePath, []byte(""), 0644)
		require.NoError(t, err)

		doc, err := NewDocumentFromFile(filePath, nil)

		require.NoError(t, err)
		assert.Equal(t, "empty", doc.Title)
		assert.Equal(t, "", doc.Content)
	})

	t.Run("Handles file without extension", func(t *testing.T) {
		// Create temporary file without extension
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "README")
		content := "Readme content"
		err := os.WriteFile(filePath, []byte(content), 0644)
		require.NoError(t, err)

		doc, err := NewDocumentFromFile(filePath, nil)

		require.NoError(t, err)
		assert.Equal(t, "README", doc.Title, "Title should be full filename when no extension")
		assert.Equal(t, content, doc.Content)
	})

	t.Run("Handles file with multiple dots in name", func(t *testing.T) {
		// Create temporary file with dots in name
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "my.file.name.txt")
		content := "Content with dots"
		err := os.WriteFile(filePath, []byte(content), 0644)
		require.NoError(t, err)

		doc, err := NewDocumentFromFile(filePath, nil)

		require.NoError(t, err)
		assert.Equal(t, "my.file.name", doc.Title, "Title should remove only last extension")
		assert.Equal(t, content, doc.Content)
	})

	t.Run("Handles nil metadata", func(t *testing.T) {
		// Create temporary file
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "test.txt")
		err := os.WriteFile(filePath, []byte("content"), 0644)
		require.NoError(t, err)

		doc, err := NewDocumentFromFile(filePath, nil)

		require.NoError(t, err)
		assert.Nil(t, doc.Metadata)
	})

	t.Run("Handles complex metadata", func(t *testing.T) {
		// Create temporary file
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "document.md")
		content := "# Markdown content"
		err := os.WriteFile(filePath, []byte(content), 0644)
		require.NoError(t, err)

		metadata := Metadata{
			"author":   "John Doe",
			"version":  1,
			"tags":     []string{"test", "example"},
			"settings": map[string]interface{}{"enabled": true},
		}
		doc, err := NewDocumentFromFile(filePath, metadata)

		require.NoError(t, err)
		assert.Equal(t, "document", doc.Title)
		assert.Equal(t, "John Doe", doc.Metadata["author"])
		assert.Equal(t, 1, doc.Metadata["version"])
	})

	t.Run("Preserves file path as source", func(t *testing.T) {
		// Create temporary file with specific path
		tmpDir := t.TempDir()
		subDir := filepath.Join(tmpDir, "subdir")
		err := os.MkdirAll(subDir, 0755)
		require.NoError(t, err)

		filePath := filepath.Join(subDir, "nested.txt")
		err = os.WriteFile(filePath, []byte("nested content"), 0644)
		require.NoError(t, err)

		doc, err := NewDocumentFromFile(filePath, nil)

		require.NoError(t, err)
		assert.Equal(t, filePath, doc.Source, "Source should preserve full path")
		assert.Contains(t, doc.Source, "subdir")
	})

	t.Run("Handles large file content", func(t *testing.T) {
		// Create temporary file with large content
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "large.txt")
		largeContent := make([]byte, 1024*1024) // 1MB
		for i := range largeContent {
			largeContent[i] = byte('A' + (i % 26))
		}
		err := os.WriteFile(filePath, largeContent, 0644)
		require.NoError(t, err)

		doc, err := NewDocumentFromFile(filePath, nil)

		require.NoError(t, err)
		assert.Equal(t, len(largeContent), len(doc.Content))
	})

	t.Run("Handles unicode content", func(t *testing.T) {
		// Create temporary file with unicode
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "unicode.txt")
		unicodeContent := "Hello 世界 🌍 Привет"
		err := os.WriteFile(filePath, []byte(unicodeContent), 0644)
		require.NoError(t, err)

		doc, err := NewDocumentFromFile(filePath, nil)

		require.NoError(t, err)
		assert.Equal(t, unicodeContent, doc.Content)
	})
}
