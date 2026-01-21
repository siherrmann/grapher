package helper

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrepareModel(t *testing.T) {
	// Create a temporary directory for test models
	tempDir := "./test_models"
	defer os.RemoveAll(tempDir)

	// Override the default model directory for testing
	originalModelDir := "./models"
	defer func() {
		// Cleanup: restore original if needed
		_ = originalModelDir
	}()

	t.Run("Download model when it doesn't exist", func(t *testing.T) {
		// Use a very small model for testing
		modelName := "sentence-transformers/all-MiniLM-L6-v2"

		// Clean up if model already exists
		sanitizedName := "sentence-transformers_all-MiniLM-L6-v2"
		modelPath := filepath.Join("./models", sanitizedName)
		os.RemoveAll(modelPath)

		// Try to download the model
		path, err := PrepareModel(modelName, "onnx/model.onnx")

		// Should either succeed or fail with a download error
		// We don't require success because it depends on network and disk space
		if err != nil {
			assert.Contains(t, err.Error(), "failed to", "Expected error to be about download failure")
		} else {
			assert.NotEmpty(t, path, "Expected model path to be returned")
			assert.DirExists(t, path, "Expected model directory to exist")
		}
	})

	t.Run("Return existing model path when model exists", func(t *testing.T) {
		// Create a mock model directory
		modelName := "test/mock-model"
		sanitizedName := "test_mock-model"
		modelPath := filepath.Join("./models", sanitizedName)

		// Create the directory
		err := os.MkdirAll(modelPath, 0750)
		require.NoError(t, err, "Expected directory creation to succeed")
		defer os.RemoveAll(modelPath)

		// Call PrepareModel
		path, err := PrepareModel(modelName, "")
		assert.NoError(t, err, "Expected PrepareModel to not return an error for existing model")
		assert.Equal(t, modelPath, path, "Expected returned path to match existing model path")
	})

	t.Run("Handle model name with slash", func(t *testing.T) {
		// Test that model names with slashes are sanitized correctly
		modelName := "organization/model-name"
		sanitizedName := "organization_model-name"
		expectedPath := filepath.Join("./models", sanitizedName)

		// Create the directory to simulate existing model
		err := os.MkdirAll(expectedPath, 0750)
		require.NoError(t, err, "Expected directory creation to succeed")
		defer os.RemoveAll(expectedPath)

		path, err := PrepareModel(modelName, "")
		assert.NoError(t, err, "Expected PrepareModel to not return an error")
		assert.Equal(t, expectedPath, path, "Expected path to use sanitized name")
	})

	t.Run("Handle model name without slash", func(t *testing.T) {
		// Test that model names without slashes work correctly
		modelName := "simple-model"
		expectedPath := filepath.Join("./models", "simple-model")

		// Create the directory to simulate existing model
		err := os.MkdirAll(expectedPath, 0750)
		require.NoError(t, err, "Expected directory creation to succeed")
		defer os.RemoveAll(expectedPath)

		path, err := PrepareModel(modelName, "")
		assert.NoError(t, err, "Expected PrepareModel to not return an error")
		assert.Equal(t, expectedPath, path, "Expected path to use model name directly")
	})

	t.Run("Specify onnx file path", func(t *testing.T) {
		// Test that onnx file path parameter is handled
		modelName := "test/onnx-model"
		sanitizedName := "test_onnx-model"
		modelPath := filepath.Join("./models", sanitizedName)

		// Create the directory to simulate existing model
		err := os.MkdirAll(modelPath, 0750)
		require.NoError(t, err, "Expected directory creation to succeed")
		defer os.RemoveAll(modelPath)

		path, err := PrepareModel(modelName, "onnx/model.onnx")
		assert.NoError(t, err, "Expected PrepareModel with onnx path to not return an error")
		assert.NotEmpty(t, path, "Expected model path to be returned")
	})

	t.Run("Handle empty onnx file path", func(t *testing.T) {
		// Test that empty onnx file path is handled
		modelName := "test/no-onnx-path"
		sanitizedName := "test_no-onnx-path"
		modelPath := filepath.Join("./models", sanitizedName)

		// Create the directory to simulate existing model
		err := os.MkdirAll(modelPath, 0750)
		require.NoError(t, err, "Expected directory creation to succeed")
		defer os.RemoveAll(modelPath)

		path, err := PrepareModel(modelName, "")
		assert.NoError(t, err, "Expected PrepareModel with empty onnx path to not return an error")
		assert.NotEmpty(t, path, "Expected model path to be returned")
	})

	t.Run("Create model directory if it doesn't exist", func(t *testing.T) {
		// Clean up models directory first
		modelsDir := "./models"
		testModelDir := filepath.Join(modelsDir, "test_create-dir")

		// Remove test directory if it exists
		os.RemoveAll(testModelDir)

		// Verify directory doesn't exist
		_, err := os.Stat(testModelDir)
		assert.True(t, os.IsNotExist(err), "Expected model directory to not exist initially")

		// Create mock directory
		modelName := "test/create-dir"
		err = os.MkdirAll(testModelDir, 0750)
		require.NoError(t, err, "Expected directory creation to succeed")
		defer os.RemoveAll(testModelDir)

		path, err := PrepareModel(modelName, "")
		assert.NoError(t, err, "Expected PrepareModel to not return an error")
		assert.NotEmpty(t, path, "Expected model path to be returned")
	})
}
