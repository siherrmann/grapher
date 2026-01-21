package helper

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/knights-analytics/hugot"
)

// PrepareModel downloads the model if it doesn't exist and returns the model path
func PrepareModel(modelName string, onnxFilePath string) (string, error) {
	modelDir := "./models"

	// Sanitize model name for directory (replace / with _)
	sanitizedName := filepath.Base(modelName)
	if filepath.Dir(modelName) != "." {
		sanitizedName = filepath.Dir(modelName) + "_" + filepath.Base(modelName)
	}
	modelPath := filepath.Join(modelDir, sanitizedName)

	// Check if model exists, if not download it
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		if err := os.MkdirAll(modelDir, 0750); err != nil {
			return "", fmt.Errorf("failed to create model directory: %w", err)
		}
		downloadOptions := hugot.NewDownloadOptions()
		if onnxFilePath != "" {
			downloadOptions.OnnxFilePath = onnxFilePath
		}
		downloadedPath, err := hugot.DownloadModel(modelName, modelDir, downloadOptions)
		if err != nil {
			return "", fmt.Errorf("failed to download model: %w", err)
		}
		modelPath = downloadedPath
	}

	return modelPath, nil
}
