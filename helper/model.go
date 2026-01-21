package helper

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/knights-analytics/hugot"
)

// PrepareModel downloads the model if it doesn't exist and returns the model path
func PrepareModel(modelName string) (string, error) {
	modelDir := "./models"
	modelPath := filepath.Join(modelDir, "sentence-transformers_all-MiniLM-L6-v2")

	// Check if model exists, if not download it
	if _, err := os.Stat(modelPath); os.IsNotExist(err) {
		if err := os.MkdirAll(modelDir, 0755); err != nil {
			return "", fmt.Errorf("failed to create model directory: %w", err)
		}
		downloadOptions := hugot.NewDownloadOptions()
		downloadOptions.OnnxFilePath = "onnx/model.onnx"
		downloadedPath, err := hugot.DownloadModel(modelName, modelDir, downloadOptions)
		if err != nil {
			return "", fmt.Errorf("failed to download model: %w", err)
		}
		modelPath = downloadedPath
	}

	return modelPath, nil
}
