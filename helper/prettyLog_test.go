package helper

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewPrettyHandler(t *testing.T) {
	t.Run("Create PrettyHandler with default options", func(t *testing.T) {
		var buf bytes.Buffer
		opts := PrettyHandlerOptions{
			SlogOpts: slog.HandlerOptions{},
		}

		handler := NewPrettyHandler(&buf, opts)

		assert.NotNil(t, handler, "Expected NewPrettyHandler to return a non-nil handler")
		assert.NotNil(t, handler.Handler, "Expected handler to have a non-nil Handler field")
		assert.NotNil(t, handler.l, "Expected handler to have a non-nil logger field")
	})

	t.Run("Create PrettyHandler with custom level", func(t *testing.T) {
		var buf bytes.Buffer
		opts := PrettyHandlerOptions{
			SlogOpts: slog.HandlerOptions{
				Level: slog.LevelDebug,
			},
		}

		handler := NewPrettyHandler(&buf, opts)

		assert.NotNil(t, handler, "Expected NewPrettyHandler to return a non-nil handler")
	})

	t.Run("Create PrettyHandler with AddSource option", func(t *testing.T) {
		var buf bytes.Buffer
		opts := PrettyHandlerOptions{
			SlogOpts: slog.HandlerOptions{
				AddSource: true,
			},
		}

		handler := NewPrettyHandler(&buf, opts)

		assert.NotNil(t, handler, "Expected NewPrettyHandler to return a non-nil handler")
	})
}

func TestPrettyHandlerHandle(t *testing.T) {
	ctx := context.Background()

	t.Run("Handle DEBUG level log", func(t *testing.T) {
		var buf bytes.Buffer
		opts := PrettyHandlerOptions{
			SlogOpts: slog.HandlerOptions{
				Level: slog.LevelDebug,
			},
		}
		handler := NewPrettyHandler(&buf, opts)

		record := slog.NewRecord(time.Now(), slog.LevelDebug, "debug message", 0)
		record.AddAttrs(slog.String("key", "value"))

		err := handler.Handle(ctx, record)

		assert.NoError(t, err, "Expected Handle to not return an error")
		output := buf.String()
		assert.Contains(t, output, "DEBUG:", "Expected output to contain DEBUG level")
		assert.Contains(t, output, "debug message", "Expected output to contain the message")
		assert.Contains(t, output, "key", "Expected output to contain attribute key")
		assert.Contains(t, output, "value", "Expected output to contain attribute value")
	})

	t.Run("Handle INFO level log", func(t *testing.T) {
		var buf bytes.Buffer
		opts := PrettyHandlerOptions{
			SlogOpts: slog.HandlerOptions{},
		}
		handler := NewPrettyHandler(&buf, opts)

		record := slog.NewRecord(time.Now(), slog.LevelInfo, "info message", 0)
		record.AddAttrs(slog.Int("count", 42))

		err := handler.Handle(ctx, record)

		assert.NoError(t, err, "Expected Handle to not return an error")
		output := buf.String()
		assert.Contains(t, output, "INFO:", "Expected output to contain INFO level")
		assert.Contains(t, output, "info message", "Expected output to contain the message")
		assert.Contains(t, output, "count", "Expected output to contain attribute key")
		assert.Contains(t, output, "42", "Expected output to contain attribute value")
	})

	t.Run("Handle WARN level log", func(t *testing.T) {
		var buf bytes.Buffer
		opts := PrettyHandlerOptions{
			SlogOpts: slog.HandlerOptions{},
		}
		handler := NewPrettyHandler(&buf, opts)

		record := slog.NewRecord(time.Now(), slog.LevelWarn, "warning message", 0)
		record.AddAttrs(slog.Bool("flag", true))

		err := handler.Handle(ctx, record)

		assert.NoError(t, err, "Expected Handle to not return an error")
		output := buf.String()
		assert.Contains(t, output, "WARN:", "Expected output to contain WARN level")
		assert.Contains(t, output, "warning message", "Expected output to contain the message")
		assert.Contains(t, output, "flag", "Expected output to contain attribute key")
		assert.Contains(t, output, "true", "Expected output to contain attribute value")
	})

	t.Run("Handle ERROR level log", func(t *testing.T) {
		var buf bytes.Buffer
		opts := PrettyHandlerOptions{
			SlogOpts: slog.HandlerOptions{},
		}
		handler := NewPrettyHandler(&buf, opts)

		record := slog.NewRecord(time.Now(), slog.LevelError, "error message", 0)
		record.AddAttrs(slog.String("error", "something went wrong"))

		err := handler.Handle(ctx, record)

		assert.NoError(t, err, "Expected Handle to not return an error")
		output := buf.String()
		assert.Contains(t, output, "ERROR:", "Expected output to contain ERROR level")
		assert.Contains(t, output, "error message", "Expected output to contain the message")
		assert.Contains(t, output, "error", "Expected output to contain attribute key")
		assert.Contains(t, output, "something went wrong", "Expected output to contain attribute value")
	})

	t.Run("Handle log with no attributes", func(t *testing.T) {
		var buf bytes.Buffer
		opts := PrettyHandlerOptions{
			SlogOpts: slog.HandlerOptions{},
		}
		handler := NewPrettyHandler(&buf, opts)

		record := slog.NewRecord(time.Now(), slog.LevelInfo, "simple message", 0)

		err := handler.Handle(ctx, record)

		assert.NoError(t, err, "Expected Handle to not return an error")
		output := buf.String()
		assert.Contains(t, output, "INFO:", "Expected output to contain INFO level")
		assert.Contains(t, output, "simple message", "Expected output to contain the message")
		assert.Contains(t, output, "{}", "Expected output to contain empty JSON object for attributes")
	})

	t.Run("Handle log with multiple attributes", func(t *testing.T) {
		var buf bytes.Buffer
		opts := PrettyHandlerOptions{
			SlogOpts: slog.HandlerOptions{},
		}
		handler := NewPrettyHandler(&buf, opts)

		record := slog.NewRecord(time.Now(), slog.LevelInfo, "multi-attr message", 0)
		record.AddAttrs(
			slog.String("name", "test"),
			slog.Int("id", 123),
			slog.Bool("active", true),
		)

		err := handler.Handle(ctx, record)

		assert.NoError(t, err, "Expected Handle to not return an error")
		output := buf.String()
		assert.Contains(t, output, "multi-attr message", "Expected output to contain the message")
		assert.Contains(t, output, "name", "Expected output to contain first attribute")
		assert.Contains(t, output, "test", "Expected output to contain first attribute value")
		assert.Contains(t, output, "id", "Expected output to contain second attribute")
		assert.Contains(t, output, "123", "Expected output to contain second attribute value")
		assert.Contains(t, output, "active", "Expected output to contain third attribute")
		assert.Contains(t, output, "true", "Expected output to contain third attribute value")
	})

	t.Run("Handle log with nested attributes", func(t *testing.T) {
		var buf bytes.Buffer
		opts := PrettyHandlerOptions{
			SlogOpts: slog.HandlerOptions{},
		}
		handler := NewPrettyHandler(&buf, opts)

		record := slog.NewRecord(time.Now(), slog.LevelInfo, "nested message", 0)
		record.AddAttrs(slog.Any("metadata", map[string]interface{}{
			"nested_key": "nested_value",
		}))

		err := handler.Handle(ctx, record)

		assert.NoError(t, err, "Expected Handle to not return an error")
		output := buf.String()
		assert.Contains(t, output, "nested message", "Expected output to contain the message")
		assert.Contains(t, output, "metadata", "Expected output to contain attribute key")
	})

	t.Run("Handle log formats timestamp correctly", func(t *testing.T) {
		var buf bytes.Buffer
		opts := PrettyHandlerOptions{
			SlogOpts: slog.HandlerOptions{},
		}
		handler := NewPrettyHandler(&buf, opts)

		now := time.Now()
		record := slog.NewRecord(now, slog.LevelInfo, "time test", 0)

		err := handler.Handle(ctx, record)

		assert.NoError(t, err, "Expected Handle to not return an error")
		output := buf.String()

		// Check that output contains timestamp in format [HH:MM:SS.mmm]
		assert.True(t, strings.Contains(output, "[") && strings.Contains(output, "]"),
			"Expected output to contain timestamp in brackets")
		// Timestamp should be in format [15:04:05.000]
		assert.Regexp(t, `\[\d{2}:\d{2}:\d{2}\.\d{3}\]`, output,
			"Expected output to contain properly formatted timestamp")
	})

	t.Run("Handle log with context", func(t *testing.T) {
		var buf bytes.Buffer
		opts := PrettyHandlerOptions{
			SlogOpts: slog.HandlerOptions{},
		}
		handler := NewPrettyHandler(&buf, opts)

		// Create context with value
		ctx := context.WithValue(context.Background(), "request_id", "12345")

		record := slog.NewRecord(time.Now(), slog.LevelInfo, "context message", 0)

		err := handler.Handle(ctx, record)

		assert.NoError(t, err, "Expected Handle to not return an error")
		output := buf.String()
		assert.Contains(t, output, "context message", "Expected output to contain the message")
	})
}

func TestPrettyHandlerOptions(t *testing.T) {
	t.Run("PrettyHandlerOptions with all fields set", func(t *testing.T) {
		opts := PrettyHandlerOptions{
			SlogOpts: slog.HandlerOptions{
				AddSource: true,
				Level:     slog.LevelDebug,
				ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
					return a
				},
			},
		}

		var buf bytes.Buffer
		handler := NewPrettyHandler(&buf, opts)

		assert.NotNil(t, handler, "Expected handler to be created with all options set")
	})

	t.Run("PrettyHandlerOptions with empty options", func(t *testing.T) {
		opts := PrettyHandlerOptions{}

		var buf bytes.Buffer
		handler := NewPrettyHandler(&buf, opts)

		assert.NotNil(t, handler, "Expected handler to be created with empty options")
	})
}
