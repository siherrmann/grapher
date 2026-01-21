package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadata_Marshal(t *testing.T) {
	t.Run("Marshal empty metadata", func(t *testing.T) {
		m := Metadata{}

		bytes, err := m.Marshal()

		require.NoError(t, err)
		assert.Equal(t, []byte("{}"), bytes)
	})

	t.Run("Marshal metadata with simple values", func(t *testing.T) {
		m := Metadata{
			"key1": "value1",
			"key2": 42,
			"key3": true,
		}

		bytes, err := m.Marshal()

		require.NoError(t, err)

		// Unmarshal to verify structure
		var result map[string]interface{}
		err = json.Unmarshal(bytes, &result)
		require.NoError(t, err)
		assert.Equal(t, "value1", result["key1"])
		assert.Equal(t, float64(42), result["key2"]) // JSON numbers become float64
		assert.Equal(t, true, result["key3"])
	})

	t.Run("Marshal metadata with nested objects", func(t *testing.T) {
		m := Metadata{
			"nested": map[string]interface{}{
				"inner": "value",
			},
			"array": []string{"a", "b", "c"},
		}

		bytes, err := m.Marshal()

		require.NoError(t, err)
		assert.Contains(t, string(bytes), "nested")
		assert.Contains(t, string(bytes), "array")
	})

	t.Run("Marshal nil metadata", func(t *testing.T) {
		var m Metadata = nil

		bytes, err := m.Marshal()

		require.NoError(t, err)
		assert.Equal(t, []byte("null"), bytes)
	})
}

func TestMetadata_Unmarshal(t *testing.T) {
	t.Run("Unmarshal valid JSON bytes", func(t *testing.T) {
		jsonBytes := []byte(`{"key1":"value1","key2":42,"key3":true}`)
		var m Metadata

		err := m.Unmarshal(jsonBytes)

		require.NoError(t, err)
		assert.Equal(t, "value1", m["key1"])
		assert.Equal(t, float64(42), m["key2"])
		assert.Equal(t, true, m["key3"])
	})

	t.Run("Unmarshal empty JSON object", func(t *testing.T) {
		jsonBytes := []byte(`{}`)
		var m Metadata

		err := m.Unmarshal(jsonBytes)

		require.NoError(t, err)
		assert.NotNil(t, m)
		assert.Len(t, m, 0)
	})

	t.Run("Unmarshal nil value", func(t *testing.T) {
		var m Metadata

		err := m.Unmarshal(nil)

		require.NoError(t, err)
		assert.NotNil(t, m)
		assert.Len(t, m, 0)
	})

	t.Run("Unmarshal Metadata directly", func(t *testing.T) {
		source := Metadata{
			"key": "value",
		}
		var m Metadata

		err := m.Unmarshal(source)

		require.NoError(t, err)
		assert.Equal(t, "value", m["key"])
	})

	t.Run("Unmarshal invalid JSON", func(t *testing.T) {
		invalidJSON := []byte(`{invalid json}`)
		var m Metadata

		err := m.Unmarshal(invalidJSON)

		require.Error(t, err)
	})

	t.Run("Unmarshal invalid type", func(t *testing.T) {
		var m Metadata

		err := m.Unmarshal(12345)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "type assertion")
	})

	t.Run("Unmarshal nested structures", func(t *testing.T) {
		jsonBytes := []byte(`{
			"nested": {
				"inner": "value"
			},
			"array": ["a", "b", "c"]
		}`)
		var m Metadata

		err := m.Unmarshal(jsonBytes)

		require.NoError(t, err)
		nested, ok := m["nested"].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "value", nested["inner"])
	})
}

func TestMetadata_Value(t *testing.T) {
	t.Run("Value returns marshaled JSON", func(t *testing.T) {
		m := Metadata{
			"key": "value",
		}

		value, err := m.Value()

		require.NoError(t, err)
		bytes, ok := value.([]byte)
		require.True(t, ok)

		var result map[string]interface{}
		err = json.Unmarshal(bytes, &result)
		require.NoError(t, err)
		assert.Equal(t, "value", result["key"])
	})

	t.Run("Value handles empty metadata", func(t *testing.T) {
		m := Metadata{}

		value, err := m.Value()

		require.NoError(t, err)
		assert.Equal(t, []byte("{}"), value)
	})
}

func TestMetadata_Scan(t *testing.T) {
	t.Run("Scan from JSON bytes", func(t *testing.T) {
		jsonBytes := []byte(`{"key":"value"}`)
		var m Metadata

		err := m.Scan(jsonBytes)

		require.NoError(t, err)
		assert.Equal(t, "value", m["key"])
	})

	t.Run("Scan from nil", func(t *testing.T) {
		var m Metadata

		err := m.Scan(nil)

		require.NoError(t, err)
		assert.NotNil(t, m)
		assert.Len(t, m, 0)
	})

	t.Run("Scan from Metadata", func(t *testing.T) {
		source := Metadata{"key": "value"}
		var m Metadata

		err := m.Scan(source)

		require.NoError(t, err)
		assert.Equal(t, "value", m["key"])
	})
}

func TestMetadata_RoundTrip(t *testing.T) {
	t.Run("Marshal then Unmarshal preserves data", func(t *testing.T) {
		original := Metadata{
			"string":  "value",
			"number":  42,
			"boolean": true,
			"nested": map[string]interface{}{
				"inner": "data",
			},
		}

		// Marshal
		bytes, err := original.Marshal()
		require.NoError(t, err)

		// Unmarshal
		var restored Metadata
		err = restored.Unmarshal(bytes)
		require.NoError(t, err)

		// Verify
		assert.Equal(t, "value", restored["string"])
		assert.Equal(t, float64(42), restored["number"])
		assert.Equal(t, true, restored["boolean"])

		nested, ok := restored["nested"].(map[string]interface{})
		require.True(t, ok)
		assert.Equal(t, "data", nested["inner"])
	})

	t.Run("Value then Scan preserves data", func(t *testing.T) {
		original := Metadata{
			"key": "value",
		}

		// Value
		value, err := original.Value()
		require.NoError(t, err)

		// Scan
		var restored Metadata
		err = restored.Scan(value)
		require.NoError(t, err)

		// Verify
		assert.Equal(t, "value", restored["key"])
	})
}
