package database

import (
	"testing"
	"time"

	"github.com/siherrmann/grapher/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEntitiesNewEntitiesDBHandler(t *testing.T) {
	database := initDB(t)

	t.Run("Valid call NewEntitiesDBHandler", func(t *testing.T) {
		entitiesDbHandler, err := NewEntitiesDBHandler(database, true)
		assert.NoError(t, err, "Expected NewEntitiesDBHandler to not return an error")
		require.NotNil(t, entitiesDbHandler, "Expected NewEntitiesDBHandler to return a non-nil instance")
		require.NotNil(t, entitiesDbHandler.db, "Expected NewEntitiesDBHandler to have a non-nil database instance")
		require.NotNil(t, entitiesDbHandler.db.Instance, "Expected NewEntitiesDBHandler to have a non-nil database connection instance")
	})

	t.Run("Invalid call NewEntitiesDBHandler with nil database", func(t *testing.T) {
		_, err := NewEntitiesDBHandler(nil, false)
		assert.Error(t, err, "Expected error when creating EntitiesDBHandler with nil database")
		assert.Contains(t, err.Error(), "database connection is nil", "Expected specific error message for nil database connection")
	})
}

func TestEntitiesInsert(t *testing.T) {
	database := initDB(t)

	entitiesDbHandler, err := NewEntitiesDBHandler(database, true)
	require.NoError(t, err, "Expected NewEntitiesDBHandler to not return an error")

	t.Run("Insert entity", func(t *testing.T) {
		entity := &model.Entity{
			Name:     "John Doe",
			Type:     "PERSON",
			Metadata: map[string]interface{}{"occupation": "Engineer"},
		}

		err := entitiesDbHandler.InsertEntity(entity)
		assert.NoError(t, err, "Expected Insert to not return an error")
		assert.NotEmpty(t, entity.ID, "Expected inserted entity to have an ID")
		assert.WithinDuration(t, entity.CreatedAt, time.Now(), 2*time.Second, "Expected CreatedAt to be set")

		// Cleanup
		entitiesDbHandler.DeleteEntity(entity.ID)
	})

	t.Run("Insert duplicate entity (upsert)", func(t *testing.T) {
		entity := &model.Entity{
			Name:     "Jane Smith",
			Type:     "PERSON",
			Metadata: map[string]interface{}{"age": 30},
		}

		err := entitiesDbHandler.InsertEntity(entity)
		require.NoError(t, err)
		firstID := entity.ID

		// Insert again with same name and type
		entity2 := &model.Entity{
			Name:     "Jane Smith",
			Type:     "PERSON",
			Metadata: map[string]interface{}{"age": 31},
		}

		err = entitiesDbHandler.InsertEntity(entity2)
		assert.NoError(t, err, "Expected Insert to not return an error for duplicate")
		// Depending on implementation, this might update or create new - verify behavior
		assert.NotEmpty(t, entity2.ID, "Expected entity to have an ID")

		// Cleanup
		entitiesDbHandler.DeleteEntity(firstID)
		if entity2.ID != firstID {
			entitiesDbHandler.DeleteEntity(entity2.ID)
		}
	})
}

func TestEntitiesGet(t *testing.T) {
	database := initDB(t)

	entitiesDbHandler, err := NewEntitiesDBHandler(database, true)
	require.NoError(t, err)

	// Create an entity
	entity := &model.Entity{
		Name:     "Test Entity",
		Type:     "ORGANIZATION",
		Metadata: map[string]interface{}{"founded": 2020},
	}
	err = entitiesDbHandler.InsertEntity(entity)
	require.NoError(t, err)

	// Test Get
	retrievedEntity, err := entitiesDbHandler.SelectEntity(entity.ID)
	assert.NoError(t, err, "Expected Get to not return an error")
	assert.NotNil(t, retrievedEntity, "Expected Get to return a non-nil entity")
	assert.Equal(t, entity.ID, retrievedEntity.ID, "Expected entity IDs to match")
	assert.Equal(t, entity.Name, retrievedEntity.Name, "Expected names to match")
	assert.Equal(t, entity.Type, retrievedEntity.Type, "Expected types to match")

	// Cleanup
	entitiesDbHandler.DeleteEntity(entity.ID)
}

func TestEntitiesGetByName(t *testing.T) {
	database := initDB(t)

	entitiesDbHandler, err := NewEntitiesDBHandler(database, true)
	require.NoError(t, err)

	// Create an entity
	entity := &model.Entity{
		Name:     "Unique Entity Name",
		Type:     "LOCATION",
		Metadata: map[string]interface{}{},
	}
	err = entitiesDbHandler.InsertEntity(entity)
	require.NoError(t, err)

	// Test GetByName
	retrievedEntity, err := entitiesDbHandler.SelectEntityByName("Unique Entity Name", "LOCATION")
	assert.NoError(t, err, "Expected GetByName to not return an error")
	assert.NotNil(t, retrievedEntity, "Expected GetByName to return a non-nil entity")
	assert.Equal(t, entity.ID, retrievedEntity.ID, "Expected entity IDs to match")

	// Cleanup
	entitiesDbHandler.DeleteEntity(entity.ID)
}

func TestEntitiesSearch(t *testing.T) {
	database := initDB(t)

	entitiesDbHandler, err := NewEntitiesDBHandler(database, true)
	require.NoError(t, err)

	// Create entities with different names
	searchTerm := "SearchableEntity"
	matchingCount := 3
	otherCount := 2

	entities := []*model.Entity{}

	for i := 0; i < matchingCount; i++ {
		entity := &model.Entity{
			Name:     searchTerm + " " + string(rune('A'+i)),
			Type:     "PERSON",
			Metadata: map[string]interface{}{},
		}
		err = entitiesDbHandler.InsertEntity(entity)
		require.NoError(t, err)
		entities = append(entities, entity)
	}

	for i := 0; i < otherCount; i++ {
		entity := &model.Entity{
			Name:     "Other Entity " + string(rune('A'+i)),
			Type:     "PERSON",
			Metadata: map[string]interface{}{},
		}
		err = entitiesDbHandler.InsertEntity(entity)
		require.NoError(t, err)
		entities = append(entities, entity)
	}

	// Test Search
	results, err := entitiesDbHandler.SelectEntitiesBySearch(searchTerm, nil, 10)
	assert.NoError(t, err, "Expected Search to not return an error")
	assert.GreaterOrEqual(t, len(results), matchingCount, "Expected to find at least matching entities")

	// Test Search with type filter
	personType := "PERSON"
	resultsWithType, err := entitiesDbHandler.SelectEntitiesBySearch(searchTerm, &personType, 10)
	assert.NoError(t, err, "Expected Search with type to not return an error")
	assert.GreaterOrEqual(t, len(resultsWithType), matchingCount, "Expected to find matching entities with type filter")

	// Cleanup
	for _, entity := range entities {
		entitiesDbHandler.DeleteEntity(entity.ID)
	}
}

func TestEntitiesGetByType(t *testing.T) {
	database := initDB(t)

	entitiesDbHandler, err := NewEntitiesDBHandler(database, true)
	require.NoError(t, err)

	// Create entities of different types
	entityType := "CONCEPT"
	entityCount := 4

	entities := []*model.Entity{}

	for i := 0; i < entityCount; i++ {
		entity := &model.Entity{
			Name:     "Concept " + string(rune('A'+i)),
			Type:     entityType,
			Metadata: map[string]interface{}{},
		}
		err = entitiesDbHandler.InsertEntity(entity)
		require.NoError(t, err)
		entities = append(entities, entity)
	}

	// Test GetByType
	results, err := entitiesDbHandler.SelectEntitiesByType(entityType, 10)
	assert.NoError(t, err, "Expected GetByType to not return an error")
	assert.GreaterOrEqual(t, len(results), entityCount, "Expected to find all entities of type")

	// Cleanup
	for _, entity := range entities {
		entitiesDbHandler.DeleteEntity(entity.ID)
	}
}

func TestEntitiesDelete(t *testing.T) {
	database := initDB(t)

	entitiesDbHandler, err := NewEntitiesDBHandler(database, true)
	require.NoError(t, err)

	// Create an entity
	entity := &model.Entity{
		Name:     "To Delete",
		Type:     "TEST",
		Metadata: map[string]interface{}{},
	}
	err = entitiesDbHandler.InsertEntity(entity)
	require.NoError(t, err)

	// Delete the entity
	err = entitiesDbHandler.DeleteEntity(entity.ID)
	assert.NoError(t, err, "Expected Delete to not return an error")

	// Verify deletion
	_, err = entitiesDbHandler.SelectEntity(entity.ID)
	assert.Error(t, err, "Expected Get to return an error for deleted entity")
}

func TestEntitiesUpdateMetadata(t *testing.T) {
	database := initDB(t)

	entitiesDbHandler, err := NewEntitiesDBHandler(database, true)
	require.NoError(t, err)

	// Create an entity
	entity := &model.Entity{
		Name:     "Test Entity",
		Type:     "PERSON",
		Metadata: model.Metadata{"status": "active"},
	}
	err = entitiesDbHandler.InsertEntity(entity)
	require.NoError(t, err)

	// Update metadata
	newMetadata := model.Metadata{"status": "inactive", "reason": "test"}
	err = entitiesDbHandler.UpdateEntityMetadata(entity.ID, newMetadata)
	assert.NoError(t, err, "Expected UpdateMetadata to not return an error")

	// Verify update
	retrievedEntity, err := entitiesDbHandler.SelectEntity(entity.ID)
	require.NoError(t, err)
	assert.Equal(t, "inactive", retrievedEntity.Metadata["status"], "Expected metadata to be updated")
	assert.Equal(t, "test", retrievedEntity.Metadata["reason"], "Expected new metadata field")

	// Cleanup
	entitiesDbHandler.DeleteEntity(entity.ID)
}
