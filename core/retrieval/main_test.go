package retrieval

import (
	"context"
	"log"
	"testing"

	"github.com/siherrmann/grapher/database"
	"github.com/siherrmann/grapher/helper"
	loadSql "github.com/siherrmann/grapher/sql"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

var dbPort string

func TestMain(m *testing.M) {
	var teardown func(ctx context.Context, opts ...testcontainers.TerminateOption) error
	var err error
	teardown, dbPort, err = helper.MustStartPostgresContainer()
	if err != nil {
		log.Fatalf("error starting postgres container: %v", err)
	}

	m.Run()

	if teardown != nil && teardown(context.Background()) != nil {
		log.Fatalf("error tearing down postgres container: %v", err)
	}
}

func initDB(t *testing.T) *helper.Database {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	require.NoError(t, err, "failed to create database configuration")
	db := helper.NewTestDatabase(dbConfig)

	err = loadSql.Init(db.Instance)
	require.NoError(t, err)

	return db
}

func initHandlers(t *testing.T) (*database.ChunksDBHandler, *database.EdgesDBHandler, *database.EntitiesDBHandler) {
	db := initDB(t)

	// Create all handlers
	documents, err := database.NewDocumentsDBHandler(db, true)
	require.NoError(t, err)

	edges, err := database.NewEdgesDBHandler(db, true)
	require.NoError(t, err)

	chunks, err := database.NewChunksDBHandler(db, edges, 384, true)
	require.NoError(t, err)

	entities, err := database.NewEntitiesDBHandler(db, true)
	require.NoError(t, err)

	// Note: We don't close the db here as tests will use these handlers
	// The container will be cleaned up in TestMain
	_ = documents // Keep reference to prevent unused variable error

	return chunks, edges, entities
}
