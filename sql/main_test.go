package sql

import (
	"context"
	"log"
	"testing"

	"github.com/siherrmann/grapher/helper"
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
	database := helper.NewTestDatabase(dbConfig)

	err = Init(database.Instance)
	require.NoError(t, err)

	return database
}
