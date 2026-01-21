package sql

import (
	"database/sql"
	_ "embed"
	"fmt"
	"log"
)

//go:embed init.sql
var initSQL string

//go:embed chunks.sql
var chunksSQL string

//go:embed documents.sql
var documentsSQL string

//go:embed edges.sql
var edgesSQL string

//go:embed entities.sql
var entitiesSQL string

// Function lists for verification
var ChunksFunctions = []string{
	"init_chunks",
	"insert_chunk",
	"select_chunk",
	"select_chunks_by_document",
	"select_chunks_by_path_descendant",
	"select_chunks_by_path_ancestor",
	"select_chunks_by_similarity",
	"select_chunks_by_similarity_with_context",
	"delete_chunk",
	"update_chunk_embedding",
}

var DocumentsFunctions = []string{
	"init_documents",
	"insert_document",
	"select_document",
	"select_all_documents",
	"search_documents",
	"update_document",
	"delete_document",
}

var EdgesFunctions = []string{
	"init_edges",
	"insert_edge",
	"select_edge",
	"select_edges_from_chunk",
	"select_edges_to_chunk",
	"select_edges_connected_to_chunk",
	"select_edges_from_entity",
	"select_edges_to_entity",
	"delete_edge",
	"update_edge_weight",
	"traverse_bfs_from_chunk",
}

var EntitiesFunctions = []string{
	"init_entities",
	"insert_entity",
	"select_entity",
	"select_entity_by_name",
	"search_entities",
	"select_entities_by_type",
	"delete_entity",
	"update_entity_metadata",
	"select_chunks_mentioning_entity",
}

// Init intializes db extensions
func Init(db *sql.DB) error {
	_, err := db.Exec(initSQL)
	if err != nil {
		return fmt.Errorf("error executing schema SQL: %w", err)
	}

	log.Println("Database extensions initialized successfully")
	return nil
}

// LoadChunksSql loads chunk-related SQL functions
func LoadChunksSql(db *sql.DB, force bool) error {
	if !force {
		exist, err := checkFunctions(db, ChunksFunctions)
		if err != nil {
			return fmt.Errorf("error checking existing chunks functions: %w", err)
		}
		if exist {
			return nil
		}
	}

	_, err := db.Exec(chunksSQL)
	if err != nil {
		return fmt.Errorf("error executing chunks SQL: %w", err)
	}

	exist, err := checkFunctions(db, ChunksFunctions)
	if err != nil {
		return fmt.Errorf("error checking existing functions: %w", err)
	}
	if !exist {
		return fmt.Errorf("not all required SQL functions were created")
	}

	log.Println("SQL chunks functions loaded successfully")
	return nil
}

// LoadDocumentsSql loads document-related SQL functions
func LoadDocumentsSql(db *sql.DB, force bool) error {
	if !force {
		exist, err := checkFunctions(db, DocumentsFunctions)
		if err != nil {
			return fmt.Errorf("error checking existing documents functions: %w", err)
		}
		if exist {
			return nil
		}
	}

	_, err := db.Exec(documentsSQL)
	if err != nil {
		return fmt.Errorf("error executing documents SQL: %w", err)
	}

	exist, err := checkFunctions(db, DocumentsFunctions)
	if err != nil {
		return fmt.Errorf("error checking existing functions: %w", err)
	}
	if !exist {
		return fmt.Errorf("not all required SQL functions were created")
	}

	log.Println("SQL documents functions loaded successfully")
	return nil
}

// LoadEdgesSql loads edge-related SQL functions
func LoadEdgesSql(db *sql.DB, force bool) error {
	if !force {
		exist, err := checkFunctions(db, EdgesFunctions)
		if err != nil {
			return fmt.Errorf("error checking existing edges functions: %w", err)
		}
		if exist {
			return nil
		}
	}

	_, err := db.Exec(edgesSQL)
	if err != nil {
		return fmt.Errorf("error executing edges SQL: %w", err)
	}

	exist, err := checkFunctions(db, EdgesFunctions)
	if err != nil {
		return fmt.Errorf("error checking existing functions: %w", err)
	}
	if !exist {
		return fmt.Errorf("not all required SQL functions were created")
	}

	log.Println("SQL edges functions loaded successfully")
	return nil
}

// LoadEntitiesSql loads entity-related SQL functions
func LoadEntitiesSql(db *sql.DB, force bool) error {
	if !force {
		exist, err := checkFunctions(db, EntitiesFunctions)
		if err != nil {
			return fmt.Errorf("error checking existing entities functions: %w", err)
		}
		if exist {
			return nil
		}
	}

	_, err := db.Exec(entitiesSQL)
	if err != nil {
		return fmt.Errorf("error executing entities SQL: %w", err)
	}

	exist, err := checkFunctions(db, EntitiesFunctions)
	if err != nil {
		return fmt.Errorf("error checking existing functions: %w", err)
	}
	if !exist {
		return fmt.Errorf("not all required SQL functions were created")
	}

	log.Println("SQL entities functions loaded successfully")
	return nil
}

// LoadAllSql loads all SQL functions
func LoadAllSql(db *sql.DB, force bool) error {
	if err := LoadChunksSql(db, force); err != nil {
		return err
	}

	if err := LoadDocumentsSql(db, force); err != nil {
		return err
	}

	if err := LoadEdgesSql(db, force); err != nil {
		return err
	}

	if err := LoadEntitiesSql(db, force); err != nil {
		return err
	}

	return nil
}

// checkFunctions verifies that all required functions exist in the database
func checkFunctions(db *sql.DB, sqlFunctions []string) (bool, error) {
	var allExist bool
	for _, f := range sqlFunctions {
		err := db.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = $1);`,
			f,
		).Scan(&allExist)
		if err != nil {
			return false, fmt.Errorf("error checking existence of function %s: %w", f, err)
		}
		if !allExist {
			log.Printf("Function %s does not exist", f)
			break
		}
	}
	return allExist, nil
}
