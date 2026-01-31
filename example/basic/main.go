package main

import (
	"context"
	"fmt"
	"log"

	"github.com/siherrmann/grapher"
	"github.com/siherrmann/grapher/helper"
	"github.com/siherrmann/grapher/model"
)

const sampleContent = `This is a sample document about graph databases.

Graph databases are designed to store and query data with complex relationships.
They use nodes to represent entities and edges to represent relationships between them.

PostgreSQL with extensions like ltree and pgvector can be used to build powerful graph-based systems.
The ltree extension provides hierarchical tree structures, while pgvector enables vector similarity search.

Combining these features allows for hybrid retrieval strategies that leverage both semantic similarity
and graph structure for more sophisticated information retrieval.`

func main() {
	// Start a test PostgreSQL container
	teardown, dbPort, err := helper.MustStartPostgresContainer()
	if err != nil {
		log.Fatalf("Failed to start PostgreSQL container: %v", err)
	}
	defer teardown(context.Background())

	// Create database configuration using the container port
	dbConfig := &helper.DatabaseConfiguration{
		Host:     "localhost",
		Port:     dbPort,
		Database: "database",
		Username: "user",
		Password: "password",
		Schema:   "public",
		SSLMode:  "disable",
	}

	g, err := grapher.NewGrapher(dbConfig, 384)
	if err != nil {
		log.Fatalf("Failed to create grapher: %v", err)
	}
	defer g.Close()

	// Set up the default pipeline (semantic chunking + embeddings)
	if err := g.UseDefaultPipeline(); err != nil {
		log.Fatalf("Failed to set up pipeline: %v", err)
	}

	// Create document with content - simplified API
	doc := &model.Document{
		Title:   "Introduction to Graph Databases",
		Source:  "basic_example",
		Content: sampleContent,
		Metadata: model.Metadata{
			"author": "Example Author",
			"topic":  "graph databases",
		},
	}

	// Process and insert document in one call
	fmt.Println("Ingesting document...")
	numChunks, err := g.ProcessAndInsertDocument(doc)
	if err != nil {
		log.Fatalf("Failed to process and insert document: %v", err)
	}
	fmt.Printf("Document inserted with ID: %s\n", doc.RID)
	fmt.Printf("Inserted %d chunks\n", numChunks)

	// Perform a simple vector search using the new Grapher methods
	queryText := "What are graph databases?"

	fmt.Printf("\nQuerying: %s\n", queryText)

	// Use the new Search method from Grapher (takes query string directly)
	config := model.DefaultQueryConfig()
	config.TopK = 5
	config.SimilarityThreshold = 0.0

	results, err := g.Search(context.Background(), queryText, &config)
	if err != nil {
		log.Fatalf("Failed to search: %v", err)
	}

	// Display results
	fmt.Printf("\nFound %d results:\n", len(results))
	for i, result := range results {
		fmt.Printf("\n--- Result %d ---\n", i+1)
		fmt.Printf("Score: %.4f\n", result.Score)
		fmt.Printf("Content: %s\n", result.Content)
		fmt.Printf("Path: %s\n", result.Path)
		fmt.Printf("Method: %s\n", result.RetrievalMethod)
	}

	fmt.Println("\nBasic example completed successfully!")
}
