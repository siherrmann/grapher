package main

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/siherrmann/grapher"
	"github.com/siherrmann/grapher/helper"
	"github.com/siherrmann/grapher/model"
)

const sampleContent1 = `This is a comprehensive document about graph databases and their applications.

Graph databases are designed to store and query data with complex relationships.
They use nodes to represent entities and edges to represent relationships between them.

PostgreSQL with extensions like ltree and pgvector can be used to build powerful graph-based systems.
The ltree extension provides hierarchical tree structures, while pgvector enables vector similarity search.

Combining these features allows for hybrid retrieval strategies that leverage both semantic similarity
and graph structure for more sophisticated information retrieval.`

const sampleContent2 = `Machine learning is transforming how we process and retrieve information.

Vector embeddings capture semantic meaning of text, enabling similarity-based search.
Neural networks can learn representations that understand context and relationships.

Modern retrieval systems combine traditional database indexing with machine learning models
to provide more intelligent and context-aware search capabilities.`

func main() {
	// Start a test PostgreSQL container
	teardown, dbPort, err := helper.MustStartPostgresContainer()
	if err != nil {
		log.Fatalf("Failed to start PostgreSQL container: %v", err)
	}
	defer teardown(context.Background())

	// Create database configuration
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

	// Process and insert multiple documents
	doc1 := &model.Document{
		Title:   "Introduction to Graph Databases",
		Source:  "advanced_example",
		Content: sampleContent1,
		Metadata: model.Metadata{
			"author": "Example Author",
			"topic":  "graph databases",
		},
	}

	doc2 := &model.Document{
		Title:   "Machine Learning for Information Retrieval",
		Source:  "advanced_example",
		Content: sampleContent2,
		Metadata: model.Metadata{
			"author": "Example Author",
			"topic":  "machine learning",
		},
	}

	fmt.Println("=== Ingesting Documents ===")
	numChunks1, err := g.ProcessAndInsertDocument(doc1)
	if err != nil {
		log.Fatalf("Failed to process and insert document 1: %v", err)
	}
	fmt.Printf("Document 1 '%s' (RID: %s): %d chunks\n", doc1.Title, doc1.RID, numChunks1)

	numChunks2, err := g.ProcessAndInsertDocument(doc2)
	if err != nil {
		log.Fatalf("Failed to process and insert document 2: %v", err)
	}
	fmt.Printf("Document 2 '%s' (RID: %s): %d chunks\n", doc2.Title, doc2.RID, numChunks2)

	// Prepare query
	queryText := "What are graph databases?"

	ctx := context.Background()

	// 1. Vector-only search
	fmt.Println("\n=== 1. Vector-Only Search ===")
	vectorConfig := model.DefaultQueryConfig()
	vectorConfig.TopK = 3
	vectorResults, err := g.Search(ctx, queryText, &vectorConfig)
	if err != nil {
		log.Fatalf("Vector search failed: %v", err)
	}
	printResults("Vector Search", vectorResults)

	// 2. Contextual search (vector + neighbors + hierarchy)
	fmt.Println("\n=== 2. Contextual Search ===")
	contextConfig := model.DefaultQueryConfig()
	contextConfig.TopK = 3
	contextConfig.IncludeSiblings = true
	contextResults, err := g.ContextualSearch(ctx, queryText, &contextConfig)
	if err != nil {
		log.Fatalf("Contextual search failed: %v", err)
	}
	printResults("Contextual Search", contextResults)

	// 3. Multi-hop search
	fmt.Println("\n=== 3. Multi-Hop Search ===")
	multiHopConfig := model.DefaultQueryConfig()
	multiHopConfig.TopK = 3
	multiHopConfig.MaxHops = 2
	multiHopResults, err := g.MultiHopSearch(ctx, queryText, &multiHopConfig)
	if err != nil {
		log.Fatalf("Multi-hop search failed: %v", err)
	}
	printResults("Multi-Hop Search", multiHopResults)

	// 4. Hybrid search with custom weights
	fmt.Println("\n=== 4. Hybrid Search (Custom Weights) ===")
	hybridConfig := model.DefaultQueryConfig()
	hybridConfig.TopK = 5
	hybridConfig.VectorWeight = 0.5
	hybridConfig.GraphWeight = 0.3
	hybridConfig.HierarchyWeight = 0.2
	hybridConfig.MaxHops = 1
	hybridConfig.IncludeSiblings = true
	hybridResults, err := g.HybridSearch(ctx, queryText, &hybridConfig)
	if err != nil {
		log.Fatalf("Hybrid search failed: %v", err)
	}
	printResults("Hybrid Search", hybridResults)

	// 5. Document-scoped search
	fmt.Println("\n=== 5. Document-Scoped Search ===")
	fmt.Println("Searching only within 'Introduction to Graph Databases'...")
	docScopedConfig := model.DefaultQueryConfig()
	docScopedConfig.TopK = 3
	docScopedResults, err := g.DocumentScopedSearch(ctx, queryText, []uuid.UUID{doc1.RID}, &docScopedConfig)
	if err != nil {
		log.Fatalf("Document-scoped search failed: %v", err)
	}
	printResults("Document-Scoped Search", docScopedResults)

	fmt.Println("\nSearching only within 'Machine Learning for Information Retrieval'...")
	mlQuery := "How does machine learning help with search?"
	mlScopedResults, err := g.DocumentScopedSearch(ctx, mlQuery, []uuid.UUID{doc2.RID}, &docScopedConfig)
	if err != nil {
		log.Fatalf("ML document-scoped search failed: %v", err)
	}
	printResults("ML Document Search", mlScopedResults)

	// 6. Demonstrate index type switching
	fmt.Println("\n=== 6. Changing Index Type ===")
	fmt.Println("Switching to IVFFlat index...")
	err = g.ChangeIndexType(ctx, "ivfflat", map[string]interface{}{
		"lists": 100,
	})
	if err != nil {
		log.Printf("Warning: Index change failed (this is okay for small datasets): %v", err)
	} else {
		fmt.Println("Successfully switched to IVFFlat index")
	}

	// Switch back to HNSW
	fmt.Println("Switching back to HNSW index...")
	err = g.ChangeIndexType(ctx, "hnsw", map[string]interface{}{
		"m":               16,
		"ef_construction": 64,
	})
	if err != nil {
		log.Printf("Warning: Index change failed: %v", err)
	} else {
		fmt.Println("Successfully switched to HNSW index")
	}

	// 7. Demonstrate graph traversal
	if len(vectorResults) > 0 {
		fmt.Println("\n=== 7. Graph Traversal (BFS) ===")
		sourceChunkID := vectorResults[0].ID
		fmt.Printf("Starting BFS from chunk: %d\n", sourceChunkID)

		traversalResults, err := g.BFSTraversal(ctx, sourceChunkID, 2, []model.EdgeType{}, true)
		if err != nil {
			log.Printf("BFS traversal failed: %v", err)
		} else {
			fmt.Printf("Found %d nodes via BFS traversal\n", len(traversalResults))
			for i, tr := range traversalResults {
				if i >= 3 {
					break // Show only first 3
				}
				fmt.Printf("  - Distance %d: %s (path length: %d)\n",
					tr.Distance, tr.Chunk.Path, len(tr.Path))
			}
		}
	}

	fmt.Println("\n=== Advanced Example Completed Successfully! ===")
	fmt.Println("\nKey features demonstrated:")
	fmt.Println("✓ Vector-only search")
	fmt.Println("✓ Contextual search (vector + neighbors + hierarchy)")
	fmt.Println("✓ Multi-hop graph traversal")
	fmt.Println("✓ Hybrid search with custom weights")
	fmt.Println("✓ Document-scoped search (filter by document RID)")
	fmt.Println("✓ Index type switching (HNSW ↔ IVFFlat)")
	fmt.Println("✓ BFS graph traversal")
}

func printResults(title string, results []*model.Chunk) {
	fmt.Printf("\n%s - Found %d results:\n", title, len(results))
	for i, result := range results {
		if i >= 3 {
			break // Show only first 3
		}
		fmt.Printf("\n  Result %d:\n", i+1)
		fmt.Printf("    Score: %.4f (similarity: %.4f, graph dist: %d)\n",
			result.Score, result.Similarity, result.Distance)
		fmt.Printf("    Method: %s\n", result.RetrievalMethod)
		fmt.Printf("    Path: %s\n", result.Path)
		content := result.Content
		if len(content) > 80 {
			content = content[:80] + "..."
		}
		fmt.Printf("    Content: %s\n", content)
	}
}
