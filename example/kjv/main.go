package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/siherrmann/grapher"
	"github.com/siherrmann/grapher/helper"
	"github.com/siherrmann/grapher/model"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const kjvRepoURL = "https://raw.githubusercontent.com/arleym/kjv-markdown/master"

// List of KJV books to download
var kjvBooks = []string{
	"01 - Genesis - KJV.md",
	// "02 - Exodus - KJV.md", "03 - Leviticus - KJV.md",
	// "04 - Numbers - KJV.md", "05 - Deuteronomy - KJV.md",
	// "06 - Joshua - KJV.md", "07 - Judges - KJV.md", "08 - Ruth - KJV.md",
	// "09 - 1 Samuel - KJV.md", "10 - 2 Samuel - KJV.md",
	// "11 - 1 Kings - KJV.md", "12 - 2 Kings - KJV.md",
	// "13 - 1 Chronicles - KJV.md", "14 - 2 Chronicles - KJV.md",
	// "15 - Ezra - KJV.md", "16 - Nehemiah - KJV.md", "17 - Esther - KJV.md",
	// "18 - Job - KJV.md", "19 - Psalms - KJV.md",
	// "20 - Proverbs - KJV.md", "21 - Ecclesiastes - KJV.md",
	// "22 - The Song of Solomon - KJV.md", "23 - Isaiah - KJV.md",
	// "24 - Jeremiah - KJV.md", "25 - Lamentations - KJV.md",
	// "26 - Ezekiel - KJV.md", "27 - Daniel - KJV.md",
	// "28 - Hosea - KJV.md", "29 - Joel - KJV.md", "30 - Amos - KJV.md",
	// "31 - Obadiah - KJV.md", "32 - Jonah - KJV.md",
	// "33 - Micah - KJV.md", "34 - Nahum - KJV.md", "35 - Habakkuk - KJV.md",
	// "36 - Zephaniah - KJV.md", "37 - Haggai - KJV.md",
	// "38 - Zechariah - KJV.md", "39 - Malachi - KJV.md",
	// "40 - Matthew - KJV.md", "41 - Mark - KJV.md", "42 - Luke - KJV.md",
	// "43 - John - KJV.md", "44 - Acts - KJV.md", "45 - Romans - KJV.md",
	// "46 - 1 Corinthians - KJV.md", "47 - 2 Corinthians - KJV.md",
	// "48 - Galatians - KJV.md", "49 - Ephesians - KJV.md",
	// "50 - Philippians - KJV.md", "51 - Colossians - KJV.md",
	// "52 - 1 Thessalonians - KJV.md", "53 - 2 Thessalonians - KJV.md",
	// "54 - 1 Timothy - KJV.md", "55 - 2 Timothy - KJV.md",
	// "56 - Titus - KJV.md", "57 - Philemon - KJV.md", "58 - Hebrews - KJV.md",
	// "59 - James - KJV.md", "60 - 1 Peter - KJV.md",
	// "61 - 2 Peter - KJV.md", "62 - 1 John - KJV.md", "63 - 2 John - KJV.md",
	// "64 - 3 John - KJV.md", "65 - Jude - KJV.md", "66 - Revelation - KJV.md",
}

// startPostgresContainer starts a PostgreSQL container for the KJV example.
// If persist is true, it mounts a volume to persist data between runs.
func startPostgresContainer() (func(ctx context.Context, opts ...testcontainers.TerminateOption) error, string, error) {
	ctx := context.Background()

	// Create data directory if it doesn't exist
	dataDir := "./data"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, "", fmt.Errorf("failed to create data directory: %w", err)
	}
	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get absolute path for data directory: %w", err)
	}

	// Check if database already exists (data directory has PG_VERSION file)
	pgVersionFile := filepath.Join(absDataDir, "PG_VERSION")
	_, err = os.Stat(pgVersionFile)
	dbExists := err == nil

	// When database already exists, PostgreSQL doesn't re-initialize,
	// so the ready message only appears once instead of twice
	waitOccurrences := 2
	if dbExists {
		waitOccurrences = 1
		fmt.Printf("Using existing persistent database in: %s\n", absDataDir)
	} else {
		fmt.Printf("Creating new persistent database in: %s\n", absDataDir)
	}

	options := []testcontainers.ContainerCustomizer{
		postgres.WithDatabase("database"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(waitOccurrences),
		),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.Mounts = append(hc.Mounts, mount.Mount{
				Type:   mount.TypeBind,
				Source: absDataDir,
				Target: "/var/lib/postgresql/data",
			})
		}),
	}

	pgContainer, err := postgres.Run(
		ctx,
		"timescale/timescaledb:latest-pg17",
		options...,
	)
	if err != nil {
		return nil, "", fmt.Errorf("error starting postgres container: %w", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, "", fmt.Errorf("error getting connection string: %w", err)
	}

	u, err := url.Parse(connStr)
	if err != nil {
		return nil, "", fmt.Errorf("error parsing connection string: %v", err)
	}

	return pgContainer.Terminate, u.Port(), nil
}

func downloadBook(bookName string, outputDir string) (string, error) {
	// URL-encode the filename to handle spaces
	encodedName := url.PathEscape(bookName)
	downloadURL := fmt.Sprintf("%s/%s", kjvRepoURL, encodedName)
	resp, err := http.Get(downloadURL)
	if err != nil {
		return "", fmt.Errorf("failed to download %s: %w", bookName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download %s: status %d", bookName, resp.StatusCode)
	}

	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read %s: %w", bookName, err)
	}

	outputPath := filepath.Join(outputDir, bookName)
	if err := os.WriteFile(outputPath, content, 0644); err != nil {
		return "", fmt.Errorf("failed to write %s: %w", bookName, err)
	}

	return outputPath, nil
}

func main() {
	// Start a PostgreSQL container with optional persistence
	teardown, dbPort, err := startPostgresContainer()
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

	// Set up the default pipeline (semantic chunking + embeddings + entity/relation extraction)
	fmt.Println("Setting up pipeline with entity and relation extraction...")
	if err := g.UseDefaultPipeline(); err != nil {
		log.Fatalf("Failed to set up pipeline: %v", err)
	}

	// Create temporary directory for downloads
	tmpDir, err := os.MkdirTemp("", "kjv-books-*")
	if err != nil {
		log.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Println("Downloading KJV books from GitHub...")

	// Check existing documents to avoid re-processing
	existingDocs, err := checkExistingDocuments(g)
	if err != nil {
		log.Printf("Warning: could not check existing documents: %v", err)
		existingDocs = make(map[string]bool)
	}

	if len(existingDocs) > 0 {
		fmt.Printf("Found %d existing documents in database\n", len(existingDocs))
	}

	// Download and process each book
	totalChunks := 0
	skipped := 0
	processed := 0
	for i, bookName := range kjvBooks {
		source := fmt.Sprintf("kjv/%s", bookName)

		// Skip if document already exists
		if existingDocs[source] {
			fmt.Printf("Skipping %s (%d/%d) - already processed\n", bookName, i+1, len(kjvBooks))
			skipped++
			continue
		}

		fmt.Printf("Downloading %s (%d/%d)...\n", bookName, i+1, len(kjvBooks))

		// Download the book
		bookPath, err := downloadBook(bookName, tmpDir)
		if err != nil {
			log.Printf("Warning: %v, skipping...", err)
			continue
		}

		// Read the book content
		content, err := os.ReadFile(bookPath)
		if err != nil {
			log.Printf("Warning: failed to read %s, skipping...", bookName)
			continue
		}

		// Create document
		bookTitle := extractBookTitle(bookName)
		doc := &model.Document{
			Title:   bookTitle,
			Source:  source,
			Content: string(content),
			Metadata: model.Metadata{
				"testament": getTestament(bookTitle),
				"book":      bookTitle,
				"source":    "King James Version (KJV)",
			},
		}

		// Process and insert document
		fmt.Printf("Processing %s...\n", bookTitle)
		numChunks, err := g.ProcessAndInsertDocument(doc)
		if err != nil {
			log.Printf("Warning: failed to process %s: %v, skipping...", bookTitle, err)
			continue
		}

		fmt.Printf("  ✓ Inserted %d chunks from %s\n", numChunks, bookTitle)
		totalChunks += numChunks
		processed++
	}

	fmt.Printf("\n✓ KJV Bible Status:\n")
	fmt.Printf("  - Processed: %d books (%d chunks)\n", processed, totalChunks)
	fmt.Printf("  - Skipped (already in DB): %d books\n", skipped)
	fmt.Printf("  - Total: %d books\n\n", len(kjvBooks))

	// Search for information about Moses
	query := "What did Moses do on the mountain?"
	fmt.Printf("Searching: %q\n", query)
	fmt.Println(strings.Repeat("=", 20))

	ctx := context.Background()

	// // 1. Basic vector search
	// fmt.Println("\n1. BASIC SEARCH (vector-based)")
	// fmt.Println(strings.Repeat("-", 20))
	// config := model.DefaultQueryConfig()
	// config.TopK = 5
	// config.SimilarityThreshold = 0.0
	// results, err := g.Search(ctx, query, &config)
	// if err != nil {
	// 	log.Printf("Search error: %v", err)
	// } else {
	// 	printResults(results, "Basic Search")
	// }

	// // 2. Contextual search (with hierarchy)
	// fmt.Println("\n2. CONTEXTUAL SEARCH (with hierarchy)")
	// fmt.Println(strings.Repeat("-", 20))
	// config = model.DefaultQueryConfig()
	// config.TopK = 3
	// config.SimilarityThreshold = 0.0
	// config.IncludeAncestors = true
	// config.IncludeDescendants = true
	// config.IncludeSiblings = true
	// results, err = g.ContextualSearch(ctx, query, &config)
	// if err != nil {
	// 	log.Printf("Contextual search error: %v", err)
	// } else {
	// 	printResults(results, "Contextual Search")
	// }

	// 3. Multi-hop search (graph traversal)
	fmt.Println("\n3. MULTI-HOP SEARCH (graph traversal)")
	fmt.Println(strings.Repeat("-", 20))
	config := model.DefaultQueryConfig()
	config.TopK = 3
	config.SimilarityThreshold = 0.0
	config.MaxHops = 2
	results, err := g.MultiHopSearch(ctx, query, &config)
	if err != nil {
		log.Printf("Multi-hop search error: %v", err)
	} else {
		printResults(results, "Multi-Hop Search")
	}

	// 4. Hybrid search (combines vector + graph + hierarchy)
	fmt.Println("\n4. HYBRID SEARCH (vector + graph + hierarchy)")
	fmt.Println(strings.Repeat("-", 20))
	config = model.DefaultQueryConfig()
	config.TopK = 5
	config.SimilarityThreshold = 0.0
	config.MaxHops = 2
	config.IncludeAncestors = true
	config.VectorWeight = 0.1
	config.GraphWeight = 0.1
	config.HierarchyWeight = 0.1
	config.EntityWeight = 1
	results, err = g.HybridSearch(ctx, query, &config)
	if err != nil {
		log.Printf("Hybrid search error: %v", err)
	} else {
		printResults(results, "Hybrid Search")
	}

	fmt.Println("\n" + strings.Repeat("=", 20))
	fmt.Println("Search complete!")
}

// checkExistingDocuments queries the database for documents that start with "kjv/"
// and returns a map of source strings for quick lookup.
func checkExistingDocuments(g *grapher.Grapher) (map[string]bool, error) {
	// Get the documents database handler from the grapher
	docs, err := g.Documents.SelectAllDocuments(nil, 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to query documents: %w", err)
	}

	existingDocs := make(map[string]bool)
	for _, doc := range docs {
		// Check if this is a KJV document
		if strings.HasPrefix(doc.Source, "kjv/") {
			existingDocs[doc.Source] = true
		}
	}

	return existingDocs, nil
}

func getTestament(bookTitle string) string {
	// List of Old Testament books
	oldTestament := map[string]bool{
		"Genesis": true, "Exodus": true, "Leviticus": true, "Numbers": true, "Deuteronomy": true,
		"Joshua": true, "Judges": true, "Ruth": true, "1 Samuel": true, "2 Samuel": true,
		"1 Kings": true, "2 Kings": true, "1 Chronicles": true, "2 Chronicles": true,
		"Ezra": true, "Nehemiah": true, "Esther": true, "Job": true, "Psalms": true,
		"Proverbs": true, "Ecclesiastes": true, "The Song of Solomon": true, "Isaiah": true,
		"Jeremiah": true, "Lamentations": true, "Ezekiel": true, "Daniel": true,
		"Hosea": true, "Joel": true, "Amos": true, "Obadiah": true, "Jonah": true,
		"Micah": true, "Nahum": true, "Habakkuk": true, "Zephaniah": true, "Haggai": true,
		"Zechariah": true, "Malachi": true,
	}

	if oldTestament[bookTitle] {
		return "Old Testament"
	}
	return "New Testament"
}

func extractBookTitle(filename string) string {
	// Extract book name from format like "01 - Genesis - KJV.md"
	parts := strings.Split(filename, " - ")
	if len(parts) >= 2 {
		return strings.TrimSpace(parts[1])
	}
	return strings.TrimSuffix(filename, ".md")
}

func printResults(results []*model.RetrievalResult, searchType string) {
	if len(results) == 0 {
		fmt.Printf("No results found for %s\n", searchType)
		return
	}

	for i, result := range results {
		// Safely get source from metadata
		source := ""
		if s, ok := result.Chunk.Metadata["source"].(string); ok {
			source = s
		}

		book := "Unknown"
		if source != "" {
			book = getBookFromSource(source)
		} else if b, ok := result.Chunk.Metadata["book"].(string); ok {
			book = b
		}

		fmt.Printf("\n[%d] Score: %.4f | Book: %s | Method: %s\n",
			i+1, result.Score, book, result.RetrievalMethod)

		// Print content (truncated if too long)
		content := result.Chunk.Content
		if len(content) > 300 {
			content = content[:300] + "..."
		}
		fmt.Printf("    %s\n", strings.ReplaceAll(content, "\n", "\n    "))

		// Print metadata if available
		if bookMeta, ok := result.Chunk.Metadata["book"].(string); ok {
			fmt.Printf("    [Book: %s", bookMeta)
			if testament, ok := result.Chunk.Metadata["testament"].(string); ok {
				fmt.Printf(", %s", testament)
			}
			fmt.Printf("]\n")
		}
	}
}

func getBookFromSource(source string) string {
	parts := strings.Split(source, "/")
	if len(parts) >= 2 {
		return extractBookTitle(parts[1])
	}
	return source
}
