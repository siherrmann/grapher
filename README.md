# grapher

[![Go Reference](https://pkg.go.dev/badge/github.com/siherrmann/grapher.svg)](https://pkg.go.dev/github.com/siherrmann/grapher)
[![Go Coverage](https://github.com/siherrmann/grapher/wiki/coverage.svg)](https://raw.githack.com/wiki/siherrmann/grapher/coverage.html)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/siherrmann/grapher/blob/master/LICENSE)

A PostgreSQL-based graph database with hybrid retrieval capabilities for document chunks, combining vector similarity search with graph traversal for advanced reasoning.

## 💡 Goal of this package

This package is designed to make graph-based document retrieval as easy as possible while maintaining powerful capabilities. Simple API, flexible chunking strategies, and multiple retrieval methods make it suitable for both simple vector search and complex multi-hop reasoning.

The system combines PostgreSQL's ltree for hierarchical document structure, edge tables for graph relationships, and pgvector for semantic embeddings. All core logic lives in PostgreSQL functions, with Go handlers acting as thin wrappers.

---

## 🛠️ Installation

To integrate the grapher package into your Go project, use the standard go get command:

```bash
go get github.com/siherrmann/grapher
```

To use the package you also need a running PostgreSQL database with the pgvector and ltree extensions. You can start a Docker container with the `timescale/timescaledb:latest-pg17` image which includes both extensions.

---

## 🚀 Getting started

The full initialization is (in the easiest case):

```go
// Create database configuration
dbConfig := &helper.DatabaseConfiguration{
    Host:     "localhost",
    Port:     "5432",
    Database: "grapher",
    Username: "user",
    Password: "password",
    Schema:   "public",
    SSLMode:  "disable",
}

// Create grapher instance with 384-dimensional embeddings
g, err := grapher.NewGrapher(dbConfig, 384)
if err != nil {
    log.Fatal(err)
}
defer g.Close()

// Set up the default pipeline (semantic chunking + embeddings)
err := g.UseDefaultPipeline()
if err != nil {
    log.Fatal(err)
}
```

That's easy, right? Ingesting a document and searching is just as easy:

```go
// Ingest a document
doc := &model.Document{
    Title:   "Introduction to AI",
    Source:  "example.txt",
    Content: "Artificial intelligence is...",
    Metadata: map[string]interface{}{"topic": "AI"},
}

numChunks, err := g.ProcessAndInsertDocument(doc)
if err != nil {
    log.Fatal(err)
}

// Search with query text (embedding is generated automatically)
config := model.DefaultQueryConfig()
config.TopK = 5

results, err := g.Search(context.Background(), "What is AI?", &config)
if err != nil {
    log.Fatal(err)
}
```

In the initialization of the grapher, the existence of the necessary database tables is checked and if they don't exist they get created. The database can be configured with a `DatabaseConfiguration` struct or with these environment variables:

```shell
GRAPHER_DB_HOST=localhost
GRAPHER_DB_PORT=5432
GRAPHER_DB_DATABASE=grapher
GRAPHER_DB_USERNAME=username
GRAPHER_DB_PASSWORD=password
GRAPHER_DB_SCHEMA=public
GRAPHER_DB_SSLMODE=disable
```

You can find full examples in the examples folder demonstrating basic usage, different retrieval strategies, and advanced features like index type switching and graph traversal.

---

## NewGrapher

`NewGrapher` is the primary constructor for creating a new Grapher instance. It initializes the database connection, creates all necessary handlers, and sets up the retrieval engine.

```go
func NewGrapher(config *helper.DatabaseConfiguration, embeddingDim int) (*Grapher, error)
```

- `config`: A `*helper.DatabaseConfiguration` containing database connection parameters (host, port, database, username, password, schema, SSL mode).
- `embeddingDim`: An `int` specifying the dimensionality of the embeddings that will be stored (e.g., 384 for MiniLM, 1536 for OpenAI).

This function performs the following setup:

- Initializes a logger for tracking operations.
- Establishes the database connection.
- Initializes PostgreSQL extensions (pgvector, ltree) and loads SQL functions.
- Creates database handlers for documents, chunks, edges, and entities.
- Initializes the retrieval engine with the handlers.
- Returns a pointer to the configured `Grapher` instance.

If any critical error occurs during initialization (e.g., database connection failure, extension initialization error), the function will return an error. The `Grapher` struct provides access to all handlers (`Chunks`, `Documents`, `Edges`, `Entities`) as well as the `Pipeline` and `Engine` for document processing and retrieval.

---

## UseDefaultPipeline

The `UseDefaultPipeline` method is the recommended way to configure document processing. It automatically sets up semantic chunking and embeddings using production-ready models.

```go
func (g *Grapher) UseDefaultPipeline() error
```

This method configures:

- **Chunker**: Semantic chunking using `all-MiniLM-L6-v2` that identifies natural topic boundaries
  - Maximum chunk size: 500 characters
  - Similarity threshold: 0.7 (creates new chunk when semantic similarity drops below this)
- **Embedder**: Real embeddings using `sentence-transformers/all-MiniLM-L6-v2` (384 dimensions)

The semantic chunker analyzes the text using embeddings to detect topic shifts, creating chunks at natural semantic boundaries rather than arbitrary character or sentence counts.

**Usage:**

```go
g, err := grapher.NewGrapher(dbConfig, 384)
if err != nil {
    log.Fatal(err)
}

// One line to set up the entire pipeline
if err := g.UseDefaultPipeline(); err != nil {
    log.Fatal(err)
}
```

For custom chunking strategies, you can still use `SetPipeline` with your own chunker:

- `pipeline.SentenceChunker(maxSentencesPerChunk)` - Simple sentence-based chunking
- `pipeline.ParagraphChunker()` - Paragraph-based chunking
- `pipeline.DefaultChunker(maxSize, threshold)` - Customizable semantic chunking

---

## SetPipeline

The `SetPipeline` method configures the document processing pipeline for the grapher. A pipeline consists of a chunking function and an embedding function that work together to process documents into searchable chunks.

```go
func (g *Grapher) SetPipeline(pipeline *pipeline.Pipeline)
```

- `pipeline`: A `*pipeline.Pipeline` containing both chunking and embedding functions.

The pipeline is required for:

- Processing documents with `ProcessAndInsertDocument`
- Generating embeddings from query text in search methods
- Any operation that needs to convert text into vector embeddings

You must set a pipeline before calling document processing or search methods, otherwise those methods will return an error.

**Note**: For most use cases, `UseDefaultPipeline()` is recommended instead of manually configuring a pipeline.

---

## ProcessAndInsertDocument

The `ProcessAndInsertDocument` method handles the complete workflow of ingesting a document into the graph database. It inserts the document metadata, processes the content into chunks using the configured pipeline, and inserts all chunks with their embeddings.

```go
func (g *Grapher) ProcessAndInsertDocument(doc *model.Document) (int, error)
```

- `doc`: A `*model.Document` containing the document metadata and content. The `Content` field is used for processing but not stored in the database.

The method performs these steps:

1. Validates that a pipeline is set and content is not empty.
2. Inserts document metadata (title, source, metadata) into the documents table.
3. Processes the content into chunks using the pipeline's chunker.
4. Generates embeddings for each chunk using the pipeline's embedder.
5. Inserts all chunks with their embeddings and hierarchical paths.

Returns the number of chunks successfully inserted and any error encountered. If the pipeline is not set or if any step fails, an error is returned indicating the failure point.

---

## Search Methods

The grapher provides multiple search methods, each implementing different retrieval strategies. All search methods take a query string (not an embedding) as the pipeline's embedder is used automatically to generate the embedding.

### Search (Vector-Only)

Performs pure vector similarity search without any graph expansion.

```go
func (g *Grapher) Search(ctx context.Context, query string, config *model.QueryConfig) ([]*model.RetrievalResult, error)
```

- `ctx`: The context for the operation.
- `query`: The search query text (will be embedded automatically).
- `config`: Query configuration including `TopK` and `SimilarityThreshold`.

Returns chunks ranked by semantic similarity to the query.

### ContextualSearch

Performs contextual retrieval combining vector similarity with immediate graph neighbors and hierarchical siblings.

```go
func (g *Grapher) ContextualSearch(ctx context.Context, query string, config *model.QueryConfig) ([]*model.RetrievalResult, error)
```

- `ctx`: The context for the operation.
- `query`: The search query text.
- `config`: Configuration including `GraphWeight`, `HierarchyWeight`, `IncludeAncestors`, `IncludeDescendants`, and `IncludeSiblings`.

Returns a weighted combination of vector search results, their graph neighbors, and hierarchical relatives.

### MultiHopSearch

Performs multi-hop graph traversal from vector search results, exploring the graph up to the configured depth.

```go
func (g *Grapher) MultiHopSearch(ctx context.Context, query string, config *model.QueryConfig) ([]*model.RetrievalResult, error)
```

- `ctx`: The context for the operation.
- `query`: The search query text.
- `config`: Configuration including `MaxHops`, `EdgeTypes`, `FollowBidirectional`, and `GraphWeight`.

Uses BFS traversal to explore connections from the initial vector search results, useful for finding related information through relationship chains.

### HybridSearch

Performs fully configurable hybrid retrieval with custom weights for vector similarity, graph expansion, and hierarchical context.

```go
func (g *Grapher) HybridSearch(ctx context.Context, query string, config *model.QueryConfig) ([]*model.RetrievalResult, error)
```

- `ctx`: The context for the operation.
- `query`: The search query text.
- `config`: Full configuration with `VectorWeight`, `GraphWeight`, `HierarchyWeight`, `MaxHops`, and all other parameters.

Provides maximum flexibility to balance different retrieval signals based on your specific use case.

### DocumentScopedSearch

Performs hybrid search within specific documents only. This is optimized for single or multi-document Q&A by filtering at the database level, making it more efficient than global search with post-filtering.

```go
func (g *Grapher) DocumentScopedSearch(ctx context.Context, query string, documentRIDs []uuid.UUID, config *model.QueryConfig) ([]*model.RetrievalResult, error)
```

- `ctx`: The context for the operation.
- `query`: The search query text.
- `documentRIDs`: Array of document RIDs to search within.
- `config`: Full configuration (DocumentRIDs will be automatically set).

This method filters chunks at the database level during vector search, using indexed document_id for efficient filtering. This is significantly faster than searching all documents and filtering results afterward.

**Use cases:**

- Single document Q&A: "What does this specific document say about X?"
- Multi-document comparison: Search within a subset of related documents
- User-scoped search: Limit search to documents a user has access to

Example:

```go
// Search within a single document
results, err := g.DocumentScopedSearch(ctx, "How does X work?", []uuid.UUID{docRID}, config)

// Search within multiple specific documents
results, err := g.DocumentScopedSearch(ctx, "Compare X and Y", []uuid.UUID{doc1RID, doc2RID}, config)
```

### EntityCentricSearch

Retrieves all chunks connected to a specific entity, useful for answering questions about a particular person, place, or concept.

```go
func (g *Grapher) EntityCentricSearch(ctx context.Context, entityID uuid.UUID, config *model.QueryConfig) ([]*model.RetrievalResult, error)
```

- `ctx`: The context for the operation.
- `entityID`: The UUID of the entity to search from.
- `config`: Query configuration for filtering and limiting results.

Returns all chunks that have relationships with the specified entity.

---

## Graph Traversal

The grapher provides direct access to graph traversal algorithms for exploring chunk relationships.

### BFSTraversal

Performs breadth-first search from a source chunk.

```go
func (g *Grapher) BFSTraversal(ctx context.Context, sourceID uuid.UUID, maxHops int, edgeTypes []model.EdgeType, followBidirectional bool) ([]*retrieval.TraversalResult, error)
```

### DFSTraversal

Performs depth-first search from a source chunk.

```go
func (g *Grapher) DFSTraversal(ctx context.Context, sourceID uuid.UUID, maxHops int, edgeTypes []model.EdgeType, followBidirectional bool) ([]*retrieval.TraversalResult, error)
```

Both methods return `TraversalResult` objects containing the visited chunks and their distances from the source.

---

## Index Management

### ChangeIndexType

Switches the vector index type between HNSW (better recall) and IVFFlat (faster inserts).

```go
func (g *Grapher) ChangeIndexType(ctx context.Context, indexType string, params map[string]interface{}) error
```

- `ctx`: The context for the operation.
- `indexType`: Either "hnsw" or "ivfflat".
- `params`: Index-specific parameters:
  - HNSW: `m` (max connections per layer), `ef_construction` (build quality)
  - IVFFlat: `lists` (number of clusters)

Example:

```go
// Switch to IVFFlat for faster inserts
g.ChangeIndexType(ctx, "ivfflat", map[string]interface{}{"lists": 100})

// Switch to HNSW for better recall
g.ChangeIndexType(ctx, "hnsw", map[string]interface{}{
    "m": 16,
    "ef_construction": 64,
})
```

---

## Query Configuration

The `QueryConfig` struct allows you to define specific behaviors for retrieval operations.

```go
type QueryConfig struct {
    TopK                int
    SimilarityThreshold float64
    DocumentRIDs        []uuid.UUID
    MaxHops             int
    EdgeTypes           []EdgeType
    FollowBidirectional bool
    IncludeAncestors    bool
    IncludeDescendants  bool
    IncludeSiblings     bool
    VectorWeight        float64
    GraphWeight         float64
    HierarchyWeight     float64
}
```

- `TopK`: Maximum number of results to return.
- `SimilarityThreshold`: Minimum similarity score (0-1) for vector search results.
- `DocumentRIDs`: Filter results to specific documents only (set automatically by DocumentScopedSearch).
- `MaxHops`: Maximum graph traversal depth for multi-hop strategies.
- `EdgeTypes`: Filter edges by type (e.g., semantic, reference, hierarchical).
- `FollowBidirectional`: Whether to traverse edges in both directions.
- `IncludeAncestors`: Include parent chunks in hierarchical expansion.
- `IncludeDescendants`: Include child chunks in hierarchical expansion.
- `IncludeSiblings`: Include sibling chunks at the same hierarchy level.
- `VectorWeight`: Weight for vector similarity scores (0-1).
- `GraphWeight`: Weight for graph-based scores (0-1).
- `HierarchyWeight`: Weight for hierarchical context scores (0-1).

Use `model.DefaultQueryConfig()` to get sensible defaults, then customize as needed.

---

## Edge Types

The system supports different types of relationships between chunks:

```go
const (
    EdgeTypeSemantic     EdgeType = "semantic"      // Conceptual relationships
    EdgeTypeReference    EdgeType = "reference"     // Citation/reference links
    EdgeTypeHierarchical EdgeType = "hierarchical"  // Parent-child in document
    EdgeTypeEntity       EdgeType = "entity"        // Entity co-occurrence
)
```

You can create custom edges between chunks to represent domain-specific relationships:

```go
edge := &model.Edge{
    SourceChunkID: &sourceID,
    TargetChunkID: &targetID,
    EdgeType:      model.EdgeTypeSemantic,
    Weight:        0.8,
    Bidirectional: true,
    Metadata:      map[string]interface{}{"reason": "topic_similarity"},
}

err := g.Edges.InsertEdge(edge)
```

---

## ⭐ Features

- Simple, intuitive API for document ingestion and retrieval
- Multiple retrieval strategies from vector-only to complex multi-hop reasoning
- Automatic query embedding - just pass text, no manual embedding needed
- Hierarchical document structure using PostgreSQL ltree
- Graph relationships with typed edges (semantic, reference, hierarchical, entity)
- Vector similarity search using pgvector with HNSW or IVFFlat indexes
- Configurable chunking strategies (paragraph, sentence, fixed-size, custom)
- Pluggable embedding functions for any model
- SQL-first architecture with all logic in PostgreSQL functions
- Thin Go handlers using standard library database/sql
- Weighted hybrid search combining vector, graph, and hierarchy signals
- BFS and DFS graph traversal algorithms
- Entity-centric retrieval for knowledge graph queries
- Flexible index switching between recall-optimized and insert-optimized
- Comprehensive examples demonstrating all features
- Test suite with testcontainers for reliable integration testing

---

## Examples

See the [examples](examples/) directory for complete working examples:

- **[basic](examples/basic/main.go)** - Simple document ingestion and vector search
- **[advanced](examples/advanced/main.go)** - All retrieval strategies (vector, contextual, multi-hop, hybrid, document-scoped), index switching, and graph traversal

All examples use testcontainers to automatically start PostgreSQL, so they run out of the box without any manual database setup.

---

## License

Apache 2.0
