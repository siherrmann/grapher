package grapher

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/google/uuid"
	"github.com/siherrmann/grapher/core/pipeline"
	"github.com/siherrmann/grapher/core/retrieval"
	"github.com/siherrmann/grapher/database"
	"github.com/siherrmann/grapher/helper"
	"github.com/siherrmann/grapher/model"
	loadSql "github.com/siherrmann/grapher/sql"
)

// Grapher provides a unified interface to all database handlers
type Grapher struct {
	DB        *helper.Database
	Chunks    *database.ChunksDBHandler
	Documents *database.DocumentsDBHandler
	Edges     *database.EdgesDBHandler
	Entities  *database.EntitiesDBHandler
	Pipeline  *pipeline.Pipeline // Optional chunking pipeline
	Engine    *retrieval.Engine  // Retrieval engine for hybrid search
	// Logging
	log *slog.Logger
}

// NewGrapher creates a new Grapher instance with all handlers initialized
func NewGrapher(config *helper.DatabaseConfiguration, embeddingDim int) (*Grapher, error) {
	// Logger
	opts := helper.PrettyHandlerOptions{
		SlogOpts: slog.HandlerOptions{
			Level: slog.LevelInfo,
		},
	}
	logger := slog.New(helper.NewPrettyHandler(os.Stdout, opts))

	// Initialize database
	db := helper.NewDatabase("grapher", config, logger)
	err := loadSql.Init(db.Instance)
	if err != nil {
		return nil, helper.NewError("initialize database extensions", err)
	}

	// Create all handlers in the correct order (documents first, then chunks)
	// force=false to not reload if functions already exist
	documents, err := database.NewDocumentsDBHandler(db, false)
	if err != nil {
		return nil, helper.NewError("create documents handler", err)
	}

	edges, err := database.NewEdgesDBHandler(db, false)
	if err != nil {
		return nil, helper.NewError("create edges handler", err)
	}

	chunks, err := database.NewChunksDBHandler(db, edges, embeddingDim, false)
	if err != nil {
		return nil, helper.NewError("create chunks handler", err)
	}

	entities, err := database.NewEntitiesDBHandler(db, false)
	if err != nil {
		return nil, helper.NewError("create entities handler", err)
	}

	// Create retrieval engine with database handlers
	engine := retrieval.NewEngine(chunks, edges, entities)

	return &Grapher{
		DB:        db,
		Chunks:    chunks,
		Documents: documents,
		Edges:     edges,
		Entities:  entities,
		Engine:    engine,
		log:       logger,
	}, nil
}

// Close closes the database connection
func (g *Grapher) Close() error {
	if g.DB != nil && g.DB.Instance != nil {
		return g.DB.Instance.Close()
	}
	return nil
}

// SetPipeline sets the chunking pipeline for document processing
func (g *Grapher) SetPipeline(pipeline *pipeline.Pipeline) {
	g.Pipeline = pipeline
}

// UseDefaultPipeline sets up the default semantic chunking and embedding pipeline
// This uses DefaultChunker with 500 char max chunks and 0.7 similarity threshold,
// and DefaultEmbedder with the all-MiniLM-L6-v2 model (384 dimensions)
func (g *Grapher) UseDefaultPipeline() error {
	chunker := pipeline.DefaultChunker(500, 0.7)
	embedder, err := pipeline.DefaultEmbedder()
	if err != nil {
		return helper.NewError("create default embedder", err)
	}

	g.Pipeline = pipeline.NewPipeline(chunker, embedder)
	return nil
}

// ProcessAndInsertDocument processes a document by:
// 1. Inserting the document metadata (without content)
// 2. Processing the content into chunks using the pipeline
// 3. Inserting all chunks with the document ID
// The document's Content field is used for processing but not stored in the database.
// Returns the number of chunks inserted and any error encountered.
func (g *Grapher) ProcessAndInsertDocument(doc *model.Document) (int, error) {
	if g.Pipeline == nil {
		return 0, helper.NewError("process document", fmt.Errorf("pipeline not set, use SetPipeline() first"))
	}

	if doc.Content == "" {
		return 0, helper.NewError("process document", fmt.Errorf("document content is empty"))
	}

	// Store content temporarily and clear it before DB insert
	content := doc.Content
	doc.Content = ""

	// Insert document metadata
	if err := g.Documents.InsertDocument(doc); err != nil {
		return 0, helper.NewError("insert document", err)
	}

	g.log.Info("Inserted document", slog.String("document_id", doc.RID.String()), slog.String("title", doc.Title))

	// Process content into chunks
	chunks, err := g.Pipeline.Process(content, fmt.Sprintf("doc_%s", doc.RID.String()))
	if err != nil {
		return 0, helper.NewError("process chunks", err)
	}

	g.log.Info("Processed document into chunks", slog.Int("num_chunks", len(chunks)), slog.String("document_id", doc.RID.String()))

	// Insert all chunks
	for i, chunk := range chunks {
		chunk.DocumentID = doc.ID
		if err := g.Chunks.InsertChunk(chunk); err != nil {
			return i, helper.NewError(fmt.Sprintf("insert chunk %d", i), err)
		}
	}

	return len(chunks), nil
}

// Search performs vector similarity search
func (g *Grapher) Search(ctx context.Context, query string, config *model.QueryConfig) ([]*model.RetrievalResult, error) {
	if g.Engine == nil {
		return nil, helper.NewError("vector search", fmt.Errorf("retrieval engine not initialized"))
	}
	if g.Pipeline == nil || g.Pipeline.Embedder == nil {
		return nil, helper.NewError("vector search", fmt.Errorf("pipeline with embedder not set, use SetPipeline() first"))
	}

	// Generate embedding from query
	embedding, err := g.Pipeline.Embedder(query)
	if err != nil {
		return nil, helper.NewError("generate embedding", err)
	}

	return g.Engine.VectorRetrieve(ctx, embedding, config)
}

// ContextualSearch performs contextual retrieval (vector + neighbors + hierarchy)
func (g *Grapher) ContextualSearch(ctx context.Context, query string, config *model.QueryConfig) ([]*model.RetrievalResult, error) {
	if g.Pipeline == nil || g.Pipeline.Embedder == nil {
		return nil, helper.NewError("contextual search", fmt.Errorf("pipeline with embedder not set, use SetPipeline() first"))
	}

	// Generate embedding from query
	embedding, err := g.Pipeline.Embedder(query)
	if err != nil {
		return nil, helper.NewError("generate embedding", err)
	}

	strategy := retrieval.NewContextualStrategy(g.Engine)
	return strategy.Retrieve(ctx, embedding, config)
}

// MultiHopSearch performs multi-hop graph traversal retrieval
func (g *Grapher) MultiHopSearch(ctx context.Context, query string, config *model.QueryConfig) ([]*model.RetrievalResult, error) {
	if g.Pipeline == nil || g.Pipeline.Embedder == nil {
		return nil, helper.NewError("multi-hop search", fmt.Errorf("pipeline with embedder not set, use SetPipeline() first"))
	}

	// Generate embedding from query
	embedding, err := g.Pipeline.Embedder(query)
	if err != nil {
		return nil, helper.NewError("generate embedding", err)
	}

	strategy := retrieval.NewMultiHopStrategy(g.Engine)
	return strategy.Retrieve(ctx, embedding, config)
}

// HybridSearch performs fully configurable hybrid retrieval
func (g *Grapher) HybridSearch(ctx context.Context, query string, config *model.QueryConfig) ([]*model.RetrievalResult, error) {
	if g.Pipeline == nil || g.Pipeline.Embedder == nil {
		return nil, helper.NewError("hybrid search", fmt.Errorf("pipeline with embedder not set, use SetPipeline() first"))
	}

	// Generate embedding from query
	embedding, err := g.Pipeline.Embedder(query)
	if err != nil {
		return nil, helper.NewError("generate embedding", err)
	}

	strategy := retrieval.NewHybridStrategy(g.Engine)
	return strategy.Retrieve(ctx, embedding, config)
}

// DocumentScopedSearch performs hybrid search within specific documents only
// This is optimized for single or multi-document Q&A by filtering at the database level
func (g *Grapher) DocumentScopedSearch(ctx context.Context, query string, documentRIDs []uuid.UUID, config *model.QueryConfig) ([]*model.RetrievalResult, error) {
	if g.Pipeline == nil || g.Pipeline.Embedder == nil {
		return nil, helper.NewError("document scoped search", fmt.Errorf("pipeline with embedder not set, use SetPipeline() first"))
	}

	if len(documentRIDs) == 0 {
		return nil, helper.NewError("document scoped search", fmt.Errorf("at least one document RID must be provided"))
	}

	// Generate embedding from query
	embedding, err := g.Pipeline.Embedder(query)
	if err != nil {
		return nil, helper.NewError("generate embedding", err)
	}

	// Set document filter in config
	if config == nil {
		config = &model.QueryConfig{}
	}
	config.DocumentRIDs = documentRIDs

	strategy := retrieval.NewHybridStrategy(g.Engine)
	return strategy.Retrieve(ctx, embedding, config)
}

// EntityCentricSearch performs entity-centric retrieval
func (g *Grapher) EntityCentricSearch(ctx context.Context, entityID uuid.UUID, config *model.QueryConfig) ([]*model.RetrievalResult, error) {
	strategy := retrieval.NewEntityCentricStrategy(g.Engine, g.Entities)
	return strategy.Retrieve(ctx, entityID, config)
}

// BFSTraversal performs breadth-first search from a chunk
func (g *Grapher) BFSTraversal(ctx context.Context, sourceID uuid.UUID, maxHops int, edgeTypes []model.EdgeType, followBidirectional bool) ([]*retrieval.TraversalResult, error) {
	return g.Engine.BFS(ctx, sourceID, maxHops, edgeTypes, followBidirectional)
}

// DFSTraversal performs depth-first search from a chunk
func (g *Grapher) DFSTraversal(ctx context.Context, sourceID uuid.UUID, maxHops int, edgeTypes []model.EdgeType, followBidirectional bool) ([]*retrieval.TraversalResult, error) {
	return g.Engine.DFS(ctx, sourceID, maxHops, edgeTypes, followBidirectional)
}

// ChangeIndexType changes the vector index type between HNSW and IVFFlat
func (g *Grapher) ChangeIndexType(ctx context.Context, indexType string, params map[string]interface{}) error {
	return g.Chunks.ChangeIndexType(ctx, indexType, params)
}
