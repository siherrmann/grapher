package grapher

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"

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

	chunks, err := database.NewChunksDBHandler(db, embeddingDim, false)
	if err != nil {
		return nil, helper.NewError("create chunks handler", err)
	}

	entities, err := database.NewEntitiesDBHandler(db, false)
	if err != nil {
		return nil, helper.NewError("create entities handler", err)
	}

	edges, err := database.NewEdgesDBHandler(db, false)
	if err != nil {
		return nil, helper.NewError("create edges handler", err)
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
// DefaultEmbedder with the all-MiniLM-L6-v2 model (384 dimensions),
// DefaultEntityExtractor with distilbert-NER for entity recognition,
// and DefaultRelationExtractor with distilbert-NER for citation and reference detection
func (g *Grapher) UseDefaultPipeline() error {
	chunker := pipeline.DefaultChunker(500, 0.7)
	embedder, err := pipeline.DefaultEmbedder()
	if err != nil {
		return helper.NewError("create default embedder", err)
	}

	entityExtractor, err := pipeline.DefaultEntityExtractorBasic()
	if err != nil {
		return helper.NewError("create default entity extractor", err)
	}

	relationExtractor, err := pipeline.DefaultRelationExtractor()
	if err != nil {
		return helper.NewError("create default relation extractor", err)
	}

	g.Pipeline = pipeline.NewPipeline(chunker, embedder)
	g.Pipeline.SetEntityExtractor(entityExtractor)
	g.Pipeline.SetRelationExtractor(relationExtractor)
	return nil
}

// UseGraphPipeline sets up a pipeline with REBEL-based combined entity and relation extraction
// This uses DefaultChunker with 500 char max chunks and 0.7 similarity threshold,
// DefaultEmbedder with the all-MiniLM-L6-v2 model (384 dimensions),
// and REBEL model for combined entity and relation extraction in a single pass
// Entity names are embedded for similarity search
func (g *Grapher) UseGraphPipeline() error {
	chunker := pipeline.DefaultChunker(500, 0.7)
	embedder, err := pipeline.DefaultEmbedder()
	if err != nil {
		return helper.NewError("create default embedder", err)
	}

	graphExtractor, err := pipeline.DefaultGraphExtractor(embedder)
	if err != nil {
		return helper.NewError("create graph extractor", err)
	}

	g.Pipeline = pipeline.NewPipeline(chunker, embedder)
	g.Pipeline.SetGraphExtractor(graphExtractor)
	return nil
}

// ProcessAndInsertDocument processes a document by:
// 1. Inserting the document metadata (without content)
// 2. Processing the content into chunks using the pipeline
// 3. Inserting all chunks with the document ID
// 4. Extracting and inserting entities (if entity extractor is configured)
// 5. Extracting and inserting relations/edges (if relation extractor is configured)
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

	// Process content with entity and relation extraction
	result, err := g.Pipeline.ProcessWithExtraction(content, fmt.Sprintf("doc_%s", doc.RID.String()))
	if err != nil {
		return 0, helper.NewError("process chunks", err)
	}

	g.log.Info("Processed document into chunks",
		slog.Int("num_chunks", len(result.Chunks)),
		slog.Int("num_entities", len(result.Entities)),
		slog.Int("num_relations", len(result.Relations)),
		slog.String("document_id", doc.RID.String()))

	// Insert all chunks and build a path-to-ID mapping
	chunkPathToID := make(map[string]int)
	for i, chunk := range result.Chunks {
		chunk.DocumentID = doc.ID

		// Merge document metadata into chunk metadata
		if chunk.Metadata == nil {
			chunk.Metadata = make(model.Metadata)
		}
		for key, value := range doc.Metadata {
			// Only add if not already set by chunker
			if _, exists := chunk.Metadata[key]; !exists {
				chunk.Metadata[key] = value
			}
		}

		if err := g.Chunks.InsertChunk(chunk); err != nil {
			return i, helper.NewError(fmt.Sprintf("insert chunk %d", i), err)
		}
		chunkPathToID[chunk.Path] = chunk.ID
	}

	// Insert entities
	if len(result.Entities) > 0 {
		for _, entity := range result.Entities {
			if err := g.Entities.InsertEntity(entity); err != nil {
				g.log.Error("Failed to insert entity", slog.String("entity", entity.Name), slog.String("error", err.Error()))
				// Continue processing other entities even if one fails
			}
		}
		g.log.Info("Inserted entities", slog.Int("count", len(result.Entities)))
	}

	// Insert relations/edges
	if len(result.Relations) > 0 {
		for _, edge := range result.Relations {
			// For reference edges without entity IDs, link to the source chunk
			if edge.SourceEntityID == nil && edge.SourceChunkID == nil {
				// Get chunk ID from extracted_from metadata
				if extractedFrom, ok := edge.Metadata["extracted_from"].(string); ok {
					if chunkID, found := chunkPathToID[extractedFrom]; found {
						edge.SourceChunkID = &chunkID
					}
				}
			}

			// Skip edges that don't have both source and target
			// (e.g., citations to external documents not in our database)
			hasSource := edge.SourceChunkID != nil || edge.SourceEntityID != nil
			hasTarget := edge.TargetChunkID != nil || edge.TargetEntityID != nil
			if !hasSource || !hasTarget {
				continue // Skip this edge
			}

			if err := g.Edges.InsertEdge(edge); err != nil {
				g.log.Error("Failed to insert edge", slog.String("type", string(edge.EdgeType)), slog.String("error", err.Error()))
				// Continue processing other edges even if one fails
			}
		}
		g.log.Info("Inserted relations", slog.Int("count", len(result.Relations)))
	}

	return len(result.Chunks), nil
}

// Search performs vector similarity search
func (g *Grapher) Search(ctx context.Context, query string, config *model.QueryConfig) ([]*model.Chunk, error) {
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

	return g.Engine.Similarity(ctx, embedding, config)
}

// ContextualSearch performs contextual retrieval (vector + neighbors + hierarchy)
func (g *Grapher) ContextualSearch(ctx context.Context, query string, config *model.QueryConfig) ([]*model.Chunk, error) {
	if g.Pipeline == nil || g.Pipeline.Embedder == nil {
		return nil, helper.NewError("contextual search", fmt.Errorf("pipeline with embedder not set, use SetPipeline() first"))
	}

	// Generate embedding from query
	embedding, err := g.Pipeline.Embedder(query)
	if err != nil {
		return nil, helper.NewError("generate embedding", err)
	}

	return g.Engine.Contextual(ctx, embedding, config)
}

// MultiHopSearch performs multi-hop graph traversal retrieval
func (g *Grapher) MultiHopSearch(ctx context.Context, query string, config *model.QueryConfig) ([]*model.Chunk, error) {
	if g.Pipeline == nil || g.Pipeline.Embedder == nil {
		return nil, helper.NewError("multi-hop search", fmt.Errorf("pipeline with embedder not set, use SetPipeline() first"))
	}

	// Generate embedding from query
	embedding, err := g.Pipeline.Embedder(query)
	if err != nil {
		return nil, helper.NewError("generate embedding", err)
	}

	return g.Engine.MultiHop(ctx, embedding, config)
}

// HybridSearch performs fully configurable hybrid retrieval with entity-based boosting
func (g *Grapher) HybridSearch(ctx context.Context, query string, config *model.QueryConfig) ([]*model.Chunk, error) {
	if g.Pipeline == nil || g.Pipeline.Embedder == nil {
		return nil, helper.NewError("hybrid search", fmt.Errorf("pipeline with embedder not set, use SetPipeline() first"))
	}

	// Generate embedding from query
	embedding, err := g.Pipeline.Embedder(query)
	if err != nil {
		return nil, helper.NewError("generate embedding", err)
	}

	results, err := g.Engine.Hybrid(ctx, embedding, config)
	if err != nil {
		return nil, err
	}

	// Extract entities from query and boost scores for chunks mentioning them
	if g.Pipeline.EntityExtractor != nil && config != nil && config.EntityWeight > 0 {
		queryEntities, err := g.Pipeline.EntityExtractor(query)
		if err == nil && len(queryEntities) > 0 {
			// Create result map for efficient lookup
			resultMap := make(map[int]*model.Chunk)
			for _, r := range results {
				resultMap[r.ID] = r
			}

			// Search for each extracted entity
			for _, queryEntity := range queryEntities {
				// Find matching entities in database
				chunks, err := g.Engine.EntityCentric(ctx, queryEntity.ID, config)
				if err != nil || len(chunks) == 0 {
					continue
				}

				for _, cmen := range chunks {
					if existing, exists := resultMap[cmen.ID]; exists {
						// Boost existing chunk
						existing.Score += config.EntityWeight
						// if existing.RetrievalMethod != "entity" {
						// 	existing.RetrievalMethod = existing.RetrievalMethod + "+entity"
						// }
					} else {
						results = append(results, cmen)
						resultMap[cmen.ID] = cmen
					}
				}
			}

			// Re-sort by score after entity boosting
			sort.Slice(results, func(i, j int) bool {
				return results[i].Score > results[j].Score
			})
		}
	}

	return results, nil
}

// DocumentScopedSearch performs hybrid search within specific documents only
// This is optimized for single or multi-document Q&A by filtering at the database level
func (g *Grapher) DocumentScopedSearch(ctx context.Context, query string, documentRIDs []uuid.UUID, config *model.QueryConfig) ([]*model.Chunk, error) {
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

	return g.Engine.Hybrid(ctx, embedding, config)
}

// EntityCentricSearch performs entity-centric retrieval
func (g *Grapher) EntityCentricSearch(ctx context.Context, entityID int, config *model.QueryConfig) ([]*model.Chunk, error) {
	return g.Engine.EntityCentric(ctx, entityID, config)
}

// BFSTraversal performs breadth-first search from a chunk
func (g *Grapher) BFSTraversal(ctx context.Context, sourceID int, maxHops int, edgeTypes []model.EdgeType, followBidirectional bool) ([]*retrieval.TraversalResult, error) {
	return g.Engine.BFS(ctx, sourceID, maxHops, edgeTypes, followBidirectional)
}

// DFSTraversal performs depth-first search from a chunk
func (g *Grapher) DFSTraversal(ctx context.Context, sourceID int, maxHops int, edgeTypes []model.EdgeType, followBidirectional bool) ([]*retrieval.TraversalResult, error) {
	return g.Engine.DFS(ctx, sourceID, maxHops, edgeTypes, followBidirectional)
}

// ChangeIndexType changes the vector index type between HNSW and IVFFlat
func (g *Grapher) ChangeIndexType(ctx context.Context, indexType string, params map[string]interface{}) error {
	return g.Chunks.ChangeIndexType(ctx, indexType, params)
}
