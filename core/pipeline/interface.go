package pipeline

import "github.com/siherrmann/grapher/model"

// ChunkFunc is a function that splits text into chunks with their hierarchical paths
// The path should follow ltree format (e.g., "doc.chapter1.section2.chunk3")
type ChunkFunc func(text string, basePath string) ([]ChunkWithPath, error)

// EmbedFunc is a function that generates embeddings for text
type EmbedFunc func(text string) ([]float32, error)

// EntityExtractFunc extracts entities from text
// Returns a list of entities with their types and metadata
type EntityExtractFunc func(text string) ([]*model.Entity, error)

// RelationExtractFunc extracts relationships between entities or chunks
// Returns a list of edges representing the relationships
type RelationExtractFunc func(text string, chunkID string, entities []*model.Entity) ([]*model.Edge, error)

// GraphExtractFunc extracts both entities and relationships in a single pass
// Returns entities and edges, typically more efficient than separate extraction
// Used by models like REBEL that jointly extract entities and relations
type GraphExtractFunc func(text string) ([]*model.Entity, []*model.Edge, error)

// ChunkWithPath represents a chunk with its hierarchical path
type ChunkWithPath struct {
	Content    string
	Path       string // ltree path
	StartPos   *int
	EndPos     *int
	ChunkIndex *int
	Metadata   map[string]interface{}
}

// Pipeline combines chunking and embedding functions
type Pipeline struct {
	Chunker           ChunkFunc
	Embedder          EmbedFunc
	EntityExtractor   EntityExtractFunc   // Optional
	RelationExtractor RelationExtractFunc // Optional
	GraphExtractor    GraphExtractFunc    // Optional - extracts entities and relations in one pass
}

// NewPipeline creates a new processing pipeline
func NewPipeline(chunker ChunkFunc, embedder EmbedFunc) *Pipeline {
	return &Pipeline{
		Chunker:  chunker,
		Embedder: embedder,
	}
}

// SetEntityExtractor sets the entity extraction function
func (p *Pipeline) SetEntityExtractor(extractor EntityExtractFunc) {
	p.EntityExtractor = extractor
}

// SetRelationExtractor sets the relation extraction function
func (p *Pipeline) SetRelationExtractor(extractor RelationExtractFunc) {
	p.RelationExtractor = extractor
}

// SetGraphExtractor sets the graph extraction function (combined entity and relation extraction)
// When set, this takes precedence over separate EntityExtractor and RelationExtractor
func (p *Pipeline) SetGraphExtractor(extractor GraphExtractFunc) {
	p.GraphExtractor = extractor
}

// ProcessingResult contains chunks and optionally extracted entities and relations
type ProcessingResult struct {
	Chunks    []*model.Chunk
	Entities  []*model.Entity
	Relations []*model.Edge
}

// Process processes text through the pipeline, returning chunks with embeddings
func (p *Pipeline) Process(text string, basePath string) ([]*model.Chunk, error) {
	result, err := p.ProcessWithExtraction(text, basePath)
	if err != nil {
		return nil, err
	}
	return result.Chunks, nil
}

// ProcessWithExtraction processes text and optionally extracts entities and relations
func (p *Pipeline) ProcessWithExtraction(text string, basePath string) (*ProcessingResult, error) {
	// Split into chunks
	chunksWithPath, err := p.Chunker(text, basePath)
	if err != nil {
		return nil, err
	}

	// Generate embeddings
	chunks := make([]*model.Chunk, 0, len(chunksWithPath))
	var allEntities []*model.Entity
	var allRelations []*model.Edge

	for _, cwp := range chunksWithPath {
		embedding, err := p.Embedder(cwp.Content)
		if err != nil {
			return nil, err
		}

		chunk := &model.Chunk{
			Content:    cwp.Content,
			Path:       cwp.Path,
			Embedding:  embedding,
			StartPos:   cwp.StartPos,
			EndPos:     cwp.EndPos,
			ChunkIndex: cwp.ChunkIndex,
			Metadata:   cwp.Metadata,
		}
		chunks = append(chunks, chunk)

		// Use GraphExtractor if available (combined entity and relation extraction)
		if p.GraphExtractor != nil {
			entities, relations, err := p.GraphExtractor(cwp.Content)
			if err == nil {
				if entities != nil {
					allEntities = append(allEntities, entities...)
				}
				if relations != nil {
					allRelations = append(allRelations, relations...)
				}
			}
		} else {
			// Fall back to separate entity and relation extraction
			var chunkEntities []*model.Entity
			if p.EntityExtractor != nil {
				entities, err := p.EntityExtractor(cwp.Content)
				if err == nil && entities != nil {
					chunkEntities = entities
					allEntities = append(allEntities, entities...)
				}
			}

			// Extract relations if extractor is set
			if p.RelationExtractor != nil {
				relations, err := p.RelationExtractor(cwp.Content, cwp.Path, chunkEntities)
				if err == nil && relations != nil {
					allRelations = append(allRelations, relations...)
				}
			}
		}
	}

	return &ProcessingResult{
		Chunks:    chunks,
		Entities:  allEntities,
		Relations: allRelations,
	}, nil
}
