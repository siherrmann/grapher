-- Chunks SQL Functions

-- Initialize chunks table and related objects
CREATE OR REPLACE FUNCTION init_chunks(embedding_dim INT DEFAULT 384) RETURNS VOID AS $$
BEGIN
    -- Create required extensions
    CREATE EXTENSION IF NOT EXISTS vector;
    CREATE EXTENSION IF NOT EXISTS ltree;
    
    -- Create chunks table
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS chunks (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
            content TEXT NOT NULL,
            path LTREE NOT NULL,
            embedding VECTOR(%s),
            start_pos INTEGER,
            end_pos INTEGER,
            chunk_index INTEGER,
            metadata JSONB DEFAULT ''{}''::jsonb,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )', embedding_dim);
    
    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_chunks_path ON chunks USING GIST (path);
    CREATE INDEX IF NOT EXISTS idx_chunks_path_btree ON chunks USING BTREE (path);
    CREATE INDEX IF NOT EXISTS idx_chunks_document ON chunks(document_id);
    CREATE INDEX IF NOT EXISTS idx_chunks_metadata ON chunks USING GIN (metadata);
    
    -- Create HNSW index for vector similarity if it doesn't exist
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_chunks_embedding') THEN
        CREATE INDEX idx_chunks_embedding ON chunks USING hnsw (embedding vector_cosine_ops)
        WITH (m = 16, ef_construction = 64);
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Insert a new chunk
CREATE OR REPLACE FUNCTION insert_chunk(
    input_document_id BIGINT,
    input_content TEXT,
    input_path LTREE,
    input_embedding VECTOR,
    input_start_pos INT,
    input_end_pos INT,
    input_chunk_index INT,
    input_metadata JSONB
)
RETURNS TABLE (
    output_id UUID,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding VECTOR,
    output_start_pos INT,
    output_end_pos INT,
    output_chunk_index INT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    INSERT INTO chunks (document_id, content, path, embedding, start_pos, end_pos, chunk_index, metadata)
    VALUES (input_document_id, input_content, input_path, input_embedding, input_start_pos, input_end_pos, input_chunk_index, input_metadata)
    RETURNING 
        chunks.id,
        chunks.document_id,
        (SELECT d.rid FROM documents d WHERE d.id = chunks.document_id),
        chunks.content,
        chunks.path,
        chunks.embedding,
        chunks.start_pos,
        chunks.end_pos,
        chunks.chunk_index,
        chunks.metadata,
        chunks.created_at;
END;
$$ LANGUAGE plpgsql;

-- Select chunk by ID
CREATE OR REPLACE FUNCTION select_chunk(input_id UUID)
RETURNS TABLE (
    output_id UUID,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding VECTOR,
    output_start_pos INT,
    output_end_pos INT,
    output_chunk_index INT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.id,
        c.document_id,
        d.rid,
        c.content,
        c.path,
        c.embedding,
        c.start_pos,
        c.end_pos,
        c.chunk_index,
        c.metadata,
        c.created_at
    FROM chunks c
    LEFT JOIN documents d ON c.document_id = d.id
    WHERE c.id = input_id;
END;
$$ LANGUAGE plpgsql;

-- Select chunks by document ID
CREATE OR REPLACE FUNCTION select_chunks_by_document(input_document_rid UUID)
RETURNS TABLE (
    output_id UUID,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding VECTOR,
    output_start_pos INT,
    output_end_pos INT,
    output_chunk_index INT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.id,
        c.document_id,
        d.rid,
        c.content,
        c.path,
        c.embedding,
        c.start_pos,
        c.end_pos,
        c.chunk_index,
        c.metadata,
        c.created_at
    FROM chunks c
    INNER JOIN documents d ON c.document_id = d.id
    WHERE d.rid = input_document_rid
    ORDER BY c.chunk_index ASC NULLS LAST, c.created_at ASC;
END;
$$ LANGUAGE plpgsql;

-- Select chunks by path (hierarchical query)
-- Matches descendants of the given path
CREATE OR REPLACE FUNCTION select_chunks_by_path_descendant(input_path LTREE)
RETURNS TABLE (
    output_id UUID,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding VECTOR,
    output_start_pos INT,
    output_end_pos INT,
    output_chunk_index INT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.id,
        c.document_id,
        d.rid,
        c.content,
        c.path,
        c.embedding,
        c.start_pos,
        c.end_pos,
        c.chunk_index,
        c.metadata,
        c.created_at
    FROM chunks c
    LEFT JOIN documents d ON c.document_id = d.id
    WHERE c.path <@ input_path  -- is descendant of
    ORDER BY c.path;
END;
$$ LANGUAGE plpgsql;

-- Select chunks by path (ancestor query)
CREATE OR REPLACE FUNCTION select_chunks_by_path_ancestor(input_path LTREE)
RETURNS TABLE (
    output_id UUID,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding VECTOR,
    output_start_pos INT,
    output_end_pos INT,
    output_chunk_index INT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.id,
        c.document_id,
        d.rid,
        c.content,
        c.path,
        c.embedding,
        c.start_pos,
        c.end_pos,
        c.chunk_index,
        c.metadata,
        c.created_at
    FROM chunks c
    LEFT JOIN documents d ON c.document_id = d.id
    WHERE c.path @> input_path  -- is ancestor of
    ORDER BY c.path;
END;
$$ LANGUAGE plpgsql;

-- Select sibling chunks (same parent, same level)
CREATE OR REPLACE FUNCTION select_sibling_chunks(input_path LTREE)
RETURNS TABLE (
    output_id UUID,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding VECTOR,
    output_start_pos INT,
    output_end_pos INT,
    output_chunk_index INT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.id,
        c.document_id,
        d.rid,
        c.content,
        c.path,
        c.embedding,
        c.start_pos,
        c.end_pos,
        c.chunk_index,
        c.metadata,
        c.created_at
    FROM chunks c
    LEFT JOIN documents d ON c.document_id = d.id
    WHERE 
        -- Same parent (subpath removes last component)
        subpath(c.path, 0, nlevel(input_path) - 1) = subpath(input_path, 0, nlevel(input_path) - 1)
        -- Same depth level
        AND nlevel(c.path) = nlevel(input_path)
        -- Not the same chunk
        AND c.path != input_path
    ORDER BY c.path;
END;
$$ LANGUAGE plpgsql;

-- Vector similarity search
CREATE OR REPLACE FUNCTION select_chunks_by_similarity(
    input_embedding VECTOR,
    input_limit INT,
    input_threshold FLOAT DEFAULT 0.0,
    input_document_rids UUID[] DEFAULT NULL
)
RETURNS TABLE (
    output_id UUID,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding VECTOR,
    output_start_pos INT,
    output_end_pos INT,
    output_chunk_index INT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE,
    output_similarity FLOAT
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        c.id,
        c.document_id,
        d.rid,
        c.content,
        c.path,
        c.embedding,
        c.start_pos,
        c.end_pos,
        c.chunk_index,
        c.metadata,
        c.created_at,
        1 - (c.embedding <=> input_embedding) AS similarity
    FROM chunks c
    LEFT JOIN documents d ON c.document_id = d.id
    WHERE c.embedding IS NOT NULL
        AND (1 - (c.embedding <=> input_embedding)) >= input_threshold
        AND (input_document_rids IS NULL OR d.rid = ANY(input_document_rids))
    ORDER BY c.embedding <=> input_embedding
    LIMIT input_limit;
END;
$$ LANGUAGE plpgsql;

-- Hybrid search: vector similarity + ltree hierarchy
-- Finds similar chunks and also includes their ancestors/descendants
CREATE OR REPLACE FUNCTION select_chunks_by_similarity_with_context(
    input_embedding VECTOR,
    input_limit INT,
    input_include_ancestors BOOLEAN DEFAULT TRUE,
    input_include_descendants BOOLEAN DEFAULT TRUE,
    input_threshold FLOAT DEFAULT 0.0,
    input_document_rids UUID[] DEFAULT NULL
)
RETURNS TABLE (
    output_id UUID,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding VECTOR,
    output_start_pos INT,
    output_end_pos INT,
    output_chunk_index INT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE,
    output_similarity FLOAT,
    output_is_match BOOLEAN
)
AS $$
BEGIN
    RETURN QUERY
    WITH similar_chunks AS (
        SELECT 
            c.id,
            c.document_id,
            c.content,
            c.path,
            c.embedding,
            c.start_pos,
            c.end_pos,
            c.chunk_index,
            c.metadata,
            c.created_at,
            1 - (c.embedding <=> input_embedding) AS similarity
        FROM chunks c
        LEFT JOIN documents d ON c.document_id = d.id
        WHERE c.embedding IS NOT NULL
            AND (1 - (c.embedding <=> input_embedding)) >= input_threshold
            AND (input_document_rids IS NULL OR d.rid = ANY(input_document_rids))
        ORDER BY c.embedding <=> input_embedding
        LIMIT input_limit
    ),
    context_chunks AS (
        SELECT DISTINCT ON (c.id)
            c.id,
            c.document_id,
            c.content,
            c.path,
            c.embedding,
            c.start_pos,
            c.end_pos,
            c.chunk_index,
            c.metadata,
            c.created_at,
            sc.similarity,
            (c.id = sc.id) AS is_match
        FROM similar_chunks sc
        CROSS JOIN LATERAL (
            SELECT * FROM chunks c2
            WHERE 
                (input_include_ancestors AND c2.path @> sc.path)
                OR (input_include_descendants AND c2.path <@ sc.path)
                OR c2.id = sc.id
        ) c
    )
    SELECT 
        cc.id,
        cc.document_id,
        d.rid,
        cc.content,
        cc.path,
        cc.embedding,
        cc.start_pos,
        cc.end_pos,
        cc.chunk_index,
        cc.metadata,
        cc.created_at,
        cc.similarity,
        cc.is_match
    FROM context_chunks cc
    LEFT JOIN documents d ON cc.document_id = d.id
    ORDER BY cc.similarity DESC NULLS LAST, cc.path;
END;
$$ LANGUAGE plpgsql;

-- Delete chunk
CREATE OR REPLACE FUNCTION delete_chunk(input_id UUID)
RETURNS VOID
AS $$
BEGIN
    DELETE FROM chunks WHERE id = input_id;
END;
$$ LANGUAGE plpgsql;

-- Update chunk embedding
CREATE OR REPLACE FUNCTION update_chunk_embedding(
    input_id UUID,
    input_embedding VECTOR
)
RETURNS TABLE (
    output_id UUID,
    output_embedding VECTOR
)
AS $$
BEGIN
    RETURN QUERY
    UPDATE chunks
    SET embedding = input_embedding
    WHERE id = input_id
    RETURNING id, embedding;
END;
$$ LANGUAGE plpgsql;
