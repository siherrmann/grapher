-- Chunks SQL Functions

-- Initialize chunks table and related objects
CREATE OR REPLACE FUNCTION init_chunks(embedding_dim INT DEFAULT 384) RETURNS VOID AS $$
BEGIN
    -- Create chunks table
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS chunks (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
            content TEXT NOT NULL,
            path LTREE NOT NULL,
            embedding VECTOR(%s) DEFAULT array_fill(0.0::REAL, ARRAY[%s])::VECTOR,
            start_pos INTEGER,
            end_pos INTEGER,
            chunk_index INTEGER,
            metadata JSONB DEFAULT ''{}''::jsonb,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )', embedding_dim, embedding_dim);
    
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
    input_embedding FLOAT[],
    input_start_pos INT,
    input_end_pos INT,
    input_chunk_index INT,
    input_metadata JSONB
)
RETURNS TABLE (
    output_id BIGINT,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding REAL[],
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
        chunks.embedding::REAL[],
        chunks.start_pos,
        chunks.end_pos,
        chunks.chunk_index,
        chunks.metadata,
        chunks.created_at;
END;
$$ LANGUAGE plpgsql;

-- Update chunk embedding
CREATE OR REPLACE FUNCTION update_chunk(
    input_id BIGINT,
    input_embedding VECTOR
)
RETURNS TABLE (
    output_id BIGINT,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding REAL[],
    output_start_pos INT,
    output_end_pos INT,
    output_chunk_index INT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    UPDATE chunks
    SET embedding = input_embedding
    WHERE id = input_id
    RETURNING
        id,
        document_id,
        (SELECT d.rid FROM documents d WHERE d.id = chunks.document_id),
        content,
        path,
        embedding::REAL[],
        start_pos,
        end_pos,
        chunk_index,
        metadata,
        created_at;
END;
$$ LANGUAGE plpgsql;

-- Delete chunk
CREATE OR REPLACE FUNCTION delete_chunk(input_id BIGINT)
RETURNS VOID
AS $$
BEGIN
    DELETE FROM chunks WHERE id = input_id;
END;
$$ LANGUAGE plpgsql;

-- Select chunk by ID
CREATE OR REPLACE FUNCTION select_chunk(input_id BIGINT)
RETURNS TABLE (
    output_id BIGINT,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding REAL[],
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
        c.embedding::REAL[],
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
    output_id BIGINT,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding REAL[],
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
        c.embedding::REAL[],
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

-- Select all chunks by entity ID with edge metadata
CREATE OR REPLACE FUNCTION select_chunks_by_entity(
    input_entity_id BIGINT
)
RETURNS TABLE (
    output_id BIGINT,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding REAL[],
    output_start_pos INTEGER,
    output_end_pos INTEGER,
    output_chunk_index INTEGER,
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
        c.embedding::REAL[],
        c.start_pos,
        c.end_pos,
        c.chunk_index,
        c.metadata,
        c.created_at
    FROM entities ent
    JOIN edges e ON (
        (e.source_entity_id = ent.id AND e.target_chunk_id IS NOT NULL)
        OR 
        (e.target_entity_id = ent.id AND e.source_chunk_id IS NOT NULL)
    )
    JOIN chunks c ON c.id = COALESCE(e.target_chunk_id, e.source_chunk_id)
    LEFT JOIN documents d ON c.document_id = d.id
    WHERE ent.id = input_entity_id
        AND e.edge_type = 'entity_mention'
    ORDER BY e.created_at DESC;
END;
$$ LANGUAGE plpgsql;

-- Select chunks by path (hierarchical query)
-- Matches descendants of the given path
CREATE OR REPLACE FUNCTION select_chunks_by_path_descendant(input_path LTREE)
RETURNS TABLE (
    output_id BIGINT,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding REAL[],
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
        c.embedding::REAL[],
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
    output_id BIGINT,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding REAL[],
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
        c.embedding::REAL[],
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
    output_id BIGINT,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding REAL[],
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
        c.embedding::REAL[],
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
    output_id BIGINT,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding REAL[],
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
        c.embedding::REAL[],
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
    output_id BIGINT,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding REAL[],
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
            c.embedding::REAL[],
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
            c.embedding::REAL[],
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
        cc.embedding::REAL[],
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

-- BFS traversal from a starting chunk
-- Returns all chunks reachable within max_hops via edges
CREATE OR REPLACE FUNCTION select_chunks_by_bfs(
    input_start_chunk_id BIGINT,
    input_max_hops INT DEFAULT 3,
    input_edge_types TEXT[] DEFAULT NULL,
    input_bidirectional BOOLEAN DEFAULT TRUE
)
RETURNS TABLE (
    output_id BIGINT,
    output_document_id BIGINT,
    output_document_rid UUID,
    output_content TEXT,
    output_path LTREE,
    output_embedding REAL[],
    output_start_pos INT,
    output_end_pos INT,
    output_chunk_index INT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE,
    output_hop_distance INT
)
AS $$
BEGIN
    RETURN QUERY
    WITH RECURSIVE bfs_traversal AS (
        -- Base case: start with the initial chunk at hop 0
        SELECT 
            c.id,
            c.document_id,
            c.content,
            c.path,
            c.embedding::REAL[],
            c.start_pos,
            c.end_pos,
            c.chunk_index,
            c.metadata,
            c.created_at,
            0 AS hop_distance
        FROM chunks c
        WHERE c.id = input_start_chunk_id
        
        UNION
        
        -- Recursive case: find neighbors
        SELECT DISTINCT
            c.id,
            c.document_id,
            c.content,
            c.path,
            c.embedding::REAL[],
            c.start_pos,
            c.end_pos,
            c.chunk_index,
            c.metadata,
            c.created_at,
            bt.hop_distance + 1
        FROM bfs_traversal bt
        JOIN edges e ON (
            -- Forward edges: bt.id is source, find target
            (e.source_chunk_id = bt.id AND e.target_chunk_id IS NOT NULL)
            -- Backward edges (if bidirectional or edge is marked bidirectional)
            OR (input_bidirectional AND e.target_chunk_id = bt.id AND e.source_chunk_id IS NOT NULL)
            OR (e.bidirectional AND e.target_chunk_id = bt.id AND e.source_chunk_id IS NOT NULL)
        )
        JOIN chunks c ON (
            -- Get the target chunk (handle both directions)
            c.id = CASE 
                WHEN e.source_chunk_id = bt.id THEN e.target_chunk_id
                ELSE e.source_chunk_id
            END
        )
        WHERE bt.hop_distance < input_max_hops
            AND (input_edge_types IS NULL OR e.edge_type::TEXT = ANY(input_edge_types))
    )
    SELECT DISTINCT ON (bt.id)
        bt.id,
        bt.document_id,
        d.rid,
        bt.content,
        bt.path,
        bt.embedding,
        bt.start_pos,
        bt.end_pos,
        bt.chunk_index,
        bt.metadata,
        bt.created_at,
        bt.hop_distance
    FROM bfs_traversal bt
    LEFT JOIN documents d ON bt.document_id = d.id
    ORDER BY bt.id, bt.hop_distance;
END;
$$ LANGUAGE plpgsql;
