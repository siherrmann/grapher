-- Edges SQL Functions

-- Initialize edges table and related objects
CREATE OR REPLACE FUNCTION init_edges() RETURNS VOID AS $$
BEGIN
    -- Create edges table
    CREATE TABLE IF NOT EXISTS edges (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        source_chunk_id BIGINT REFERENCES chunks(id) ON DELETE CASCADE,
        target_chunk_id BIGINT REFERENCES chunks(id) ON DELETE CASCADE,
        source_entity_id BIGINT REFERENCES entities(id) ON DELETE CASCADE,
        target_entity_id BIGINT REFERENCES entities(id) ON DELETE CASCADE,
        edge_type edge_type NOT NULL,
        weight FLOAT DEFAULT 1.0,
        bidirectional BOOLEAN DEFAULT FALSE,
        metadata JSONB DEFAULT '{}',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        
        CONSTRAINT edge_source_check CHECK (
            (source_chunk_id IS NOT NULL) OR (source_entity_id IS NOT NULL)
        ),
        CONSTRAINT edge_target_check CHECK (
            (target_chunk_id IS NOT NULL) OR (target_entity_id IS NOT NULL)
        )
    );
    
    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_edges_source_chunk ON edges(source_chunk_id) WHERE source_chunk_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_edges_target_chunk ON edges(target_chunk_id) WHERE target_chunk_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_edges_source_entity ON edges(source_entity_id) WHERE source_entity_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_edges_target_entity ON edges(target_entity_id) WHERE target_entity_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_edges_type ON edges(edge_type);
    CREATE INDEX IF NOT EXISTS idx_edges_composite ON edges(source_chunk_id, edge_type) WHERE source_chunk_id IS NOT NULL;
END;
$$ LANGUAGE plpgsql;

-- Insert a new edge
CREATE OR REPLACE FUNCTION insert_edge(
    input_source_chunk_id BIGINT,
    input_target_chunk_id BIGINT,
    input_source_entity_id BIGINT,
    input_target_entity_id BIGINT,
    input_edge_type edge_type,
    input_weight FLOAT,
    input_bidirectional BOOLEAN,
    input_metadata JSONB
)
RETURNS TABLE (
    output_id BIGINT,
    output_source_chunk_id BIGINT,
    output_target_chunk_id BIGINT,
    output_source_entity_id BIGINT,
    output_target_entity_id BIGINT,
    output_edge_type edge_type,
    output_weight FLOAT,
    output_bidirectional BOOLEAN,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    INSERT INTO edges (
        source_chunk_id, 
        target_chunk_id, 
        source_entity_id, 
        target_entity_id, 
        edge_type, 
        weight, 
        bidirectional, 
        metadata
    )
    VALUES (
        input_source_chunk_id,
        input_target_chunk_id,
        input_source_entity_id,
        input_target_entity_id,
        input_edge_type,
        input_weight,
        input_bidirectional,
        input_metadata
    )
    RETURNING 
        id,
        source_chunk_id,
        target_chunk_id,
        source_entity_id,
        target_entity_id,
        edge_type,
        weight,
        bidirectional,
        metadata,
        created_at;
END;
$$ LANGUAGE plpgsql;

-- Update edge weight
CREATE OR REPLACE FUNCTION update_edge_weight(
    input_id BIGINT,
    input_weight FLOAT
)
RETURNS TABLE (
    output_id BIGINT,
    output_weight FLOAT
)
AS $$
BEGIN
    RETURN QUERY
    UPDATE edges
    SET weight = input_weight
    WHERE id = input_id
    RETURNING id, weight;
END;
$$ LANGUAGE plpgsql;

-- Delete edge
CREATE OR REPLACE FUNCTION delete_edge(input_id BIGINT)
RETURNS VOID
AS $$
BEGIN
    DELETE FROM edges WHERE id = input_id;
END;
$$ LANGUAGE plpgsql;

-- Select edge by ID
CREATE OR REPLACE FUNCTION select_edge(input_id BIGINT)
RETURNS TABLE (
    output_id BIGINT,
    output_source_chunk_id BIGINT,
    output_target_chunk_id BIGINT,
    output_source_entity_id BIGINT,
    output_target_entity_id BIGINT,
    output_edge_type edge_type,
    output_weight FLOAT,
    output_bidirectional BOOLEAN,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        id,
        source_chunk_id,
        target_chunk_id,
        source_entity_id,
        target_entity_id,
        edge_type,
        weight,
        bidirectional,
        metadata,
        created_at
    FROM edges
    WHERE id = input_id;
END;
$$ LANGUAGE plpgsql;

-- Select edges from a chunk (outgoing)
CREATE OR REPLACE FUNCTION select_edges_from_chunk(
    input_chunk_id BIGINT,
    input_edge_type edge_type DEFAULT NULL
)
RETURNS TABLE (
    output_id BIGINT,
    output_source_chunk_id BIGINT,
    output_target_chunk_id BIGINT,
    output_source_entity_id BIGINT,
    output_target_entity_id BIGINT,
    output_edge_type edge_type,
    output_weight FLOAT,
    output_bidirectional BOOLEAN,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        id,
        source_chunk_id,
        target_chunk_id,
        source_entity_id,
        target_entity_id,
        edge_type,
        weight,
        bidirectional,
        metadata,
        created_at
    FROM edges
    WHERE source_chunk_id = input_chunk_id
        AND (input_edge_type IS NULL OR edge_type = input_edge_type)
    ORDER BY weight DESC, created_at;
END;
$$ LANGUAGE plpgsql;

-- Select edges to a chunk (incoming)
CREATE OR REPLACE FUNCTION select_edges_to_chunk(
    input_chunk_id BIGINT,
    input_edge_type edge_type DEFAULT NULL
)
RETURNS TABLE (
    output_id BIGINT,
    output_source_chunk_id BIGINT,
    output_target_chunk_id BIGINT,
    output_source_entity_id BIGINT,
    output_target_entity_id BIGINT,
    output_edge_type edge_type,
    output_weight FLOAT,
    output_bidirectional BOOLEAN,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        id,
        source_chunk_id,
        target_chunk_id,
        source_entity_id,
        target_entity_id,
        edge_type,
        weight,
        bidirectional,
        metadata,
        created_at
    FROM edges
    WHERE target_chunk_id = input_chunk_id
        AND (input_edge_type IS NULL OR edge_type = input_edge_type)
    ORDER BY weight DESC, created_at;
END;
$$ LANGUAGE plpgsql;

-- Select edges connected to a chunk (both directions, considering bidirectional)
CREATE OR REPLACE FUNCTION select_edges_connected_to_chunk(
    input_chunk_id BIGINT,
    input_edge_type edge_type DEFAULT NULL
)
RETURNS TABLE (
    output_id BIGINT,
    output_source_chunk_id BIGINT,
    output_target_chunk_id BIGINT,
    output_source_entity_id BIGINT,
    output_target_entity_id BIGINT,
    output_edge_type edge_type,
    output_weight FLOAT,
    output_bidirectional BOOLEAN,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE,
    output_is_outgoing BOOLEAN
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        id,
        source_chunk_id,
        target_chunk_id,
        source_entity_id,
        target_entity_id,
        edge_type,
        weight,
        bidirectional,
        metadata,
        created_at,
        TRUE as is_outgoing
    FROM edges
    WHERE source_chunk_id = input_chunk_id
        AND (input_edge_type IS NULL OR edge_type = input_edge_type)
    UNION ALL
    SELECT 
        id,
        source_chunk_id,
        target_chunk_id,
        source_entity_id,
        target_entity_id,
        edge_type,
        weight,
        bidirectional,
        metadata,
        created_at,
        FALSE as is_outgoing
    FROM edges
    WHERE target_chunk_id = input_chunk_id
        AND (input_edge_type IS NULL OR edge_type = input_edge_type)
        AND bidirectional = TRUE
    ORDER BY weight DESC, created_at;
END;
$$ LANGUAGE plpgsql;

-- Select edges from an entity
CREATE OR REPLACE FUNCTION select_edges_from_entity(
    input_entity_id BIGINT,
    input_edge_type edge_type DEFAULT NULL
)
RETURNS TABLE (
    output_id BIGINT,
    output_source_chunk_id BIGINT,
    output_target_chunk_id BIGINT,
    output_source_entity_id BIGINT,
    output_target_entity_id BIGINT,
    output_edge_type edge_type,
    output_weight FLOAT,
    output_bidirectional BOOLEAN,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        id,
        source_chunk_id,
        target_chunk_id,
        source_entity_id,
        target_entity_id,
        edge_type,
        weight,
        bidirectional,
        metadata,
        created_at
    FROM edges
    WHERE source_entity_id = input_entity_id
        AND (input_edge_type IS NULL OR edge_type = input_edge_type)
    ORDER BY weight DESC, created_at;
END;
$$ LANGUAGE plpgsql;

-- Select edges to an entity
CREATE OR REPLACE FUNCTION select_edges_to_entity(
    input_entity_id BIGINT,
    input_edge_type edge_type DEFAULT NULL
)
RETURNS TABLE (
    output_id BIGINT,
    output_source_chunk_id BIGINT,
    output_target_chunk_id BIGINT,
    output_source_entity_id BIGINT,
    output_target_entity_id BIGINT,
    output_edge_type edge_type,
    output_weight FLOAT,
    output_bidirectional BOOLEAN,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        id,
        source_chunk_id,
        target_chunk_id,
        source_entity_id,
        target_entity_id,
        edge_type,
        weight,
        bidirectional,
        metadata,
        created_at
    FROM edges
    WHERE target_entity_id = input_entity_id
        AND (input_edge_type IS NULL OR edge_type = input_edge_type)
    ORDER BY weight DESC, created_at;
END;
$$ LANGUAGE plpgsql;

-- BFS traversal from a chunk
-- Returns chunks reachable within max_depth hops
CREATE OR REPLACE FUNCTION traverse_bfs_from_chunk(
    input_start_chunk_id BIGINT,
    input_max_depth INT,
    input_edge_type edge_type DEFAULT NULL
)
RETURNS TABLE (
    output_chunk_id BIGINT,
    output_depth INT,
    output_path BIGINT[]
)
AS $$
BEGIN
    RETURN QUERY
    WITH RECURSIVE traversal AS (
        -- Base case: start chunk
        SELECT 
            input_start_chunk_id as chunk_id,
            0 as depth,
            ARRAY[input_start_chunk_id] as path
        
        UNION
        
        -- Recursive case: follow edges
        SELECT DISTINCT
            CASE 
                WHEN e.source_chunk_id = t.chunk_id THEN e.target_chunk_id
                WHEN e.target_chunk_id = t.chunk_id AND e.bidirectional THEN e.source_chunk_id
            END as chunk_id,
            t.depth + 1 as depth,
            t.path || CASE 
                WHEN e.source_chunk_id = t.chunk_id THEN e.target_chunk_id
                WHEN e.target_chunk_id = t.chunk_id AND e.bidirectional THEN e.source_chunk_id
            END as path
        FROM traversal t
        JOIN edges e ON (
            (e.source_chunk_id = t.chunk_id)
            OR (e.target_chunk_id = t.chunk_id AND e.bidirectional)
        )
        WHERE t.depth < input_max_depth
            AND (input_edge_type IS NULL OR e.edge_type = input_edge_type)
            AND CASE 
                WHEN e.source_chunk_id = t.chunk_id THEN e.target_chunk_id
                WHEN e.target_chunk_id = t.chunk_id AND e.bidirectional THEN e.source_chunk_id
            END IS NOT NULL
            AND NOT (CASE 
                WHEN e.source_chunk_id = t.chunk_id THEN e.target_chunk_id
                WHEN e.target_chunk_id = t.chunk_id AND e.bidirectional THEN e.source_chunk_id
            END = ANY(t.path))  -- Prevent cycles
    )
    SELECT chunk_id, depth, path
    FROM traversal
    ORDER BY depth, chunk_id;
END;
$$ LANGUAGE plpgsql;
