-- Entities SQL Functions

-- Initialize entities table and related objects
CREATE OR REPLACE FUNCTION init_entities() RETURNS VOID AS $$
BEGIN
    -- Create entities table
    CREATE TABLE IF NOT EXISTS entities (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        metadata JSONB DEFAULT '{}',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        
        UNIQUE(name, entity_type)
    );
    
    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(name);
    CREATE INDEX IF NOT EXISTS idx_entities_type ON entities(entity_type);
END;
$$ LANGUAGE plpgsql;

-- Insert a new entity
CREATE OR REPLACE FUNCTION insert_entity(
    input_name TEXT,
    input_entity_type TEXT,
    input_metadata JSONB
)
RETURNS TABLE (
    output_id UUID,
    output_name TEXT,
    output_entity_type TEXT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    INSERT INTO entities (name, entity_type, metadata)
    VALUES (input_name, input_entity_type, input_metadata)
    ON CONFLICT (name, entity_type) DO UPDATE
        SET metadata = EXCLUDED.metadata
    RETURNING 
        id, 
        name, 
        entity_type, 
        metadata, 
        created_at;
END;
$$ LANGUAGE plpgsql;

-- Select entity by ID
CREATE OR REPLACE FUNCTION select_entity(input_id UUID)
RETURNS TABLE (
    output_id UUID,
    output_name TEXT,
    output_entity_type TEXT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        id, 
        name, 
        entity_type, 
        metadata, 
        created_at
    FROM entities
    WHERE id = input_id;
END;
$$ LANGUAGE plpgsql;

-- Select entity by name and type
CREATE OR REPLACE FUNCTION select_entity_by_name(
    input_name TEXT,
    input_entity_type TEXT
)
RETURNS TABLE (
    output_id UUID,
    output_name TEXT,
    output_entity_type TEXT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        id, 
        name, 
        entity_type, 
        metadata, 
        created_at
    FROM entities
    WHERE name = input_name AND entity_type = input_entity_type;
END;
$$ LANGUAGE plpgsql;

-- Search entities by name pattern
CREATE OR REPLACE FUNCTION search_entities(
    input_search TEXT,
    input_entity_type TEXT DEFAULT NULL,
    input_limit INT DEFAULT 100
)
RETURNS TABLE (
    output_id UUID,
    output_name TEXT,
    output_entity_type TEXT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        id, 
        name, 
        entity_type, 
        metadata, 
        created_at
    FROM entities
    WHERE name ILIKE '%' || input_search || '%'
        AND (input_entity_type IS NULL OR entity_type = input_entity_type)
    ORDER BY name
    LIMIT input_limit;
END;
$$ LANGUAGE plpgsql;

-- Select all entities by type
CREATE OR REPLACE FUNCTION select_entities_by_type(
    input_entity_type TEXT,
    input_limit INT DEFAULT 100
)
RETURNS TABLE (
    output_id UUID,
    output_name TEXT,
    output_entity_type TEXT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        id, 
        name, 
        entity_type, 
        metadata, 
        created_at
    FROM entities
    WHERE entity_type = input_entity_type
    ORDER BY name
    LIMIT input_limit;
END;
$$ LANGUAGE plpgsql;

-- Delete entity
CREATE OR REPLACE FUNCTION delete_entity(input_id UUID)
RETURNS VOID
AS $$
BEGIN
    DELETE FROM entities WHERE id = input_id;
END;
$$ LANGUAGE plpgsql;

-- Update entity metadata
CREATE OR REPLACE FUNCTION update_entity_metadata(
    input_id UUID,
    input_metadata JSONB
)
RETURNS TABLE (
    output_id UUID,
    output_name TEXT,
    output_entity_type TEXT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    UPDATE entities
    SET metadata = input_metadata
    WHERE id = input_id
    RETURNING 
        id, 
        name, 
        entity_type, 
        metadata, 
        created_at;
END;
$$ LANGUAGE plpgsql;

-- Get chunks that mention an entity
CREATE OR REPLACE FUNCTION select_chunks_mentioning_entity(
    input_entity_id UUID
)
RETURNS TABLE (
    output_chunk_id UUID,
    output_edge_id UUID,
    output_edge_metadata JSONB
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        e.source_chunk_id as chunk_id,
        e.id as edge_id,
        e.metadata as edge_metadata
    FROM edges e
    WHERE e.target_entity_id = input_entity_id
        AND e.edge_type = 'entity_mention'
        AND e.source_chunk_id IS NOT NULL
    ORDER BY e.created_at DESC;
END;
$$ LANGUAGE plpgsql;
