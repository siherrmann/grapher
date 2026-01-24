-- Entities SQL Functions

-- Initialize entities table and related objects
CREATE OR REPLACE FUNCTION init_entities() RETURNS VOID AS $$
BEGIN
    -- Create entities table
    CREATE TABLE IF NOT EXISTS entities (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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
    output_id BIGINT,
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

-- Update entity metadata
CREATE OR REPLACE FUNCTION update_entity_metadata(
    input_id BIGINT,
    input_metadata JSONB
)
RETURNS TABLE (
    output_id BIGINT,
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

-- Delete entity
CREATE OR REPLACE FUNCTION delete_entity(input_id BIGINT)
RETURNS VOID
AS $$
BEGIN
    DELETE FROM entities WHERE id = input_id;
END;
$$ LANGUAGE plpgsql;

-- Select entity by ID
CREATE OR REPLACE FUNCTION select_entity(input_id BIGINT)
RETURNS TABLE (
    output_id BIGINT,
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
    output_id BIGINT,
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
CREATE OR REPLACE FUNCTION select_entities_by_search(
    input_search TEXT,
    input_entity_type TEXT DEFAULT NULL,
    input_limit INT DEFAULT 100
)
RETURNS TABLE (
    output_id BIGINT,
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
    output_id BIGINT,
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
