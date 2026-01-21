-- Documents SQL Functions

-- Initialize documents table and related objects
CREATE OR REPLACE FUNCTION init_documents() RETURNS VOID AS $$
BEGIN
    -- Create documents table
    CREATE TABLE IF NOT EXISTS documents (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        rid UUID UNIQUE DEFAULT gen_random_uuid(),
        title TEXT NOT NULL,
        source TEXT,
        metadata JSONB DEFAULT '{}',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    
    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_documents_metadata ON documents USING GIN (metadata);
    
    -- Create trigger function for updated_at
    CREATE OR REPLACE FUNCTION update_updated_at_column()
    RETURNS TRIGGER AS $trigger$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $trigger$ LANGUAGE plpgsql;
    
    -- Create trigger for documents
    DROP TRIGGER IF EXISTS update_documents_updated_at ON documents;
    CREATE TRIGGER update_documents_updated_at BEFORE UPDATE ON documents
        FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
END;
$$ LANGUAGE plpgsql;

-- Insert a new document
CREATE OR REPLACE FUNCTION insert_document(
    input_title TEXT,
    input_source TEXT,
    input_metadata JSONB
)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_title TEXT,
    output_source TEXT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE,
    output_updated_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    INSERT INTO documents (title, source, metadata)
    VALUES (input_title, input_source, input_metadata)
    RETURNING 
        id,
        rid,
        title, 
        source, 
        metadata, 
        created_at, 
        updated_at;
END;
$$ LANGUAGE plpgsql;

-- Select document by ID
CREATE OR REPLACE FUNCTION select_document(input_rid UUID)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_title TEXT,
    output_source TEXT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE,
    output_updated_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        id,
        rid,
        title, 
        source, 
        metadata, 
        created_at, 
        updated_at
    FROM documents
    WHERE rid = input_rid;
END;
$$ LANGUAGE plpgsql;

-- Select all documents with pagination
CREATE OR REPLACE FUNCTION select_all_documents(
    input_last_created_at TIMESTAMP WITH TIME ZONE,
    input_limit INT
)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_title TEXT,
    output_source TEXT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE,
    output_updated_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        id,
        rid,
        title, 
        source, 
        metadata, 
        created_at, 
        updated_at
    FROM documents
    WHERE (input_last_created_at IS NULL OR created_at < input_last_created_at)
    ORDER BY created_at DESC
    LIMIT input_limit;
END;
$$ LANGUAGE plpgsql;

-- Search documents by title or source
CREATE OR REPLACE FUNCTION search_documents(
    input_search TEXT,
    input_limit INT
)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_title TEXT,
    output_source TEXT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE,
    output_updated_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        id,
        rid,
        title, 
        source, 
        metadata, 
        created_at, 
        updated_at
    FROM documents
    WHERE 
        title ILIKE '%' || input_search || '%'
        OR source ILIKE '%' || input_search || '%'
    ORDER BY created_at DESC
    LIMIT input_limit;
END;
$$ LANGUAGE plpgsql;

-- Update document
CREATE OR REPLACE FUNCTION update_document(
    input_rid UUID,
    input_title TEXT,
    input_source TEXT,
    input_metadata JSONB
)
RETURNS TABLE (
    output_id BIGINT,
    output_rid UUID,
    output_title TEXT,
    output_source TEXT,
    output_metadata JSONB,
    output_created_at TIMESTAMP WITH TIME ZONE,
    output_updated_at TIMESTAMP WITH TIME ZONE
)
AS $$
BEGIN
    RETURN QUERY
    UPDATE documents
    SET 
        title = input_title,
        source = input_source,
        metadata = input_metadata
    WHERE rid = input_rid
    RETURNING 
        id,
        rid,
        title, 
        source, 
        metadata, 
        created_at, 
        updated_at;
END;
$$ LANGUAGE plpgsql;

-- Delete document (cascades to chunks)
CREATE OR REPLACE FUNCTION delete_document(input_rid UUID)
RETURNS VOID
AS $$
BEGIN
    DELETE FROM documents WHERE rid = input_rid;
END;
$$ LANGUAGE plpgsql;
