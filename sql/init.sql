-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS ltree;

-- Create edge_type enum if it doesn't exist
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'edge_type') THEN
        CREATE TYPE edge_type AS ENUM (
            'semantic',
            'hierarchical',
            'reference',
            'entity_mention',
            'temporal',
            'causal',
            'custom'
        );
    END IF;
END $$;
