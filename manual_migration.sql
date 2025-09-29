-- Manual migration for tenant isolation and events
-- Run this directly on Railway database

-- Step 1: Add org_id and version columns to permits table
ALTER TABLE permits ADD COLUMN IF NOT EXISTS org_id VARCHAR(50) NOT NULL DEFAULT 'default_org';
ALTER TABLE permits ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1;

-- Step 2: Create indexes for tenant isolation
CREATE INDEX IF NOT EXISTS idx_permit_org_status_no ON permits (org_id, status_no);
CREATE INDEX IF NOT EXISTS idx_permit_org_api_no ON permits (org_id, api_no);
CREATE INDEX IF NOT EXISTS idx_permit_org_operator ON permits (org_id, operator_name);
CREATE INDEX IF NOT EXISTS idx_permit_org_county ON permits (org_id, county);
CREATE INDEX IF NOT EXISTS idx_permit_org_district ON permits (org_id, district);
CREATE INDEX IF NOT EXISTS idx_permit_org_status_date ON permits (org_id, status_date);
CREATE INDEX IF NOT EXISTS idx_permit_org_created ON permits (org_id, created_at);
CREATE INDEX IF NOT EXISTS idx_permit_org_updated ON permits (org_id, updated_at);

-- Step 3: Create events table
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    entity VARCHAR(32) NOT NULL,
    entity_id INTEGER NOT NULL,
    org_id VARCHAR(50) NOT NULL,
    payload JSON,
    ts TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

-- Step 4: Create indexes for events table
CREATE INDEX IF NOT EXISTS idx_events_org_entity ON events (org_id, entity, entity_id);
CREATE INDEX IF NOT EXISTS idx_events_org_ts ON events (org_id, ts);

-- Step 5: Add org_id to field_corrections if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'field_corrections') THEN
        ALTER TABLE field_corrections ADD COLUMN IF NOT EXISTS org_id VARCHAR(50) NOT NULL DEFAULT 'default_org';
        CREATE INDEX IF NOT EXISTS ix_field_corrections_org_id ON field_corrections (org_id);
    END IF;
END $$;

-- Step 6: Backfill snapshot events for existing permits
INSERT INTO events (type, entity, entity_id, org_id, payload, ts)
SELECT 'snapshot', 'permit', p.id, p.org_id, 
       json_build_object('id', p.id, 'status_no', p.status_no), 
       now()
FROM permits p
WHERE NOT EXISTS (
    SELECT 1 FROM events e 
    WHERE e.entity = 'permit' AND e.entity_id = p.id AND e.type = 'snapshot'
);

-- Step 7: Update alembic version table
INSERT INTO alembic_version (version_num) VALUES ('013_add_tenant_isolation_and_events')
ON CONFLICT (version_num) DO NOTHING;
