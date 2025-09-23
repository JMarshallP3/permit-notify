-- db/migrations/001_init.sql
SET search_path TO permits, public;

-- Raw ingest table (append-only)
CREATE TABLE IF NOT EXISTS permits_raw (
  raw_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  scraped_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_url    TEXT NOT NULL,
  payload_json  JSONB NOT NULL,
  fingerprint   TEXT UNIQUE,
  status        TEXT NOT NULL DEFAULT 'new',
  error_msg     TEXT
);

-- Normalized table (source of truth for reads)
CREATE TABLE IF NOT EXISTS permits_normalized (
  permit_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  fingerprint     TEXT UNIQUE,
  api_number      TEXT,
  operator_name   TEXT,
  operator_code   TEXT,
  lease_name      TEXT,
  county          TEXT,
  district        TEXT,
  well_number     TEXT,
  wellbore_profile TEXT,
  lat             DOUBLE PRECISION,
  lon             DOUBLE PRECISION,
  filing_date     DATE,
  approval_date   DATE,
  status          TEXT,
  raw_ref         UUID REFERENCES permits_raw(raw_id),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Events table (immutable change log)
CREATE TABLE IF NOT EXISTS events (
  event_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  entity_type  TEXT NOT NULL,         -- 'permit'
  entity_id    UUID NOT NULL,         -- refers to permits_normalized.permit_id
  event_type   TEXT NOT NULL,         -- 'created' | 'updated'
  diff_json    JSONB,
  source       TEXT NOT NULL          -- 'normalizer'
);

-- Add-on columns guarded with IF NOT EXISTS
-- Replace your previous "ALTER TABLE permits ADD COLUMN status_date DATE"
-- with a safe version below, and aim it at the correct table.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'permits'
      AND table_name   = 'permits_normalized'
      AND column_name  = 'status_date'
  ) THEN
    ALTER TABLE permits.permits_normalized ADD COLUMN status_date DATE;
  END IF;
END$$;

-- Add more columns that match your current schema
DO $$
BEGIN
  -- status_no
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'permits'
      AND table_name   = 'permits_normalized'
      AND column_name  = 'status_no'
  ) THEN
    ALTER TABLE permits.permits_normalized ADD COLUMN status_no TEXT;
  END IF;
  
  -- operator_number
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'permits'
      AND table_name   = 'permits_normalized'
      AND column_name  = 'status_no'
  ) THEN
    ALTER TABLE permits.permits_normalized ADD COLUMN operator_number TEXT;
  END IF;
  
  -- well_no
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'permits'
      AND table_name   = 'permits_normalized'
      AND column_name  = 'well_no'
  ) THEN
    ALTER TABLE permits.permits_normalized ADD COLUMN well_no TEXT;
  END IF;
  
  -- filing_purpose
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'permits'
      AND table_name   = 'permits_normalized'
      AND column_name  = 'filing_purpose'
  ) THEN
    ALTER TABLE permits.permits_normalized ADD COLUMN filing_purpose TEXT;
  END IF;
  
  -- amend
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'permits'
      AND table_name   = 'permits_normalized'
      AND column_name  = 'amend'
  ) THEN
    ALTER TABLE permits.permits_normalized ADD COLUMN amend BOOLEAN;
  END IF;
  
  -- total_depth
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'permits'
      AND table_name   = 'permits_normalized'
      AND column_name  = 'total_depth'
  ) THEN
    ALTER TABLE permits.permits_normalized ADD COLUMN total_depth NUMERIC(10,2);
  END IF;
  
  -- stacked_lateral_parent_well_dp
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'permits'
      AND table_name   = 'permits_normalized'
      AND column_name  = 'stacked_lateral_parent_well_dp'
  ) THEN
    ALTER TABLE permits.permits_normalized ADD COLUMN stacked_lateral_parent_well_dp TEXT;
  END IF;
  
  -- current_queue
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'permits'
      AND table_name   = 'permits_normalized'
      AND column_name  = 'current_queue'
  ) THEN
    ALTER TABLE permits.permits_normalized ADD COLUMN current_queue TEXT;
  END IF;
END$$;

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_permits_raw_scraped_at ON permits_raw(scraped_at);
CREATE INDEX IF NOT EXISTS idx_permits_raw_status ON permits_raw(status);
CREATE INDEX IF NOT EXISTS idx_permits_raw_fingerprint ON permits_raw(fingerprint);

CREATE INDEX IF NOT EXISTS idx_permits_normalized_api_number ON permits_normalized(api_number);
CREATE INDEX IF NOT EXISTS idx_permits_normalized_operator_name ON permits_normalized(operator_name);
CREATE INDEX IF NOT EXISTS idx_permits_normalized_county ON permits_normalized(county);
CREATE INDEX IF NOT EXISTS idx_permits_normalized_district ON permits_normalized(district);
CREATE INDEX IF NOT EXISTS idx_permits_normalized_status_date ON permits_normalized(status_date);
CREATE INDEX IF NOT EXISTS idx_permits_normalized_created_at ON permits_normalized(created_at);

CREATE INDEX IF NOT EXISTS idx_events_entity_type_id ON events(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_events_occurred_at ON events(occurred_at);
