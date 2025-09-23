-- db/migrations/002_add_fingerprint_constraint.sql
SET search_path TO permits, public;

-- Add unique constraint to fingerprint column if it doesn't exist
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.table_constraints
    WHERE table_schema = 'permits'
      AND table_name = 'permits_raw'
      AND constraint_name = 'permits_raw_fingerprint_key'
  ) THEN
    ALTER TABLE permits.permits_raw ADD CONSTRAINT permits_raw_fingerprint_key UNIQUE (fingerprint);
  END IF;
END$$;
