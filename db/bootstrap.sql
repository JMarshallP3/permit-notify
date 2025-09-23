-- db/bootstrap.sql
-- 1) Create the application role (safe if re-run)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'permit_app') THEN
    CREATE ROLE permit_app LOGIN PASSWORD 'CHANGE_ME_STRONG';
  END IF;
END$$;

-- 2) Ensure a dedicated schema
CREATE SCHEMA IF NOT EXISTS permits AUTHORIZATION permit_app;

-- 3) Default privileges so new tables are owned/readable by the app
ALTER ROLE permit_app SET search_path = permits, public;

-- 4) Make sure our app can create objects in the schema
GRANT USAGE, CREATE ON SCHEMA permits TO permit_app;

-- 5) Optional: if you created tables before under public, grant read
GRANT SELECT ON ALL TABLES IN SCHEMA public TO permit_app;
