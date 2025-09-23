# PermitNotify

Railway-based multi-service app for Texas permit scraping and notifications.

## Database

This application uses PostgreSQL for persistent storage of permit data.

### Setup

1. Set the `DATABASE_URL` environment variable:
   ```bash
   export DATABASE_URL="postgresql://user:password@localhost:5432/permitdb"
   ```

2. Run the application locally:
   ```bash
   DATABASE_URL="postgresql://user:password@localhost:5432/permitdb" uvicorn app.main:app --host 127.0.0.1 --port 5000
   ```

### Database Schema

The application automatically creates the following table on startup:

- `permits`: Stores permit data with fields for permit number, operator, county, district, well name, lease number, field, submission date, API number, and creation timestamp.

## Migrations

This application uses Alembic for database migrations.

### Setup

1. Set the `DATABASE_URL` environment variable:
   ```bash
   export DATABASE_URL="postgresql://user:password@localhost:5432/permitdb"
   ```

2. Create initial migration:
   ```bash
   alembic revision --autogenerate -m "init"
   ```

3. Apply migrations:
   ```bash
   alembic upgrade head
   ```

### Creating New Migrations

When you make changes to the database models:

1. Create a new migration:
   ```bash
   alembic revision --autogenerate -m "description of changes"
   ```

2. Apply the migration:
   ```bash
   alembic upgrade head
   ```

### Using the Migration Script

You can also use the provided migration script for convenience:

```bash
python tools/migrate.py
```

## Playwright Setup

The scraper uses Playwright as a fallback engine for complex form interactions. To install Playwright browsers:

```bash
python -m playwright install --with-deps chromium
```

This installs Chromium and its dependencies for browser automation.