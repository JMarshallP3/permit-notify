# Alembic Migrations

This directory contains database migration scripts managed by Alembic.

## Commands

### Create a new migration
```bash
alembic revision --autogenerate -m "description of changes"
```

### Apply migrations
```bash
alembic upgrade head
```

### Check current migration status
```bash
alembic current
```

### Show migration history
```bash
alembic history
```

## Initial Setup

1. Set the `DATABASE_URL` environment variable
2. Create initial migration: `alembic revision --autogenerate -m "init"`
3. Apply migrations: `alembic upgrade head`

## Using the Migration Script

You can also use the provided migration script:

```bash
python tools/migrate.py
```

This script will automatically run `alembic upgrade head` with proper error handling.
