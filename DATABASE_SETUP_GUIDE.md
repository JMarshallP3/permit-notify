# Database Setup Guide

This guide walks you through setting up the new database structure with proper user management and raw data ingestion.

## Overview

The new setup separates concerns:
- **Raw Data Ingestion**: `permits_raw` table stores all scraped data as-is
- **Normalized Data**: `permits_normalized` table stores cleaned, structured data
- **Event Logging**: `events` table tracks all changes
- **App User**: Dedicated `permit_app` user with limited permissions

## Step 1: Environment Configuration

1. Copy the environment template:
   ```bash
   cp env.template .env
   ```

2. Edit `.env` and fill in your Railway database details:
   ```bash
   # Get these from Railway → Postgres → Connect
   DATABASE_URL=postgresql+psycopg://permit_app:${PERMIT_DB_PASSWORD}@<HOST>:<PORT>/<DBNAME>?sslmode=require
   PERMIT_DB_PASSWORD=your_strong_password_here
   ```

3. In Railway, set the `PERMIT_DB_PASSWORD` environment variable in your service.

## Step 2: Bootstrap Database User

Run the bootstrap script to create the `permit_app` user:

```bash
# Using psql (if you have it locally)
psql "host=<HOST> port=<PORT> dbname=<DBNAME> user=postgres sslmode=require" -f db/bootstrap.sql -v PERMIT_DB_PASSWORD="'$PERMIT_DB_PASSWORD'"

# Or use Railway's SQL console:
# 1. Go to Railway → Postgres → Query
# 2. Paste the contents of db/bootstrap.sql
# 3. Replace :'PERMIT_DB_PASSWORD' with your actual password
```

## Step 3: Run Migrations

Create the database schema:

```bash
python scripts/run_migrations.py
```

This will create:
- `permits_raw` table for raw data ingestion
- `permits_normalized` table for clean data
- `events` table for change tracking
- All necessary indexes

## Step 4: Test Database Connection

```bash
python scripts/test_db.py
```

## Step 5: Run the New Scraper

Use the new scraper that writes to the raw table:

```bash
python save_permits_to_raw.py
```

This will:
1. Scrape permits from RRC W-1
2. Save raw data to `permits_raw` table
3. Generate fingerprints for deduplication
4. Handle errors gracefully

## Step 6: Check Raw Data

You can inspect the raw data:

```python
from app.ingest import get_raw_records, get_raw_record_count

# Get counts by status
counts = get_raw_record_count()
print(f"Raw records: {counts}")

# Get new records
new_records = get_raw_records(status='new', limit=10)
for record in new_records:
    print(f"ID: {record['raw_id']}, Status: {record['status']}")
    print(f"Payload: {record['payload_json']}")
```

## Next Steps

1. **Normalization**: Create a script to process raw data into `permits_normalized`
2. **Deduplication**: Use fingerprints to avoid duplicate processing
3. **Error Handling**: Process failed records and retry
4. **Monitoring**: Set up alerts for scraping failures

## Railway Deployment

For Railway deployment, update your service to:

1. Set environment variables in Railway dashboard
2. Run migrations at startup:
   ```bash
   python scripts/run_migrations.py && python save_permits_to_raw.py
   ```

## Troubleshooting

### Connection Issues
- Ensure `sslmode=require` is in your DATABASE_URL
- Verify the `permit_app` user exists and has correct permissions
- Check that the password is set correctly in Railway

### Migration Issues
- All migrations are idempotent (safe to run multiple times)
- Check the `permits` schema exists
- Verify the `permit_app` user can create tables

### Scraping Issues
- Check the raw table for error records
- Look at the `error_msg` column for details
- Verify the scraper is writing to the correct table
