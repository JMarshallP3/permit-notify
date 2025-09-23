"""add_permit_enrichment_fields

Revision ID: 002
Revises: 001
Create Date: 2025-01-27 11:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '002'
down_revision = '001'
branch_labels = None
depends_on = None


def upgrade():
    """Add permit enrichment fields to permits table."""
    
    # HTML detail page fields
    op.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS horizontal_wellbore TEXT;")
    op.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS field_name TEXT;")
    op.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS acres NUMERIC(12,2);")
    op.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS section TEXT;")
    op.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS block TEXT;")
    op.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS survey TEXT;")
    op.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS abstract_no TEXT;")
    
    # PDF fields & bookkeeping
    op.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS reservoir_well_count INT;")
    op.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS w1_pdf_url TEXT;")
    op.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS w1_parse_status TEXT;")
    op.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS w1_parse_confidence NUMERIC;")
    op.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS w1_text_snippet TEXT;")
    op.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS w1_last_enriched_at TIMESTAMPTZ;")
    
    # Ensure detail_url exists
    op.execute("ALTER TABLE permits ADD COLUMN IF NOT EXISTS detail_url TEXT;")
    
    # Create indexes for better performance
    op.execute("CREATE INDEX IF NOT EXISTS ix_permits_w1_parse_status ON permits (w1_parse_status);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_permits_w1_last_enriched_at ON permits (w1_last_enriched_at);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_permits_field_name ON permits (field_name);")
    op.execute("CREATE INDEX IF NOT EXISTS ix_permits_detail_url ON permits (detail_url);")


def downgrade():
    """Remove permit enrichment fields from permits table."""
    
    # Drop indexes if they exist
    op.execute("DROP INDEX IF EXISTS ix_permits_detail_url;")
    op.execute("DROP INDEX IF EXISTS ix_permits_field_name;")
    op.execute("DROP INDEX IF EXISTS ix_permits_w1_last_enriched_at;")
    op.execute("DROP INDEX IF EXISTS ix_permits_w1_parse_status;")
    
    # Drop columns if they exist (using IF EXISTS to avoid errors)
    op.execute("ALTER TABLE permits DROP COLUMN IF EXISTS detail_url;")
    op.execute("ALTER TABLE permits DROP COLUMN IF EXISTS w1_last_enriched_at;")
    op.execute("ALTER TABLE permits DROP COLUMN IF EXISTS w1_text_snippet;")
    op.execute("ALTER TABLE permits DROP COLUMN IF EXISTS w1_parse_confidence;")
    op.execute("ALTER TABLE permits DROP COLUMN IF EXISTS w1_parse_status;")
    op.execute("ALTER TABLE permits DROP COLUMN IF EXISTS w1_pdf_url;")
    op.execute("ALTER TABLE permits DROP COLUMN IF EXISTS reservoir_well_count;")
    op.execute("ALTER TABLE permits DROP COLUMN IF EXISTS abstract_no;")
    op.execute("ALTER TABLE permits DROP COLUMN IF EXISTS survey;")
    op.execute("ALTER TABLE permits DROP COLUMN IF EXISTS block;")
    op.execute("ALTER TABLE permits DROP COLUMN IF EXISTS section;")
    op.execute("ALTER TABLE permits DROP COLUMN IF EXISTS acres;")
    op.execute("ALTER TABLE permits DROP COLUMN IF EXISTS field_name;")
    op.execute("ALTER TABLE permits DROP COLUMN IF EXISTS horizontal_wellbore;")
