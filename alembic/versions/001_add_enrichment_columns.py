"""Add enrichment columns

Revision ID: 001
Revises: 
Create Date: 2025-01-27 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    """Add enrichment columns to permits table."""
    # Add enrichment columns
    op.add_column('permits', sa.Column('w1_field_name', sa.String(200), nullable=True))
    op.add_column('permits', sa.Column('w1_well_count', sa.Integer(), nullable=True))
    op.add_column('permits', sa.Column('w1_pdf_url', sa.String(500), nullable=True))
    op.add_column('permits', sa.Column('w1_parse_status', sa.String(50), nullable=True))
    op.add_column('permits', sa.Column('w1_parse_confidence', sa.Float(), nullable=True))
    op.add_column('permits', sa.Column('w1_text_snippet', sa.Text(), nullable=True))
    op.add_column('permits', sa.Column('w1_last_enriched_at', sa.DateTime(), nullable=True))
    
    # Create indexes for better performance
    op.create_index('ix_permits_w1_parse_status', 'permits', ['w1_parse_status'])
    op.create_index('ix_permits_w1_last_enriched_at', 'permits', ['w1_last_enriched_at'])


def downgrade():
    """Remove enrichment columns from permits table."""
    # Drop indexes
    op.drop_index('ix_permits_w1_last_enriched_at', table_name='permits')
    op.drop_index('ix_permits_w1_parse_status', table_name='permits')
    
    # Drop columns
    op.drop_column('permits', 'w1_last_enriched_at')
    op.drop_column('permits', 'w1_text_snippet')
    op.drop_column('permits', 'w1_parse_confidence')
    op.drop_column('permits', 'w1_parse_status')
    op.drop_column('permits', 'w1_pdf_url')
    op.drop_column('permits', 'w1_well_count')
    op.drop_column('permits', 'w1_field_name')
