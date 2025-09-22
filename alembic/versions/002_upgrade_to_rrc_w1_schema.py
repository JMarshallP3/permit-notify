"""Upgrade permit schema to RRC W-1 Search Results format

Revision ID: 002_upgrade_to_rrc_w1_schema
Revises: 001_update_permit_schema
Create Date: 2025-09-22 20:45:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '002_upgrade_to_rrc_w1_schema'
down_revision = '001_update_permit_schema'
branch_labels = None
depends_on = None


def upgrade():
    """Upgrade database schema to RRC W-1 Search Results format."""
    
    # Add new RRC W-1 fields
    op.add_column('permits', sa.Column('status_date', sa.Date(), nullable=True))
    op.add_column('permits', sa.Column('status_no', sa.String(length=50), nullable=True))
    op.add_column('permits', sa.Column('operator_name', sa.String(length=200), nullable=True))
    op.add_column('permits', sa.Column('operator_number', sa.String(length=50), nullable=True))
    op.add_column('permits', sa.Column('lease_name', sa.String(length=200), nullable=True))
    op.add_column('permits', sa.Column('well_no', sa.String(length=50), nullable=True))
    op.add_column('permits', sa.Column('wellbore_profile', sa.String(length=50), nullable=True))
    op.add_column('permits', sa.Column('filing_purpose', sa.String(length=100), nullable=True))
    op.add_column('permits', sa.Column('amend', sa.Boolean(), nullable=True))
    op.add_column('permits', sa.Column('total_depth', sa.Numeric(precision=10, scale=2), nullable=True))
    op.add_column('permits', sa.Column('stacked_lateral_parent_well_dp', sa.String(length=100), nullable=True))
    op.add_column('permits', sa.Column('current_queue', sa.String(length=100), nullable=True))
    op.add_column('permits', sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False))
    
    # Create indexes for new fields
    op.create_index('idx_permit_status_no', 'permits', ['status_no'])
    op.create_index('idx_permit_operator_name', 'permits', ['operator_name'])
    op.create_index('idx_permit_status_date', 'permits', ['status_date'])
    
    # Update existing records to populate new fields from legacy fields
    op.execute("""
        UPDATE permits SET 
            status_no = permit_no,
            operator_name = operator,
            well_no = well_name,
            lease_name = lease_no,
            status_date = submission_date,
            updated_at = created_at
        WHERE status_no IS NULL
    """)
    
    # Make status_no NOT NULL after populating data
    op.alter_column('permits', 'status_no', nullable=False)
    
    # Add unique constraint on status_no
    op.create_unique_constraint('uq_permit_status_no', 'permits', ['status_no'])


def downgrade():
    """Downgrade database schema back to legacy format."""
    
    # Remove unique constraint
    op.drop_constraint('uq_permit_status_no', 'permits', type_='unique')
    
    # Remove indexes
    op.drop_index('idx_permit_status_date', 'permits')
    op.drop_index('idx_permit_operator_name', 'permits')
    op.drop_index('idx_permit_status_no', 'permits')
    
    # Remove new columns
    op.drop_column('permits', 'updated_at')
    op.drop_column('permits', 'current_queue')
    op.drop_column('permits', 'stacked_lateral_parent_well_dp')
    op.drop_column('permits', 'total_depth')
    op.drop_column('permits', 'amend')
    op.drop_column('permits', 'filing_purpose')
    op.drop_column('permits', 'wellbore_profile')
    op.drop_column('permits', 'well_no')
    op.drop_column('permits', 'lease_name')
    op.drop_column('permits', 'operator_number')
    op.drop_column('permits', 'operator_name')
    op.drop_column('permits', 'status_no')
    op.drop_column('permits', 'status_date')
