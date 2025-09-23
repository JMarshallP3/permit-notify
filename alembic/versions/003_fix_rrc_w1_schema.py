"""Fix RRC W-1 schema migration

Revision ID: 003_fix_rrc_w1_schema
Revises: 002_upgrade_to_rrc_w1_schema
Create Date: 2025-09-23 14:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '003_fix_rrc_w1_schema'
down_revision = '002_upgrade_to_rrc_w1_schema'
branch_labels = None
depends_on = None


def upgrade():
    """Fix RRC W-1 schema by handling existing columns."""
    
    # Check if columns exist and add them if they don't
    connection = op.get_bind()
    
    # Get existing columns
    result = connection.execute(sa.text("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'permits' AND table_schema = 'public'
    """))
    existing_columns = [row[0] for row in result]
    
    # Add missing columns
    if 'status_date' not in existing_columns:
        op.add_column('permits', sa.Column('status_date', sa.Date(), nullable=True))
    
    if 'status_no' not in existing_columns:
        op.add_column('permits', sa.Column('status_no', sa.String(length=50), nullable=True))
    
    if 'operator_name' not in existing_columns:
        op.add_column('permits', sa.Column('operator_name', sa.String(length=200), nullable=True))
    
    if 'operator_number' not in existing_columns:
        op.add_column('permits', sa.Column('operator_number', sa.String(length=50), nullable=True))
    
    if 'lease_name' not in existing_columns:
        op.add_column('permits', sa.Column('lease_name', sa.String(length=200), nullable=True))
    
    if 'well_no' not in existing_columns:
        op.add_column('permits', sa.Column('well_no', sa.String(length=50), nullable=True))
    
    if 'wellbore_profile' not in existing_columns:
        op.add_column('permits', sa.Column('wellbore_profile', sa.String(length=50), nullable=True))
    
    if 'filing_purpose' not in existing_columns:
        op.add_column('permits', sa.Column('filing_purpose', sa.String(length=100), nullable=True))
    
    if 'amend' not in existing_columns:
        op.add_column('permits', sa.Column('amend', sa.Boolean(), nullable=True))
    
    if 'total_depth' not in existing_columns:
        op.add_column('permits', sa.Column('total_depth', sa.Numeric(precision=10, scale=2), nullable=True))
    
    if 'stacked_lateral_parent_well_dp' not in existing_columns:
        op.add_column('permits', sa.Column('stacked_lateral_parent_well_dp', sa.String(length=100), nullable=True))
    
    if 'current_queue' not in existing_columns:
        op.add_column('permits', sa.Column('current_queue', sa.String(length=100), nullable=True))
    
    if 'updated_at' not in existing_columns:
        op.add_column('permits', sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False))
    
    # Create indexes if they don't exist
    try:
        op.create_index('idx_permit_status_no', 'permits', ['status_no'])
    except:
        pass  # Index already exists
    
    try:
        op.create_index('idx_permit_operator_name', 'permits', ['operator_name'])
    except:
        pass  # Index already exists
    
    try:
        op.create_index('idx_permit_status_date', 'permits', ['status_date'])
    except:
        pass  # Index already exists
    
    # Update existing records to populate new fields from legacy fields
    op.execute("""
        UPDATE permits SET 
            status_no = COALESCE(status_no, permit_no),
            operator_name = COALESCE(operator_name, operator),
            well_no = COALESCE(well_no, well_name),
            lease_name = COALESCE(lease_name, lease_no),
            status_date = COALESCE(status_date, submission_date),
            updated_at = COALESCE(updated_at, created_at)
        WHERE status_no IS NULL OR status_no = ''
    """)
    
    # Make status_no NOT NULL after populating data
    try:
        op.alter_column('permits', 'status_no', nullable=False)
    except:
        pass  # Already NOT NULL
    
    # Add unique constraint on status_no if it doesn't exist
    try:
        op.create_unique_constraint('uq_permit_status_no', 'permits', ['status_no'])
    except:
        pass  # Constraint already exists


def downgrade():
    """Downgrade database schema back to legacy format."""
    
    # Remove unique constraint
    try:
        op.drop_constraint('uq_permit_status_no', 'permits', type_='unique')
    except:
        pass
    
    # Remove indexes
    try:
        op.drop_index('idx_permit_status_date', 'permits')
    except:
        pass
    
    try:
        op.drop_index('idx_permit_operator_name', 'permits')
    except:
        pass
    
    try:
        op.drop_index('idx_permit_status_no', 'permits')
    except:
        pass
    
    # Remove new columns
    try:
        op.drop_column('permits', 'updated_at')
    except:
        pass
    
    try:
        op.drop_column('permits', 'current_queue')
    except:
        pass
    
    try:
        op.drop_column('permits', 'stacked_lateral_parent_well_dp')
    except:
        pass
    
    try:
        op.drop_column('permits', 'total_depth')
    except:
        pass
    
    try:
        op.drop_column('permits', 'amend')
    except:
        pass
    
    try:
        op.drop_column('permits', 'filing_purpose')
    except:
        pass
    
    try:
        op.drop_column('permits', 'wellbore_profile')
    except:
        pass
    
    try:
        op.drop_column('permits', 'well_no')
    except:
        pass
    
    try:
        op.drop_column('permits', 'lease_name')
    except:
        pass
    
    try:
        op.drop_column('permits', 'operator_number')
    except:
        pass
    
    try:
        op.drop_column('permits', 'operator_name')
    except:
        pass
    
    try:
        op.drop_column('permits', 'status_no')
    except:
        pass
    
    try:
        op.drop_column('permits', 'status_date')
    except:
        pass
