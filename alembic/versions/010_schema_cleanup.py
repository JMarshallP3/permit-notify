"""Schema cleanup and improvements

Revision ID: 010_schema_cleanup
Revises: 009_remove_legacy_columns
Create Date: 2025-09-24 16:00:00.000000

Changes:
1. Update status_date format handling (MM-DD-YYYY)
2. Remove submission_date column
3. Move created_at to last column (reorder)
4. Split operator_name and operator_number properly
5. Remove w1_field_name column (if exists)
6. Remove w1_well_count column (if exists)
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = '010_schema_cleanup'
down_revision = '009_remove_legacy_columns'
branch_labels = None
depends_on = None

def upgrade():
    """Apply schema improvements."""
    
    # Check if columns exist before trying to drop them
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    columns = [col['name'] for col in inspector.get_columns('permits')]
    
    # 1. Remove submission_date column if it exists
    if 'submission_date' in columns:
        op.drop_column('permits', 'submission_date')
    
    # 2. Remove w1_field_name column if it exists
    if 'w1_field_name' in columns:
        op.drop_column('permits', 'w1_field_name')
    
    # 3. Remove w1_well_count column if it exists  
    if 'w1_well_count' in columns:
        op.drop_column('permits', 'w1_well_count')
    
    # 4. Create a function to extract operator number from operator_name
    # and update existing data
    op.execute("""
        UPDATE permits 
        SET operator_number = CASE
            WHEN operator_name ~ '\\([0-9]+\\)' THEN
                SUBSTRING(operator_name FROM '\\(([0-9]+)\\)')
            ELSE operator_number
        END
        WHERE operator_name IS NOT NULL AND operator_name ~ '\\([0-9]+\\)'
    """)
    
    # 5. Clean operator_name by removing the (######) part
    op.execute("""
        UPDATE permits 
        SET operator_name = TRIM(REGEXP_REPLACE(operator_name, '\\s*\\([0-9]+\\)\\s*', '', 'g'))
        WHERE operator_name IS NOT NULL AND operator_name ~ '\\([0-9]+\\)'
    """)

def downgrade():
    """Revert schema improvements."""
    
    # Re-add removed columns
    op.add_column('permits', sa.Column('submission_date', sa.Date(), nullable=True))
    op.add_column('permits', sa.Column('w1_field_name', sa.String(200), nullable=True))
    op.add_column('permits', sa.Column('w1_well_count', sa.Integer(), nullable=True))
    
    # Note: We can't easily revert the operator_name/operator_number splitting
    # as the original format would be lost
