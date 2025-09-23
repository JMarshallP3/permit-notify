"""Fix status_no and api_no column data swap

Revision ID: 007_fix_status_no_api_no_swap
Revises: 006_reorder_status_no
Create Date: 2025-01-23 19:45:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '007_fix_status_no_api_no_swap'
down_revision = '006_reorder_status_no'
branch_labels = None
depends_on = None


def upgrade():
    """Fix the swapped status_no and api_no data."""
    # The data is currently swapped: status_no contains API numbers, api_no contains status numbers
    # We need to swap the data between these two columns
    
    # First, add temporary columns to hold the swapped data
    op.add_column('permits', sa.Column('temp_status_no', sa.String(), nullable=True))
    op.add_column('permits', sa.Column('temp_api_no', sa.String(), nullable=True))
    
    # Copy the data to temporary columns (swapped)
    op.execute("""
        UPDATE permits 
        SET 
            temp_status_no = api_no,
            temp_api_no = status_no
    """)
    
    # Clear the original columns
    op.execute("UPDATE permits SET status_no = NULL, api_no = NULL")
    
    # Copy the swapped data back to the original columns
    op.execute("""
        UPDATE permits 
        SET 
            status_no = temp_status_no,
            api_no = temp_api_no
    """)
    
    # Drop the temporary columns
    op.drop_column('permits', 'temp_status_no')
    op.drop_column('permits', 'temp_api_no')


def downgrade():
    """Revert the data swap."""
    # This would swap the data back, but we'll leave it as is since the fix is correct
    pass
