"""Clear incorrect data from permits table

Revision ID: 008_clear_incorrect_data
Revises: 007_fix_status_no_api_no_swap
Create Date: 2025-01-23 19:50:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '008_clear_incorrect_data'
down_revision = '007_fix_status_no_api_no_swap'
branch_labels = None
depends_on = None


def upgrade():
    """Clear the incorrect data from permits table."""
    # The current data has API numbers in both status_no and api_no columns
    # This is incorrect - we need to clear this data and let the scraper populate it correctly
    
    # Clear the incorrect data (keep the table structure)
    op.execute("DELETE FROM permits WHERE id > 1")  # Keep the header row (id=1)
    
    # Reset the sequence to start from 1
    op.execute("ALTER SEQUENCE permits_new_id_seq1 RESTART WITH 1")


def downgrade():
    """Revert the data clearing."""
    # This is a no-op since we can't restore the deleted data
    pass
