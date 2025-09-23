"""Reorder status_no column to be after status_date in permits table

Revision ID: 006_reorder_status_no
Revises: 005_reorder_status_date
Create Date: 2025-01-23 19:40:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '006_reorder_status_no'
down_revision = '005_reorder_status_date'
branch_labels = None
depends_on = None


def upgrade():
    """Reorder status_no column to be after status_date."""
    # PostgreSQL doesn't support reordering columns directly
    # We need to recreate the table with the new column order
    
    # Create a new table with the desired column order
    op.create_table('permits_new',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('status_date', sa.Date(), nullable=True),
        sa.Column('status_no', sa.String(), nullable=True),
        sa.Column('permit_no', sa.String(), nullable=False),
        sa.Column('operator', sa.String(), nullable=True),
        sa.Column('county', sa.String(), nullable=True),
        sa.Column('district', sa.String(), nullable=True),
        sa.Column('lease_no', sa.String(), nullable=True),
        sa.Column('submission_date', sa.Date(), nullable=True),
        sa.Column('api_no', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('operator_name', sa.String(), nullable=True),
        sa.Column('operator_number', sa.String(), nullable=True),
        sa.Column('lease_name', sa.String(), nullable=True),
        sa.Column('well_no', sa.String(), nullable=True),
        sa.Column('wellbore_profile', sa.String(), nullable=True),
        sa.Column('filing_purpose', sa.String(), nullable=True),
        sa.Column('amend', sa.Boolean(), nullable=True),
        sa.Column('total_depth', sa.Numeric(), nullable=True),
        sa.Column('stacked_lateral_parent_well_dp', sa.String(), nullable=True),
        sa.Column('current_queue', sa.String(), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Copy data from old table to new table
    op.execute("""
        INSERT INTO permits_new (
            id, status_date, status_no, permit_no, operator, county, district, lease_no,
            submission_date, api_no, created_at, operator_name,
            operator_number, lease_name, well_no, wellbore_profile, filing_purpose,
            amend, total_depth, stacked_lateral_parent_well_dp, current_queue, updated_at
        )
        SELECT 
            id, status_date, status_no, permit_no, operator, county, district, lease_no,
            submission_date, api_no, created_at, operator_name,
            operator_number, lease_name, well_no, wellbore_profile, filing_purpose,
            amend, total_depth, stacked_lateral_parent_well_dp, current_queue, updated_at
        FROM permits
    """)
    
    # Drop the old table
    op.drop_table('permits')
    
    # Rename the new table to the original name
    op.rename_table('permits_new', 'permits')


def downgrade():
    """Revert the column reordering."""
    # This is a no-op since we can't easily revert column ordering
    # The column will still exist, just in a different position
    pass
