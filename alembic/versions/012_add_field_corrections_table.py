"""Add field corrections table for learning system

Revision ID: 012_add_field_corrections_table
Revises: 011_merge_heads
Create Date: 2025-09-26 20:30:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import func

# revision identifiers, used by Alembic.
revision = '012_add_field_corrections_table'
down_revision = '011_merge_heads'
branch_labels = None
depends_on = None

def upgrade():
    """Add field_corrections table for intelligent field name learning."""
    
    op.create_table(
        'field_corrections',
        sa.Column('id', sa.Integer, primary_key=True),
        
        # Original permit info
        sa.Column('permit_id', sa.Integer, nullable=False),
        sa.Column('status_no', sa.String(50), nullable=False),
        sa.Column('lease_name', sa.String(255)),
        sa.Column('operator_name', sa.String(255)),
        
        # Field name correction
        sa.Column('wrong_field_name', sa.String(255), nullable=False),
        sa.Column('correct_field_name', sa.String(255), nullable=False),
        
        # Learning context
        sa.Column('detail_url', sa.Text),
        sa.Column('html_context', sa.Text),
        
        # Metadata
        sa.Column('corrected_by', sa.String(100), default='user'),
        sa.Column('corrected_at', sa.DateTime, server_default=func.now()),
        sa.Column('applied_to_permit', sa.Boolean, default=False),
        
        # Pattern learning
        sa.Column('extraction_pattern', sa.Text),
        sa.Column('correction_pattern', sa.Text),
    )
    
    # Add indexes for better performance
    op.create_index('idx_field_corrections_permit_id', 'field_corrections', ['permit_id'])
    op.create_index('idx_field_corrections_status_no', 'field_corrections', ['status_no'])
    op.create_index('idx_field_corrections_wrong_field', 'field_corrections', ['wrong_field_name'])
    op.create_index('idx_field_corrections_corrected_at', 'field_corrections', ['corrected_at'])

def downgrade():
    """Remove field_corrections table."""
    
    op.drop_index('idx_field_corrections_corrected_at', 'field_corrections')
    op.drop_index('idx_field_corrections_wrong_field', 'field_corrections')
    op.drop_index('idx_field_corrections_status_no', 'field_corrections')
    op.drop_index('idx_field_corrections_permit_id', 'field_corrections')
    op.drop_table('field_corrections')
