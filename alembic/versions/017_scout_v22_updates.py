"""Scout v2.2 model updates

Revision ID: 017_scout_v22_updates
Revises: 016_add_scout_tables
Create Date: 2025-09-30 22:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '017_scout_v22_updates'
down_revision = '016_add_scout_tables'
branch_labels = None
depends_on = None

def upgrade():
    # Create new enums for v2.2
    source_type_enum = postgresql.ENUM(
        'forum', 'news', 'pr', 'filing', 'gov_bulletin', 'blog', 'social', 'other',
        name='sourcetype'
    )
    source_type_enum.create(op.get_bind())
    
    timeframe_enum = postgresql.ENUM(
        'past', 'now', 'next_90d',
        name='timeframe'
    )
    timeframe_enum.create(op.get_bind())
    
    # Update signals table - change source_type from string to enum
    op.execute("ALTER TABLE signals ALTER COLUMN source_type TYPE sourcetype USING source_type::sourcetype")
    
    # Update signals table - change timeframe from string to enum
    op.execute("ALTER TABLE signals ALTER COLUMN timeframe TYPE timeframe USING timeframe::timeframe")
    
    # Add updated_at column to scout_insights if not exists
    try:
        op.add_column('scout_insights', sa.Column('updated_at', sa.DateTime(timezone=True), 
                     nullable=False, server_default=sa.text('now()')))
    except:
        pass  # Column might already exist
    
    # Update indexes for better performance
    try:
        op.create_index('idx_signals_source_type', 'signals', ['source_type'])
        op.create_index('idx_signals_timeframe', 'signals', ['timeframe'])
    except:
        pass  # Indexes might already exist

def downgrade():
    # Remove new indexes
    try:
        op.drop_index('idx_signals_timeframe', 'signals')
        op.drop_index('idx_signals_source_type', 'signals')
    except:
        pass
    
    # Revert timeframe to string
    op.execute("ALTER TABLE signals ALTER COLUMN timeframe TYPE varchar(100)")
    
    # Revert source_type to string
    op.execute("ALTER TABLE signals ALTER COLUMN source_type TYPE varchar(50)")
    
    # Drop new enums
    timeframe_enum = postgresql.ENUM(name='timeframe')
    timeframe_enum.drop(op.get_bind())
    
    source_type_enum = postgresql.ENUM(name='sourcetype')
    source_type_enum.drop(op.get_bind())
    
    # Remove updated_at column
    try:
        op.drop_column('scout_insights', 'updated_at')
    except:
        pass
