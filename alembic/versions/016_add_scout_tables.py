"""Add Scout v2.1 tables

Revision ID: 016_add_scout_tables
Revises: 015_add_org_id_to_field_corrections
Create Date: 2025-09-30 18:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '016_add_scout_tables'
down_revision = '015_add_org_id_to_field_corrections'
branch_labels = None
depends_on = None

def upgrade():
    # Create enums
    claim_type_enum = postgresql.ENUM('confirmed', 'likely', 'rumor', 'speculation', name='claimtype')
    claim_type_enum.create(op.get_bind())
    
    confidence_level_enum = postgresql.ENUM('low', 'medium', 'high', name='confidencelevel')
    confidence_level_enum.create(op.get_bind())
    
    insight_user_state_enum = postgresql.ENUM('default', 'kept', 'dismissed', 'archived', name='insightuserstate')
    insight_user_state_enum.create(op.get_bind())
    
    # Create signals table
    op.create_table('signals',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False, default=sa.text('gen_random_uuid()')),
        sa.Column('org_id', sa.String(length=50), nullable=False),
        sa.Column('found_at', sa.DateTime(timezone=True), nullable=False, default=sa.text('now()')),
        sa.Column('source_url', sa.Text(), nullable=False),
        sa.Column('source_type', sa.String(length=50), nullable=False),
        sa.Column('state', sa.String(length=2), nullable=True),
        sa.Column('county', sa.String(length=100), nullable=True),
        sa.Column('play_basin', sa.String(length=100), nullable=True),
        sa.Column('operators', postgresql.ARRAY(sa.String()), nullable=False, default='{}'),
        sa.Column('unit_tokens', postgresql.ARRAY(sa.String()), nullable=False, default='{}'),
        sa.Column('keywords', postgresql.ARRAY(sa.String()), nullable=False, default='{}'),
        sa.Column('claim_type', claim_type_enum, nullable=False, default='rumor'),
        sa.Column('timeframe', sa.String(length=100), nullable=True),
        sa.Column('summary', sa.Text(), nullable=False),
        sa.Column('raw_excerpt', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, default=sa.text('now()')),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create indexes for signals
    op.create_index('idx_signals_org_found_at', 'signals', ['org_id', 'found_at'])
    op.create_index('idx_signals_org_county', 'signals', ['org_id', 'county'])
    op.create_index('idx_signals_operators_gin', 'signals', ['operators'], postgresql_using='gin')
    op.create_index('idx_signals_unit_tokens_gin', 'signals', ['unit_tokens'], postgresql_using='gin')
    op.create_index('idx_signals_keywords_gin', 'signals', ['keywords'], postgresql_using='gin')
    op.create_index(op.f('ix_signals_org_id'), 'signals', ['org_id'])
    op.create_index(op.f('ix_signals_county'), 'signals', ['county'])
    
    # Create scout_insights table
    op.create_table('scout_insights',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False, default=sa.text('gen_random_uuid()')),
        sa.Column('org_id', sa.String(length=50), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, default=sa.text('now()')),
        sa.Column('title', sa.String(length=255), nullable=False),
        sa.Column('what_happened', sa.JSON(), nullable=False, default='[]'),
        sa.Column('why_it_matters', sa.JSON(), nullable=False, default='[]'),
        sa.Column('confidence', confidence_level_enum, nullable=False, default='medium'),
        sa.Column('confidence_reasons', sa.JSON(), nullable=False, default='[]'),
        sa.Column('next_checks', sa.JSON(), nullable=False, default='[]'),
        sa.Column('source_urls', sa.JSON(), nullable=False, default='[]'),
        sa.Column('related_permit_ids', postgresql.ARRAY(sa.String()), nullable=False, default='{}'),
        sa.Column('county', sa.String(length=50), nullable=True),
        sa.Column('state', sa.String(length=50), nullable=True),
        sa.Column('operator_keys', postgresql.ARRAY(sa.String()), nullable=False, default='{}'),
        sa.Column('analytics', sa.JSON(), nullable=False, default='{}'),
        sa.Column('dedup_key', sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('dedup_key', name='uq_scout_insights_dedup_key')
    )
    
    # Create indexes for scout_insights
    op.create_index('idx_scout_insights_org_created', 'scout_insights', ['org_id', 'created_at'])
    op.create_index('idx_scout_insights_org_county', 'scout_insights', ['org_id', 'county'])
    op.create_index('idx_scout_insights_operator_keys_gin', 'scout_insights', ['operator_keys'], postgresql_using='gin')
    op.create_index(op.f('ix_scout_insights_org_id'), 'scout_insights', ['org_id'])
    op.create_index(op.f('ix_scout_insights_county'), 'scout_insights', ['county'])
    op.create_index(op.f('ix_scout_insights_operator_keys'), 'scout_insights', ['operator_keys'])
    op.create_index(op.f('ix_scout_insights_dedup_key'), 'scout_insights', ['dedup_key'])
    
    # Create scout_insight_user_state table
    op.create_table('scout_insight_user_state',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False, default=sa.text('gen_random_uuid()')),
        sa.Column('org_id', sa.String(length=50), nullable=False),
        sa.Column('user_id', sa.String(length=50), nullable=False),
        sa.Column('insight_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('state', insight_user_state_enum, nullable=False, default='default'),
        sa.Column('kept_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('dismissed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('dismiss_reason', sa.Text(), nullable=True),
        sa.Column('archived_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('undo_token', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('undo_expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, default=sa.text('now()')),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False, default=sa.text('now()')),
        sa.ForeignKeyConstraint(['insight_id'], ['scout_insights.id'], ),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('org_id', 'user_id', 'insight_id', name='_org_user_insight_uc')
    )
    
    # Create indexes for scout_insight_user_state
    op.create_index('idx_scout_user_state_query', 'scout_insight_user_state', ['org_id', 'user_id', 'state', 'insight_id'])
    op.create_index('idx_scout_user_state_archive', 'scout_insight_user_state', ['org_id', 'user_id', 'archived_at'])

def downgrade():
    # Drop tables
    op.drop_table('scout_insight_user_state')
    op.drop_table('scout_insights')
    op.drop_table('signals')
    
    # Drop enums
    insight_user_state_enum = postgresql.ENUM(name='insightuserstate')
    insight_user_state_enum.drop(op.get_bind())
    
    confidence_level_enum = postgresql.ENUM(name='confidencelevel')
    confidence_level_enum.drop(op.get_bind())
    
    claim_type_enum = postgresql.ENUM(name='claimtype')
    claim_type_enum.drop(op.get_bind())
