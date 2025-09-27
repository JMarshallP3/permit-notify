"""add tenant isolation and events table

Revision ID: 013_add_tenant_isolation_and_events
Revises: 012_add_field_corrections_table
Create Date: 2025-09-27 12:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '013_add_tenant_isolation_and_events'
down_revision = '012_add_field_corrections_table'
branch_labels = None
depends_on = None


def upgrade():
    # Add tenant isolation and versioning to permits table
    op.add_column('permits', sa.Column('org_id', sa.String(length=50), nullable=False, server_default='default_org'))
    op.add_column('permits', sa.Column('version', sa.Integer(), nullable=False, server_default='1'))
    
    # Remove server defaults after adding columns
    op.alter_column('permits', 'org_id', server_default=None)
    op.alter_column('permits', 'version', server_default=None)
    
    # Create new tenant-aware indexes for permits
    op.create_index('idx_permit_org_status_no', 'permits', ['org_id', 'status_no'])
    op.create_index('idx_permit_org_api_no', 'permits', ['org_id', 'api_no'])
    op.create_index('idx_permit_org_operator', 'permits', ['org_id', 'operator_name'])
    op.create_index('idx_permit_org_county', 'permits', ['org_id', 'county'])
    op.create_index('idx_permit_org_district', 'permits', ['org_id', 'district'])
    op.create_index('idx_permit_org_status_date', 'permits', ['org_id', 'status_date'])
    op.create_index('idx_permit_org_created', 'permits', ['org_id', 'created_at'])
    op.create_index('idx_permit_org_updated', 'permits', ['org_id', 'updated_at'])
    
    # Drop old indexes (they'll be replaced by tenant-aware ones)
    op.drop_index('idx_permit_status_no', table_name='permits')
    op.drop_index('idx_permit_api_no', table_name='permits')
    op.drop_index('idx_permit_operator_name', table_name='permits')
    op.drop_index('idx_permit_county', table_name='permits')
    op.drop_index('idx_permit_district', table_name='permits')
    op.drop_index('idx_permit_status_date', table_name='permits')
    op.drop_index('idx_permit_created', table_name='permits')
    
    # Create events table
    op.create_table('events',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('type', sa.String(length=32), nullable=False),
        sa.Column('entity', sa.String(length=32), nullable=False),
        sa.Column('entity_id', sa.Integer(), nullable=False),
        sa.Column('org_id', sa.String(length=50), nullable=False),
        sa.Column('payload', sa.JSON(), nullable=True),
        sa.Column('ts', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_events_org_entity', 'events', ['org_id', 'entity', 'entity_id'])
    op.create_index('idx_events_org_ts', 'events', ['org_id', 'ts'])
    op.create_index(op.f('ix_events_id'), 'events', ['id'], unique=False)
    op.create_index(op.f('ix_events_org_id'), 'events', ['org_id'], unique=False)
    
    # Add tenant isolation to field_corrections if it exists
    try:
        op.add_column('field_corrections', sa.Column('org_id', sa.String(length=50), nullable=False, server_default='default_org'))
        op.alter_column('field_corrections', 'org_id', server_default=None)
        op.create_index('ix_field_corrections_org_id', 'field_corrections', ['org_id'])
    except Exception:
        # Table might not exist yet, that's okay
        pass
    
    # Backfill snapshot events for existing permits (keeps clients consistent)
    op.execute("""
        INSERT INTO events (type, entity, entity_id, org_id, payload, ts)
        SELECT 'snapshot', 'permit', p.id, p.org_id, 
               json_build_object('id', p.id, 'status_no', p.status_no), 
               now()
        FROM permits p;
    """)


def downgrade():
    # Drop events table
    op.drop_index(op.f('ix_events_org_id'), table_name='events')
    op.drop_index(op.f('ix_events_id'), table_name='events')
    op.drop_index('idx_events_org_ts', table_name='events')
    op.drop_index('idx_events_org_entity', table_name='events')
    op.drop_table('events')
    
    # Remove tenant isolation from field_corrections
    try:
        op.drop_index('ix_field_corrections_org_id', table_name='field_corrections')
        op.drop_column('field_corrections', 'org_id')
    except Exception:
        pass
    
    # Restore old permit indexes
    op.create_index('idx_permit_created', 'permits', ['created_at'])
    op.create_index('idx_permit_status_date', 'permits', ['status_date'])
    op.create_index('idx_permit_district', 'permits', ['district'])
    op.create_index('idx_permit_county', 'permits', ['county'])
    op.create_index('idx_permit_operator_name', 'permits', ['operator_name'])
    op.create_index('idx_permit_api_no', 'permits', ['api_no'])
    op.create_index('idx_permit_status_no', 'permits', ['status_no'])
    
    # Drop new tenant-aware indexes
    op.drop_index('idx_permit_org_updated', table_name='permits')
    op.drop_index('idx_permit_org_created', table_name='permits')
    op.drop_index('idx_permit_org_status_date', table_name='permits')
    op.drop_index('idx_permit_org_district', table_name='permits')
    op.drop_index('idx_permit_org_county', table_name='permits')
    op.drop_index('idx_permit_org_operator', table_name='permits')
    op.drop_index('idx_permit_org_api_no', table_name='permits')
    op.drop_index('idx_permit_org_status_no', table_name='permits')
    
    # Remove tenant isolation and versioning from permits
    op.drop_column('permits', 'version')
    op.drop_column('permits', 'org_id')
