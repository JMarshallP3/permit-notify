"""Add user authentication tables

Revision ID: 018_add_auth_tables
Revises: 017_scout_v22_updates
Create Date: 2025-01-27 10:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '018_add_auth_tables'
down_revision = '017_scout_v22_updates'
branch_labels = None
depends_on = None


def upgrade():
    # Create users table
    op.create_table('users',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('email', sa.String(length=255), nullable=False),
        sa.Column('username', sa.String(length=100), nullable=True),
        sa.Column('password_hash', sa.Text(), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_users_email', 'users', ['email'], unique=True)
    op.create_index('idx_users_username', 'users', ['username'], unique=True)
    op.create_index('idx_users_active', 'users', ['is_active'])

    # Create orgs table (extend existing org concept)
    op.create_table('orgs',
        sa.Column('id', sa.String(length=50), nullable=False),
        sa.Column('name', sa.String(length=200), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    # Create org_memberships table
    op.create_table('org_memberships',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('org_id', sa.String(length=50), nullable=False),
        sa.Column('role', sa.String(length=20), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['org_id'], ['orgs.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.CheckConstraint("role IN ('owner', 'admin', 'member')", name='ck_org_memberships_role')
    )
    op.create_index('idx_org_memberships_user_org', 'org_memberships', ['user_id', 'org_id'], unique=True)
    op.create_index('idx_org_memberships_org_role', 'org_memberships', ['org_id', 'role'])
    op.create_index('idx_org_memberships_user', 'org_memberships', ['user_id'])

    # Create sessions table
    op.create_table('sessions',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('refresh_token_hash', sa.Text(), nullable=False),
        sa.Column('user_agent', sa.Text(), nullable=True),
        sa.Column('ip_address', postgresql.INET(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('revoked_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_sessions_user_active', 'sessions', ['user_id', 'revoked_at'])
    op.create_index('idx_sessions_expires', 'sessions', ['expires_at'])
    op.create_index('idx_sessions_refresh_hash', 'sessions', ['refresh_token_hash'])

    # Create password_resets table
    op.create_table('password_resets',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('user_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('token_hash', sa.Text(), nullable=False),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('used_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_password_resets_user', 'password_resets', ['user_id'])
    op.create_index('idx_password_resets_token_hash', 'password_resets', ['token_hash'])
    op.create_index('idx_password_resets_expires', 'password_resets', ['expires_at'])

    # Insert default org if it doesn't exist
    op.execute("""
        INSERT INTO orgs (id, name, created_at, updated_at)
        VALUES ('default_org', 'Default Organization', NOW(), NOW())
        ON CONFLICT (id) DO NOTHING
    """)


def downgrade():
    # Drop tables in reverse order
    op.drop_table('password_resets')
    op.drop_table('sessions')
    op.drop_table('org_memberships')
    op.drop_table('orgs')
    op.drop_table('users')
