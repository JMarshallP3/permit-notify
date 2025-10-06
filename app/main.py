from fastapi import FastAPI, Query, HTTPException, Request, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any, Set
import asyncio
import sys
import os
import logging
import requests
import time
from datetime import datetime, timezone
from app.scout_api import router as scout_router
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from routes import api_router
from routes.auth import router as auth_router
from services.scraper.scraper import Scraper
from services.scraper.rrc_w1 import RRCW1Client, EngineRedirectToLogin
from services.enrichment.worker import EnrichmentWorker, run_once
from services.enrichment.detail_parser import parse_detail_page
from services.enrichment.pdf_parse import extract_text_from_pdf, parse_reservoir_well_count
# Removed conflicting field_learning import - using FieldCorrection model directly
from db.models import Permit, Event
from db.field_corrections import FieldCorrection
from db.session import Base, engine, get_session
from db.repo import upsert_permits, get_recent_permits, get_reservoir_trends
from sqlalchemy import select, func
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

app = FastAPI(
    title="PermitTracker",
    description="Professional permit monitoring dashboard and API - FIXED SYNTAX ERROR",
    version="1.0.1"
)

# Add CORS middleware for real-time sync
FRONTEND_ORIGINS = os.getenv("FRONTEND_ORIGINS", "http://localhost:3000,http://localhost:8000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=FRONTEND_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Cookie", "Authorization", "Content-Type", "X-Org-ID"],
)

# ---------------------- Pydantic Models for Real-time Sync ----------------------
class PermitOut(BaseModel):
    id: int
    org_id: str
    status_no: Optional[str] = None
    operator_name: Optional[str] = None
    lease_name: Optional[str] = None
    field_name: Optional[str] = None
    version: int
    updated_at: datetime

    @classmethod
    def from_orm_row(cls, row: Permit):
        return cls(
            id=row.id,
            org_id=row.org_id,
            status_no=row.status_no,
            operator_name=row.operator_name,
            lease_name=row.lease_name,
            field_name=row.field_name,
            version=row.version,
            updated_at=row.updated_at,
        )

class PermitDeltaResponse(BaseModel):
    last_event_id: int
    permits: List[PermitOut]

# ---------------------- Tenant Authentication (Updated) ----------------------
def get_current_org_id(request: Request) -> str:
    """
    Extract org_id from request using new auth system.
    Falls back to query param or header for backward compatibility.
    """
    # Try to get from authenticated user context first
    try:
        from services.auth_middleware import get_auth_context
        # This would require async context, so we'll use a simpler approach
        # For now, keep the existing logic but add auth support
        pass
    except:
        pass
    
    # Fallback to existing logic
    org_id = request.query_params.get('org_id') or request.headers.get('X-Org-ID')
    return org_id or 'default_org'

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# Create a single Scraper instance for reuse
scraper_instance = Scraper()

# Create a single RRCW1Client instance for reuse
rrc_w1_client = RRCW1Client()

# Create a single EnrichmentWorker instance for reuse
enrichment_worker = EnrichmentWorker()

# Include the API routes
app.include_router(api_router, prefix="/api/v1")
app.include_router(scout_router)
app.include_router(auth_router)

# Dashboard routes
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Serve the modern dashboard interface."""
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Serve the login page."""
    return templates.TemplateResponse("login.html", {"request": request})

@app.get("/sessions", response_class=HTMLResponse)
async def sessions_page(request: Request):
    """Serve the session management page."""
    return templates.TemplateResponse("sessions.html", {"request": request})

# ---------------------- WebSocket Manager for Real-time Broadcasting ----------------------
class WSManager:
    def __init__(self):
        self.active: Dict[str, Set[WebSocket]] = {}  # org_id -> set of websockets
        self.user_sessions: Dict[str, Set[WebSocket]] = {}  # user_id -> set of websockets
        self._lock = asyncio.Lock()
        self.max_connections_per_org = 10  # Limit connections per org

    async def connect(self, ws: WebSocket, org_id: str, user_id: str = None):
        await ws.accept()
        async with self._lock:
            if org_id not in self.active:
                self.active[org_id] = set()
            
            # Track user sessions
            if user_id:
                if user_id not in self.user_sessions:
                    self.user_sessions[user_id] = set()
                self.user_sessions[user_id].add(ws)
            
            # Limit connections per org to prevent memory buildup
            if len(self.active[org_id]) >= self.max_connections_per_org:
                # Close oldest connection
                oldest_ws = next(iter(self.active[org_id]), None)
                if oldest_ws:
                    try:
                        await oldest_ws.close()
                    except:
                        pass
                    self.active[org_id].discard(oldest_ws)
            
            self.active[org_id].add(ws)

    async def disconnect(self, ws: WebSocket, org_id: str):
        async with self._lock:
            if org_id in self.active:
                self.active[org_id].discard(ws)
                if not self.active[org_id]:
                    del self.active[org_id]
            
            # Clean up user sessions
            for user_id, user_ws_set in list(self.user_sessions.items()):
                user_ws_set.discard(ws)
                if not user_ws_set:
                    del self.user_sessions[user_id]

    async def broadcast_to_org(self, org_id: str, message: Dict[str, Any]):
        """Broadcast message only to websockets for the specified org."""
        if org_id not in self.active:
            return
            
        dead = []
        connections_to_broadcast = list(self.active.get(org_id, set()))
        
        for ws in connections_to_broadcast:
            try:
                await asyncio.wait_for(ws.send_json(message), timeout=1.0)  # 1 second timeout
            except Exception:
                dead.append(ws)
        
        # Clean up dead connections
        if dead:
            async with self._lock:
                for ws in dead:
                    self.active.get(org_id, set()).discard(ws)
                if org_id in self.active and not self.active[org_id]:
                    del self.active[org_id]
    
    async def cleanup_dead_connections(self):
        """Periodic cleanup of dead connections"""
        async with self._lock:
            for org_id in list(self.active.keys()):
                dead = []
                for ws in list(self.active[org_id]):
                    try:
                        # Send ping to check if connection is alive
                        await asyncio.wait_for(ws.ping(), timeout=1.0)
                    except Exception:
                        dead.append(ws)
                
                for ws in dead:
                    self.active[org_id].discard(ws)
                
                if not self.active[org_id]:
                    del self.active[org_id]

ws_manager = WSManager()

@app.websocket("/ws/events")
async def ws_events(websocket: WebSocket, org_id: str = Query(default='default_org')):
    """Tenant-scoped WebSocket endpoint for real-time updates with authentication."""
    try:
        # Authenticate WebSocket connection
        from services.auth import auth_service
        
        # Get access token from query params or cookies
        access_token = None
        query_params = dict(websocket.query_params)
        
        # Try to get token from query params first
        if 'access_token' in query_params:
            access_token = query_params['access_token']
        else:
            # Try to get from cookies (if available in WebSocket)
            cookie_header = websocket.headers.get('cookie', '')
            if cookie_header:
                cookies = {}
                for cookie in cookie_header.split(';'):
                    if '=' in cookie:
                        key, value = cookie.strip().split('=', 1)
                        cookies[key] = value
                access_token = cookies.get('access_token')
        
        if not access_token:
            await websocket.close(code=4001, reason="Authentication required")
            return
        
        # Verify access token
        try:
            payload = auth_service.verify_token(access_token)
            user_id = payload.get("sub")
            if not user_id:
                await websocket.close(code=4001, reason="Invalid token")
                return
        except Exception:
            await websocket.close(code=4001, reason="Invalid token")
            return
        
        # Verify user has access to the requested org
        from db.session import get_session
        from db.auth_models import OrgMembership
        
        with get_session() as session:
            membership = session.query(OrgMembership).filter(
                OrgMembership.user_id == user_id,
                OrgMembership.org_id == org_id
            ).first()
            
            if not membership:
                await websocket.close(code=4003, reason=f"Access denied to organization {org_id}")
                return
        
        # Connect to WebSocket manager with user context
        await ws_manager.connect(websocket, org_id, user_id)
        
        # Keep connection alive
    try:
        while True:
            # Keep connection alive; client need not send anything
            await websocket.receive_text()
    except WebSocketDisconnect:
            pass
        finally:
        await ws_manager.disconnect(websocket, org_id)
            
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket, org_id)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        try:
            await websocket.close(code=4000, reason="Authentication failed")
        except:
            pass

# ---------------------- Background Event Poller ----------------------
POLL_INTERVAL = float(os.getenv("EVENT_POLL_INTERVAL_SECONDS", "5.0"))  # Reduced from 1.0 to 5.0 seconds

async def _poll_and_broadcast_events():
    """Poll the events table and broadcast new permit deltas to WS clients per org."""
    # Initialize last seen id to current max
    last_seen = 0
    try:
        with get_session() as session:
            last_seen = session.execute(select(func.coalesce(func.max(Event.id), 0))).scalar_one()
    except Exception as e:
        logger.error(f"[event poller] initialization error: {e}")
        return

    while True:
        try:
            await asyncio.sleep(POLL_INTERVAL)
            
            # Skip if no active websocket connections
            if not ws_manager.active:
                continue
                
            with get_session() as session:
                new_events = session.execute(
                    select(Event).where(Event.id > last_seen).order_by(Event.id.asc()).limit(100)  # Limit to prevent memory issues
                ).scalars().all()

                if not new_events:
                    continue

                last_seen = new_events[-1].id

                # Group events by org_id (with memory optimization)
                events_by_org = {}
                for event in new_events:
                    if event.entity == "permit":
                        if event.org_id not in events_by_org:
                            events_by_org[event.org_id] = set()  # Use set to prevent duplicates
                        events_by_org[event.org_id].add(event.entity_id)

                # Broadcast to each org separately (only if they have active connections)
                for org_id, permit_ids in events_by_org.items():
                    if not permit_ids or org_id not in ws_manager.active:
                        continue

                    # Convert set to list for database query
                    ids_list = list(permit_ids)[:50]  # Limit to 50 permits per batch
                    
                    rows = session.execute(
                        select(Permit)
                        .where(Permit.id.in_(ids_list))
                        .where(Permit.org_id == org_id)  # Ensure tenant isolation
                    ).scalars().all()

                    if rows:
                        payload = {
                            "type": "batch.permit.delta",
                            "last_event_id": last_seen,
                            "permits": [PermitOut.from_orm_row(p).dict() for p in rows],
                        }
                        await ws_manager.broadcast_to_org(org_id, payload)
                        
        except Exception as e:
            # Don't crash the loop; log and continue
            logger.error(f"[event poller] error: {e}")
            await asyncio.sleep(5)  # Wait 5 seconds before retrying on error

async def _cleanup_websockets_periodically():
    """Periodic cleanup of dead WebSocket connections"""
    while True:
        try:
            await asyncio.sleep(300)  # Clean up every 5 minutes
            await ws_manager.cleanup_dead_connections()
            logger.info("🧹 WebSocket cleanup completed")
        except Exception as e:
            logger.error(f"[websocket cleanup] error: {e}")
            await asyncio.sleep(60)  # Wait 1 minute before retrying on error

@app.on_event("startup")
async def startup_event():
    """Initialize database tables and start background cron on startup."""
    # Start background cron for permit scraping (only if enabled)
    SCRAPER_ENABLED = os.getenv("SCRAPER_ENABLED", "false").lower() == "true"
    if SCRAPER_ENABLED:
    try:
        import sys
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from background_cron import background_cron
        background_cron.start()
        logger.info("🚀 Background permit scraper started (every 10 minutes)")
    except Exception as e:
        logger.error(f"❌ Failed to start background cron: {e}")
    else:
        logger.info("⏸️ Background scraper disabled (SCRAPER_ENABLED=false)")
    
    # Start background event poller for real-time WebSocket broadcasting
    try:
        asyncio.create_task(_poll_and_broadcast_events())
        logger.info("🔄 Real-time event broadcaster started")
    except Exception as e:
        logger.error(f"❌ Failed to start event broadcaster: {e}")
    
    # Start WebSocket cleanup task
    try:
        asyncio.create_task(_cleanup_websockets_periodically())
        logger.info("🧹 WebSocket cleanup task started")
    except Exception as e:
        logger.error(f"❌ Failed to start WebSocket cleanup: {e}")
    
    # Initialize database tables on startup.
    try:
        logger.info("Initializing database operations")
        # Base.metadata.create_all(bind=engine)  # Uncomment if needed
        
        # Run Alembic migrations on startup
        try:
            from alembic.config import Config
            from alembic import command
            
            if os.getenv('DATABASE_URL'):
                logger.info("Running database migrations...")
                database_url = os.getenv('DATABASE_URL')
                logger.info(f"Using DATABASE_URL: {database_url[:20]}...")
                
                # Create Alembic config
                alembic_cfg = Config("alembic.ini")
                
                # Override the database URL
                alembic_cfg.set_main_option("sqlalchemy.url", database_url)
                
                # Run the migration
                logger.info("Starting migration upgrade...")
                command.upgrade(alembic_cfg, "head")
                logger.info("✅ Database migrations completed successfully")
            else:
                logger.info("Skipping migrations - no DATABASE_URL set")
        except Exception as migration_error:
            logger.error(f"❌ Migration failed: {migration_error}")
            import traceback
            logger.error(f"Migration traceback: {traceback.format_exc()}")
            # Continue startup even if migrations fail
        
        logger.info("✅ Database initialization completed")
    except Exception as e:
        logger.error(f"❌ Failed to initialize database: {e}")
        # Don't raise - just log the error
        pass

@app.get("/api/status")
async def api_status():
    return {"message": "Permit Notify API running"}

@app.get("/api/debug/tables")
async def debug_tables():
    """Debug endpoint to check if auth tables exist."""
    try:
        from db.session import get_session
        from db.auth_models import User, Org
        
        with get_session() as session:
            # Try to query tables to see if they exist
            try:
                user_count = session.query(User).count()
                org_count = session.query(Org).count()
                return {
                    "status": "success",
                    "tables_exist": True,
                    "user_count": user_count,
                    "org_count": org_count
                }
            except Exception as e:
                return {
                    "status": "error",
                    "tables_exist": False,
                    "error": str(e)
                }
    except Exception as e:
        return {
            "status": "error",
            "database_connection": False,
            "error": str(e)
        }

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/api/debug/schema")
async def debug_schema():
    """Debug endpoint to check current database schema."""
    try:
        from sqlalchemy import create_engine, text
        import traceback
        
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            return {"status": "error", "error": "DATABASE_URL not set"}
        
        engine = create_engine(database_url)
        
        with engine.connect() as connection:
            # Check if auth tables exist
            tables_query = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('users', 'orgs', 'org_memberships', 'sessions', 'password_resets')
                ORDER BY table_name;
            """)
            
            tables_result = connection.execute(tables_query).fetchall()
            existing_tables = [row[0] for row in tables_result]
            
            # Check current migration version
            try:
                version_query = text("SELECT version_num FROM alembic_version ORDER BY version_num DESC LIMIT 1;")
                version_result = connection.execute(version_query).fetchone()
                current_version = version_result[0] if version_result else None
            except:
                current_version = "No alembic_version table"
            
            # Check if permits table has org_id column (this might be causing the duplicate error)
            try:
                permits_columns_query = text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    AND table_name = 'permits' 
                    AND column_name LIKE '%org%'
                    ORDER BY column_name;
                """)
                permits_columns = connection.execute(permits_columns_query).fetchall()
                permits_org_columns = [row[0] for row in permits_columns]
            except:
                permits_org_columns = ["Error checking permits table"]
            
            return {
                "status": "success",
                "current_migration_version": current_version,
                "existing_auth_tables": existing_tables,
                "permits_org_columns": permits_org_columns,
                "diagnosis": {
                    "all_auth_tables_exist": len(existing_tables) == 5,
                    "some_auth_tables_exist": len(existing_tables) > 0,
                    "migration_version_matches": current_version == "018_add_auth_tables"
                }
            }
            
    except Exception as e:
        return {
            "status": "error", 
            "error": str(e),
            "traceback": traceback.format_exc()
        }

@app.post("/api/debug/migrate-step")
async def debug_migrate_step():
    """Run migrations one step at a time to avoid conflicts."""
    try:
        from alembic.config import Config
        from alembic import command
        from alembic.runtime.migration import MigrationContext
        from sqlalchemy import create_engine
        import traceback
        
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            return {"status": "error", "error": "DATABASE_URL not set"}
        
        # Create Alembic config
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)
        
        engine = create_engine(database_url)
        
        # Get current revision
        with engine.connect() as connection:
            context = MigrationContext.configure(connection)
            current_rev = context.get_current_revision()
        
        # Run migration to next version (not all the way to head)
        try:
            command.upgrade(alembic_cfg, "+1")  # Upgrade by one step
            
            # Get new revision
            with engine.connect() as connection:
                context = MigrationContext.configure(connection)
                new_rev = context.get_current_revision()
            
            return {
                "status": "success",
                "message": "Migration step completed",
                "previous_revision": current_rev,
                "new_revision": new_rev,
                "next_action": "Run again to continue to next migration" if new_rev != "018_add_auth_tables" else "All migrations complete!"
            }
            
        except Exception as migration_error:
            return {
                "status": "error",
                "error": str(migration_error),
                "traceback": traceback.format_exc(),
                "current_revision": current_rev
            }
            
    except Exception as e:
        return {
            "status": "error", 
            "error": str(e),
            "traceback": traceback.format_exc()
        }

@app.post("/api/debug/migrate-skip")
async def debug_migrate_skip():
    """Skip problematic migration 013 since org_id column already exists."""
    try:
        from alembic.config import Config
        from alembic import command
        from alembic.runtime.migration import MigrationContext
        from sqlalchemy import create_engine, text
        import traceback
        
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            return {"status": "error", "error": "DATABASE_URL not set"}
        
        engine = create_engine(database_url)
        
        # Check if org_id column exists in permits table
        with engine.connect() as connection:
            check_column = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'permits' 
                AND column_name = 'org_id'
            """)
            column_exists = connection.execute(check_column).fetchone() is not None
            
            # Check current alembic_version table structure
            version_info = text("""
                SELECT character_maximum_length 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'alembic_version' 
                AND column_name = 'version_num'
            """)
            version_length = connection.execute(version_info).fetchone()
            max_length = version_length[0] if version_length else "unknown"
            
            if column_exists:
                # Use just the migration ID, not the full filename
                migration_id = "013_add_tenant_isolation_and_events"
                
                # Truncate if necessary (most alembic_version columns are 32 chars)
                if max_length and isinstance(max_length, int) and len(migration_id) > max_length:
                    migration_id = migration_id[:max_length]
                
                # Manually update alembic_version to skip migration 013
                update_version = text("UPDATE alembic_version SET version_num = :version_id")
                connection.execute(update_version, {"version_id": migration_id})
                connection.commit()
                
                return {
                    "status": "success",
                    "message": f"Skipped migration 013 - org_id column already exists",
                    "migration_id": migration_id,
                    "column_max_length": max_length,
                    "action": "Migration version updated, can now continue with remaining migrations"
                }
            else:
                return {
                    "status": "error",
                    "message": "org_id column doesn't exist - migration 013 should run normally"
                }
            
    except Exception as e:
        return {
            "status": "error", 
            "error": str(e),
            "traceback": traceback.format_exc()
        }

@app.post("/api/debug/fix-alembic")
async def debug_fix_alembic():
    """Fix alembic version table with multiple heads issue."""
    try:
        from sqlalchemy import create_engine, text
        import traceback
        
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            return {"status": "error", "error": "DATABASE_URL not set"}
        
        engine = create_engine(database_url)
        
        with engine.connect() as connection:
            # Check current alembic_version entries
            check_versions = text("SELECT version_num FROM alembic_version ORDER BY version_num")
            current_versions = connection.execute(check_versions).fetchall()
            version_list = [row[0] for row in current_versions]
            
            # Clear all entries and set to the correct single version
            connection.execute(text("DELETE FROM alembic_version"))
            
            # Insert the correct current version (013 since we skipped it)
            connection.execute(
                text("INSERT INTO alembic_version (version_num) VALUES (:version)"),
                {"version": "013_add_tenant_isolation_and_eve"}
            )
            
            connection.commit()
            
            return {
                "status": "success",
                "message": "Fixed alembic version table",
                "previous_versions": version_list,
                "new_version": "013_add_tenant_isolation_and_eve",
                "action": "Can now continue with step-by-step migrations"
            }
            
    except Exception as e:
        return {
            "status": "error", 
            "error": str(e),
            "traceback": traceback.format_exc()
        }

@app.post("/api/debug/skip-to-018")
async def debug_skip_to_018():
    """Skip all problematic migrations and jump directly to 018 (auth tables)."""
    try:
        from sqlalchemy import create_engine, text
        import traceback
        
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            return {"status": "error", "error": "DATABASE_URL not set"}
        
        engine = create_engine(database_url)
        
        with engine.connect() as connection:
            # Check what columns already exist in permits table
            permits_columns = text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'permits' 
                ORDER BY column_name
            """)
            existing_columns = [row[0] for row in connection.execute(permits_columns).fetchall()]
            
            # Check what tables already exist
            existing_tables = text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('users', 'orgs', 'org_memberships', 'sessions', 'password_resets', 'scout_w1_permits', 'field_corrections', 'events')
                ORDER BY table_name
            """)
            tables_list = [row[0] for row in connection.execute(existing_tables).fetchall()]
            
            # Determine what migrations to skip based on existing schema
            migrations_to_skip = []
            
            if 'org_id' in existing_columns:
                migrations_to_skip.append('013_add_tenant_isolation_and_events')
            if 'is_injection_well' in existing_columns:
                migrations_to_skip.append('014_add_is_injection_well_column')
            if 'field_corrections' in tables_list and 'org_id' in existing_columns:
                migrations_to_skip.append('015_add_org_id_to_field_corrections')
            if 'scout_w1_permits' in tables_list:
                migrations_to_skip.append('016_add_scout_tables')
                migrations_to_skip.append('017_scout_v22_updates')
            
            # Set migration version to 017 (just before auth tables)
            connection.execute(text("DELETE FROM alembic_version"))
            connection.execute(
                text("INSERT INTO alembic_version (version_num) VALUES (:version)"),
                {"version": "017_scout_v22_updates"[:32]}  # Truncate to 32 chars
            )
            connection.commit()
            
            return {
                "status": "success",
                "message": "Skipped problematic migrations based on existing schema",
                "existing_columns": existing_columns,
                "existing_tables": tables_list,
                "skipped_migrations": migrations_to_skip,
                "new_version": "017_scout_v22_updates",
                "action": "Now run migrate-step once to apply migration 018 (auth tables)"
            }
            
    except Exception as e:
        return {
            "status": "error", 
            "error": str(e),
            "traceback": traceback.format_exc()
        }

@app.post("/api/debug/test-registration")
async def debug_test_registration():
    """Test registration flow step by step to identify the 500 error."""
    try:
        from services.auth import auth_service
        from db.session import get_session
        from db.auth_models import User, Org, OrgMembership
        import traceback
        
        test_email = "test@example.com"
        test_password = "testpassword123"
        
        # Step 1: Test password hashing
        try:
            hashed_password = auth_service.hash_password(test_password)
            step1_result = "✅ Password hashing works"
        except Exception as e:
            return {"status": "error", "step": "password_hashing", "error": str(e)}
        
        # Step 2: Test database connection and user creation
        try:
            with get_session() as session:
                # Check if test user already exists
                existing_user = session.query(User).filter(User.email == test_email).first()
                if existing_user:
                    session.delete(existing_user)
                    session.commit()
                
                # Try to create user
                user = User(
                    email=test_email,
                    password_hash=hashed_password,
                    is_active=True
                )
                session.add(user)
                session.commit()
                session.refresh(user)
                step2_result = f"✅ User creation works - ID: {user.id}"
                
                # Clean up test user
                session.delete(user)
                session.commit()
                
        except Exception as e:
            return {"status": "error", "step": "user_creation", "error": str(e), "traceback": traceback.format_exc()}
        
        # Step 3: Test org lookup/creation
        try:
            with get_session() as session:
                default_org = session.query(Org).filter(Org.id == "default_org").first()
                if default_org:
                    step3_result = f"✅ Default org exists - Name: {default_org.name}"
                else:
                    step3_result = "❌ Default org missing"
        except Exception as e:
            return {"status": "error", "step": "org_lookup", "error": str(e)}
        
        # Step 4: Test JWT token creation
        try:
            test_data = {"sub": "test-user-id", "org_id": "default_org", "role": "owner"}
            access_token = auth_service.create_access_token(data=test_data)
            step4_result = f"✅ JWT creation works - Length: {len(access_token)}"
        except Exception as e:
            return {"status": "error", "step": "jwt_creation", "error": str(e)}
        
        return {
            "status": "success",
            "message": "All registration components work individually",
            "steps": {
                "1_password_hashing": step1_result,
                "2_user_creation": step2_result,
                "3_org_lookup": step3_result,
                "4_jwt_creation": step4_result
            },
            "conclusion": "The 500 error is likely in the registration endpoint logic, not the core components"
        }
        
    except Exception as e:
        return {
            "status": "error", 
            "error": str(e),
            "traceback": traceback.format_exc()
        }

@app.post("/api/debug/migrate")
async def debug_migrate():
    """Debug endpoint to manually run migrations and see detailed errors."""
    try:
        from alembic.config import Config
        from alembic import command
        import traceback
        
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            return {"status": "error", "error": "DATABASE_URL not set"}
        
        # Create Alembic config
        alembic_cfg = Config("alembic.ini")
        alembic_cfg.set_main_option("sqlalchemy.url", database_url)
        
        # Get current revision
        try:
            from alembic.runtime.migration import MigrationContext
            from sqlalchemy import create_engine
            
            engine = create_engine(database_url)
            with engine.connect() as connection:
                context = MigrationContext.configure(connection)
                current_rev = context.get_current_revision()
                
            # Run the migration
            command.upgrade(alembic_cfg, "head")
            
            # Get new revision
            with engine.connect() as connection:
                context = MigrationContext.configure(connection)
                new_rev = context.get_current_revision()
            
            return {
                "status": "success",
                "message": "Migration completed",
                "previous_revision": current_rev,
                "new_revision": new_rev
            }
            
        except Exception as migration_error:
            return {
                "status": "error",
                "error": str(migration_error),
                "traceback": traceback.format_exc()
            }
            
    except Exception as e:
        return {
            "status": "error", 
            "error": str(e),
            "traceback": traceback.format_exc()
        }

@app.get("/scrape")
async def scrape():
    """Scrape permit data and store in database."""
    try:
        # Run scraper
        result = scraper_instance.run()
        
        # Store results in database if we have items
        if result.get("items"):
            upsert_result = upsert_permits(result["items"])
            result["database"] = upsert_result
            logger.info(f"Stored {upsert_result['inserted']} new permits, updated {upsert_result['updated']} permits")
        
        return result
    except Exception as e:
        logger.error(f"Scraping error: {e}")
        return {"error": str(e), "items": [], "warning": "Scraping failed"}

# ---------------------- Tenant-Scoped Delta Sync API ----------------------
@app.get("/permits", response_model=PermitDeltaResponse)
def get_permits_since(
    request: Request,
    since_event_id: Optional[int] = Query(default=None, alias="since_event_id"),
    org_id: str = Depends(get_current_org_id)
):
    """
    Tenant-scoped delta sync endpoint.
    Returns permits that have changed since the given event ID for the current org.
    """
    with get_session() as session:  # type: Session
        # Current max event id for this org (so client can advance even if there are no new rows)
        last_event_id = session.execute(
            select(func.coalesce(func.max(Event.id), 0))
            .where(Event.org_id == org_id)
        ).scalar_one()

        if since_event_id is None:
            # Cold start: return recent permits for this org
            permits = session.execute(
                select(Permit)
                .where(Permit.org_id == org_id)
                .order_by(Permit.updated_at.desc())
                .limit(500)
            ).scalars().all()
            return {
                "last_event_id": last_event_id,
                "permits": [PermitOut.from_orm_row(p) for p in permits],
            }

        # Hot path: collect changed permit IDs since the given event id for this org
        changed_ids = session.execute(
            select(Event.entity_id)
            .where(Event.id > since_event_id)
            .where(Event.entity == "permit")
            .where(Event.org_id == org_id)
            .order_by(Event.id.asc())
        ).scalars().all()

        if not changed_ids:
            return {"last_event_id": last_event_id, "permits": []}

        ids_unique = list(dict.fromkeys(changed_ids))  # de-dupe while preserving order
        rows = session.execute(
            select(Permit)
            .where(Permit.id.in_(ids_unique))
            .where(Permit.org_id == org_id)  # Double-check tenant isolation
        ).scalars().all()

        # Keep order similar to ids_unique
        row_map = {r.id: r for r in rows}
        ordered = [row_map[i] for i in ids_unique if i in row_map]

        return {
            "last_event_id": last_event_id,
            "permits": [PermitOut.from_orm_row(p) for p in ordered],
        }

@app.get("/api/v1/permits")
async def get_permits(
    limit: int = Query(25, ge=1, le=1000),
    days_back: int = Query(30, ge=1, le=365, description="Number of days back to look for permits")
):
    """Get recent permits from database (last N days)."""
    try:
        logger.info(f"Fetching {limit} recent permits from last {days_back} days")
        permits = get_recent_permits(limit, days_back)
        return {
            "permits": permits,
            "count": len(permits),
            "limit": limit,
            "days_back": days_back
        }
    except Exception as e:
        logger.error(f"Database query error: {e}")
        return {"error": str(e), "permits": []}

@app.get("/api/v1/permits/trends")
async def get_permits_for_trends(
    limit: int = Query(1000, ge=1, le=5000, description="Maximum permits for trend analysis")
):
    """Get permits for trend analysis (no date filter, includes historical data)."""
    try:
        logger.info(f"Fetching {limit} permits for trend analysis")
        
        with get_session() as session:
            permits = session.query(Permit).order_by(
                Permit.status_date.desc(),
                Permit.created_at.desc()
            ).limit(limit).all()
            
            permit_data = [permit.to_dict() for permit in permits]
            
        return {
            "permits": permit_data,
            "count": len(permit_data),
            "limit": limit,
            "note": "Historical data for trend analysis"
        }
    except Exception as e:
        logger.error(f"Trends database query error: {e}")
        return {"error": str(e), "permits": []}

@app.get("/w1/search")
async def w1_search(
    begin: str = Query(..., description="Start date in MM/DD/YYYY format"),
    end: str = Query(..., description="End date in MM/DD/YYYY format"),
    pages: int = Query(None, description="Maximum number of pages to fetch (None for all)")
):
    """
    Search RRC W-1 drilling permits by date range.
    
    Args:
        begin: Start date in MM/DD/YYYY format (required)
        end: End date in MM/DD/YYYY format (required)
        pages: Maximum number of pages to fetch (optional, None for all)
    
    Returns:
        Dictionary with query results and metadata
    """
    try:
        logger.info(f"W-1 search request: begin={begin}, end={end}, pages={pages}")
        
        # Validate date format (basic validation)
        import re
        date_pattern = r'^\d{2}/\d{2}/\d{4}$'
        if not re.match(date_pattern, begin) or not re.match(date_pattern, end):
            raise HTTPException(
                status_code=400,
                detail="Date format must be MM/DD/YYYY"
            )
        
        # Fetch results using RRCW1Client
        result = rrc_w1_client.fetch_all(begin, end, pages)
        
        # Store results in database if we have items
        if result.get("items"):
            try:
                logger.info(f"Storing {len(result['items'])} permits in database")
                upsert_result = upsert_permits(result["items"])
                result["database"] = upsert_result
                logger.info(f"✅ Stored {upsert_result['inserted']} new permits, updated {upsert_result['updated']} permits")
            except Exception as db_error:
                logger.error(f"❌ Database storage failed: {db_error}")
                result["database"] = {"inserted": 0, "updated": 0, "error": str(db_error)}
        else:
            result["database"] = {"inserted": 0, "updated": 0, "note": "No permits found"}
            logger.info("No permits found")
        
        logger.info(f"W-1 search completed: {result['pages']} pages, {result['count']} items")
        return result
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except EngineRedirectToLogin as e:
        logger.warning(f"RRC W-1 search redirected to login: {e}")
        raise HTTPException(
            status_code=502,
            detail="RRC W-1 search redirected to login page. Please try again later."
        )
    except Exception as e:
        logger.error(f"W-1 search error: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"RRC W-1 search failed: {str(e)}"
        )

@app.post("/enrich/run")
async def run_enrichment(n: int = Query(5, ge=1, le=50, description="Maximum number of permits to process")):
    """
    Run the enrichment worker for N permits.
    
    Args:
        n: Maximum number of permits to process (1-50)
        
    Returns:
        Dictionary with enrichment results
    """
    try:
        logger.info(f"Starting enrichment for {n} permits")
        
        # Run the enrichment worker
        results = run_once(limit=n)
        
        logger.info(f"Enrichment completed: {results['processed']} processed, "
                   f"{results['successful']} successful, {results['failed']} failed")
        
        return {
            "processed": results['processed'],
            "ok": results['successful'],
            "errors": results['failed']
        }
        
    except Exception as e:
        logger.error(f"Enrichment error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Enrichment failed: {str(e)}"
        )

@app.post("/enrich/auto")
async def run_auto_enrichment(
    batch_size: int = Query(10, ge=1, le=50, description="Number of permits to process per batch"),
    max_batches: int = Query(5, ge=1, le=20, description="Maximum number of batches to run")
):
    """
    Run automated enrichment worker to process all pending permits.
    
    Args:
        batch_size: Number of permits to process per batch (1-50)
        max_batches: Maximum number of batches to process (1-20)
        
    Returns:
        Dictionary with enrichment results
    """
    try:
        logger.info(f"Starting automated enrichment: {batch_size} permits per batch, max {max_batches} batches")
        
        total_processed = 0
        total_successful = 0
        total_failed = 0
        batches_run = 0
        
        # Run enrichment in batches until no more permits need processing
        while batches_run < max_batches:
            results = run_once(limit=batch_size)
            
            # If no permits were processed, we're done
            if results['processed'] == 0:
                logger.info("No more permits need enrichment")
                break
                
            total_processed += results['processed']
            total_successful += results['successful']
            total_failed += results['failed']
            batches_run += 1
            
            logger.info(f"Batch {batches_run}: {results['processed']} processed, "
                       f"{results['successful']} successful, {results['failed']} failed")
            
            # Small delay between batches to be respectful to the RRC servers
            import asyncio
            await asyncio.sleep(2)
        
        logger.info(f"Auto-enrichment completed: {total_processed} total processed, "
                   f"{total_successful} successful, {total_failed} failed across {batches_run} batches")
        
        return {
            "processed": total_processed,
            "successful": total_successful,
            "failed": total_failed,
            "batches_run": batches_run,
            "status": "completed" if batches_run < max_batches else "max_batches_reached"
        }
        
    except Exception as e:
        logger.error(f"Auto-enrichment error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Auto-enrichment failed: {str(e)}"
        )

@app.get("/scrape-and-enrich")
async def scrape_and_enrich():
    """
    Combined endpoint that scrapes today's permits AND enriches them.
    Perfect for cron jobs - does everything in one call.
    
    Returns:
        Dictionary with scraping and enrichment results
    """
    try:
        logger.info("🔄 Starting combined scrape-and-enrich process")
        
        # Step 1: Scrape today's permits
        today = datetime.now().strftime("%m/%d/%Y")
        logger.info(f"📅 Scraping permits for {today}")
        
        # Call internal scraping endpoint
        try:
            scrape_response = requests.get(
                f"http://localhost:8000/w1/search",
                params={"begin": today, "end": today, "pages": 5},
                timeout=120
            )
            
            if scrape_response.status_code == 200:
                scrape_data = scrape_response.json()
                permits_found = len(scrape_data.get("items", []))
                db_info = scrape_data.get("database", {})
                permits_inserted = db_info.get("inserted", 0)
                permits_updated = db_info.get("updated", 0)
                
                logger.info(f"✅ Scraping completed: {permits_found} found, {permits_inserted} new, {permits_updated} updated")
            else:
                logger.warning(f"⚠️ Scraping returned status {scrape_response.status_code}")
                permits_found = permits_inserted = permits_updated = 0
                
        except Exception as scrape_error:
            logger.error(f"❌ Scraping failed: {scrape_error}")
            permits_found = permits_inserted = permits_updated = 0
        
        # Step 2: Enrich permits (regardless of scraping results)
        logger.info("🔍 Starting enrichment process")
        
        enrichment_results = run_once(limit=20)  # Process up to 20 permits
        
        logger.info(f"✅ Enrichment completed: {enrichment_results['processed']} processed, "
                   f"{enrichment_results['successful']} successful, {enrichment_results['failed']} failed")
        
        # Step 3: Return combined results
        return {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "date_processed": today,
            "scraping": {
                "permits_found": permits_found,
                "permits_inserted": permits_inserted,
                "permits_updated": permits_updated
            },
            "enrichment": {
                "permits_processed": enrichment_results['processed'],
                "permits_successful": enrichment_results['successful'],
                "permits_failed": enrichment_results['failed']
            },
            "message": f"Scraped {permits_found} permits ({permits_inserted} new), enriched {enrichment_results['processed']} permits"
        }
        
    except Exception as e:
        logger.error(f"❌ Combined scrape-and-enrich error: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/enrich/trigger")
async def trigger_enrichment():
    """
    Simple trigger endpoint for automated enrichment.
    Can be called by external cron services or monitoring tools.
    
    Returns:
        Dictionary with enrichment results
    """
    try:
        logger.info("🔄 Triggered automated enrichment via GET endpoint")
        
        # Run auto-enrichment with sensible defaults
        results = run_once(limit=15)  # Process up to 15 permits
        
        if results['processed'] > 0:
            logger.info(f"✅ Triggered enrichment completed: {results['processed']} processed, "
                       f"{results['successful']} successful, {results['failed']} failed")
        else:
            logger.info("ℹ️  No permits needed enrichment")
        
        return {
            "success": True,
            "processed": results['processed'],
            "successful": results['successful'],
            "failed": results['failed'],
            "message": f"Processed {results['processed']} permits" if results['processed'] > 0 else "No permits needed enrichment",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Triggered enrichment error: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

# Flagging endpoints temporarily disabled due to import conflicts
# Will be re-enabled once database import issues are resolved

@app.get("/enrich/debug/{permit_id}")
async def debug_enrichment(permit_id: int):
    """
    Debug endpoint to test enrichment parsing for a specific permit.
    Fetches detail page and PDF (if available) without writing to database.
    
    Args:
        permit_id: ID of the permit to debug
        
    Returns:
        Dictionary with parsed data for debugging
    """
    try:
        logger.info(f"Debug enrichment for permit ID: {permit_id}")
        
        # Load permit from database
        from db.session import get_session
        with get_session() as session:
            permit = session.query(Permit).filter(Permit.id == permit_id).first()
            if not permit:
                raise HTTPException(
                    status_code=404,
                    detail=f"Permit with ID {permit_id} not found"
                )
            
            # Check if permit has detail URL
            if not permit.detail_url:
                raise HTTPException(
                    status_code=400,
                    detail=f"Permit {permit_id} has no detail URL"
                )
        
        # Use enrichment worker to fetch data (but don't update DB)
        worker = EnrichmentWorker()
        
        # Fetch detail page
        logger.info(f"Fetching detail page: {permit.detail_url}")
        detail_response = worker._make_request(permit.detail_url)
        
        if not detail_response:
            raise HTTPException(
                status_code=502,
                detail="Failed to fetch detail page"
            )
        
        # Parse detail page
        detail_data = parse_detail_page(detail_response.text, permit.detail_url)
        
        result = {
            "permit_id": permit.id,
            "status_no": permit.status_no,
            "detail_url": permit.detail_url,
            "parsed_data": detail_data,
            "pdf_data": None,
            "error": None
        }
        
        # If PDF URL found, fetch and parse it
        if detail_data.get('view_w1_pdf_url'):
            logger.info(f"Fetching PDF: {detail_data['view_w1_pdf_url']}")
            
            pdf_response = worker._make_request(
                detail_data['view_w1_pdf_url'],
                referer=permit.detail_url
            )
            
            if pdf_response:
                try:
                    # Extract text from PDF
                    pdf_text = extract_text_from_pdf(pdf_response.content)
                    
                    if pdf_text:
                        # Parse reservoir well count
                        well_count, confidence, snippet = parse_reservoir_well_count(pdf_text)
                        
                        result["pdf_data"] = {
                            "reservoir_well_count": well_count,
                            "confidence": confidence,
                            "text_snippet": snippet,
                            "pdf_url": detail_data['view_w1_pdf_url'],
                            "text_length": len(pdf_text)
                        }
                    else:
                        result["pdf_data"] = {
                            "error": "No text extracted from PDF",
                            "pdf_url": detail_data['view_w1_pdf_url']
                        }
                        
                except Exception as e:
                    result["pdf_data"] = {
                        "error": f"PDF processing error: {str(e)}",
                        "pdf_url": detail_data['view_w1_pdf_url']
                    }
            else:
                result["pdf_data"] = {
                    "error": "Failed to download PDF",
                    "pdf_url": detail_data['view_w1_pdf_url']
                }
        
        # Calculate confidence score for debugging
        confidence = 0.0
        if detail_data.get('horizontal_wellbore'): confidence += 0.3
        if detail_data.get('field_name'): confidence += 0.3
        if detail_data.get('acres') is not None: confidence += 0.2
        if result.get('pdf_data', {}).get('reservoir_well_count') is not None: confidence += 0.3
        confidence = min(confidence, 1.0)
        
        result["debug_info"] = {
            "confidence": confidence,
            "fields_found": {
                "horizontal_wellbore": bool(detail_data.get('horizontal_wellbore')),
                "field_name": bool(detail_data.get('field_name')),
                "acres": detail_data.get('acres') is not None,
                "section": bool(detail_data.get('section')),
                "block": bool(detail_data.get('block')),
                "survey": bool(detail_data.get('survey')),
                "abstract_no": bool(detail_data.get('abstract_no')),
                "reservoir_well_count": result.get('pdf_data', {}).get('reservoir_well_count') is not None
            }
        }
        
        logger.info(f"Debug enrichment completed for permit {permit_id}: confidence={confidence:.2f}")
        return result
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Debug enrichment error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Debug enrichment failed: {str(e)}"
        )

@app.get("/api/v1/reservoir-trends")
async def get_reservoir_trends_api(
    days: int = Query(default=90, description="Number of days to look back"),
    reservoirs: str = Query(default="", description="Comma-separated list of specific reservoirs"),
    view_type: str = Query(default="daily", description="View type: 'daily' or 'cumulative'"),
    mappings: str = Query(default="", description="JSON string of reservoir mappings")
):
    """Get historical reservoir permit trends for charting."""
    try:
        reservoir_list = [r.strip() for r in reservoirs.split(",") if r.strip()] if reservoirs else None
        
        # Parse reservoir mappings if provided
        reservoir_mappings = {}
        if mappings:
            import json
            try:
                reservoir_mappings = json.loads(mappings)
                logger.info(f"📊 API received {len(reservoir_mappings)} reservoir mappings")
                if reservoir_mappings:
                    logger.info(f"📊 Sample API mappings: {dict(list(reservoir_mappings.items())[:3])}")
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON in mappings parameter: {mappings[:100]}... Error: {e}")
        else:
            logger.info("📊 No mappings parameter provided to API")
        
        trends_data = get_reservoir_trends(
            days_back=days, 
            specific_reservoirs=reservoir_list, 
            view_type=view_type,
            reservoir_mappings=reservoir_mappings
        )
        
        return {
            "success": True,
            "data": trends_data,
            "days_back": days,
            "total_reservoirs": len(trends_data.get("reservoirs", {})),
            "view_type": view_type
        }
        
    except Exception as e:
        logger.error(f"Reservoir trends error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch reservoir trends")

@app.get("/api/v1/permits/count-by-field")
async def count_permits_by_field_name(
    field_name: str = Query(..., description="Field name to count"),
    exclude_status_no: str = Query(None, description="Status number to exclude from count")
):
    """
    Count permits with the same field name (excluding a specific permit).
    Used for bulk update confirmation dialogs.
    """
    try:
        # Log the incoming parameters for debugging
        logger.info(f"Count permits by field - field_name: '{field_name}', exclude_status_no: '{exclude_status_no}'")
        
        # Validate field_name is not empty
        if not field_name or not field_name.strip():
            raise HTTPException(status_code=422, detail="Field name cannot be empty")
        
        with get_session() as session:
            query = session.query(Permit).filter(Permit.field_name == field_name)
            
            if exclude_status_no:
                query = query.filter(Permit.status_no != exclude_status_no)
            
            count = query.count()
            
            logger.info(f"Count result: {count} permits found for field '{field_name}'")
            
            return {
                "count": count,
                "field_name": field_name,
                "excluded_status_no": exclude_status_no
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Count permits by field error: {e}")
        raise HTTPException(status_code=500, detail="Failed to count permits")

@app.post("/api/v1/permits/{status_no}/re-enrich")
async def re_enrich_single_permit(status_no: str, request: Request):
    """
    Re-enrich a single permit by re-parsing its detail page.
    This will update the permit's field_name and other enriched data.
    """
    try:
        # Get org_id for tenant isolation
        org_id = request.query_params.get('org_id') or request.headers.get('X-Org-ID') or 'default_org'
        
        logger.info(f"Re-enriching permit {status_no} (org: {org_id})")
        
        with get_session() as session:
            # Find the permit with tenant isolation
            permit = session.query(Permit).filter(
                Permit.status_no == status_no,
                Permit.org_id == org_id
            ).first()
            
            # If not found with org_id, try without for legacy data
            if not permit and org_id == 'default_org':
                permit = session.query(Permit).filter(
                    Permit.status_no == status_no
                ).first()
            
            if not permit:
                raise HTTPException(status_code=404, detail=f"Permit {status_no} not found")
            
            # Check if permit has a detail_url
            if not permit.detail_url:
                raise HTTPException(status_code=400, detail=f"Permit {status_no} has no detail URL for re-enrichment")
            
            # Import the enrichment function
            try:
                from services.enrichment.detail_parser import parse_detail_page
                
                # Re-parse the detail page
                logger.info(f"Re-parsing detail page for permit {status_no}: {permit.detail_url}")
                enriched_data = parse_detail_page(permit.detail_url)
                
                if enriched_data:
                    # Update the permit with new enriched data
                    old_field_name = permit.field_name
                    
                    if 'field_name' in enriched_data and enriched_data['field_name']:
                        permit.field_name = enriched_data['field_name']
                    
                    if 'horizontal_wellbore' in enriched_data:
                        permit.horizontal_wellbore = enriched_data['horizontal_wellbore']
                    
                    if 'acres' in enriched_data:
                        permit.acres = enriched_data['acres']
                    
                    if 'section' in enriched_data:
                        permit.section = enriched_data['section']
                    
                    if 'block' in enriched_data:
                        permit.block = enriched_data['block']
                    
                    if 'survey' in enriched_data:
                        permit.survey = enriched_data['survey']
                    
                    if 'abstract_no' in enriched_data:
                        permit.abstract_no = enriched_data['abstract_no']
                    
                    # Update enrichment metadata
                    permit.w1_parse_status = 'ok'
                    permit.w1_last_enriched_at = datetime.now(timezone.utc)
                    
                    session.commit()
                    
                    logger.info(f"✅ Successfully re-enriched permit {status_no}: '{old_field_name}' → '{permit.field_name}'")
                    
                    return {
                        "success": True,
                        "message": f"Permit {status_no} re-enriched successfully",
                        "status_no": permit.status_no,
                        "old_field_name": old_field_name,
                        "new_field_name": permit.field_name,
                        "enriched_fields": list(enriched_data.keys()) if enriched_data else []
                    }
                else:
                    # Mark as failed to parse
                    permit.w1_parse_status = 'parse_error'
                    permit.w1_last_enriched_at = datetime.now(timezone.utc)
                    session.commit()
                    
                    raise HTTPException(status_code=400, detail=f"Failed to parse detail page for permit {status_no}")
                    
            except ImportError as e:
                logger.error(f"Could not import enrichment parser: {e}")
                raise HTTPException(status_code=500, detail="Enrichment service not available")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Re-enrich permit error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to re-enrich permit: {str(e)}")

@app.post("/api/v1/permits/bulk-update-field")
async def bulk_update_field_names(request_data: dict):
    """
    Bulk update all permits that have the same wrong field name.
    Expected payload: {
        "wrong_field": "There are additional problems with this ( 09/25/2025 04:31:20 PM )",
        "correct_field": "PAN PETRO (CLEVELAND)"
    }
    """
    try:
        wrong_field = request_data.get("wrong_field")
        correct_field = request_data.get("correct_field")
        
        if not all([wrong_field, correct_field]):
            raise HTTPException(status_code=400, detail="wrong_field and correct_field are required")
        
        updated_count = 0
        
        with get_session() as session:
            # Find all permits with the wrong field name
            permits_to_update = session.query(Permit).filter(
                Permit.field_name == wrong_field
            ).all()
            
            # Update each permit
            for permit in permits_to_update:
                permit.field_name = correct_field
                updated_count += 1
            
            session.commit()
            logger.info(f"Bulk updated {updated_count} permits: '{wrong_field}' → '{correct_field}'")
        
        return {
            "success": True,
            "message": f"Updated {updated_count} permits with field name correction",
            "updated_count": updated_count,
            "wrong_field": wrong_field,
            "correct_field": correct_field
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Bulk field update error: {e}")
        raise HTTPException(status_code=500, detail="Failed to update field names")

@app.post("/enrich/all-missing")
async def enrich_all_missing_permits():
    """
    Enrich ALL permits that are missing detailed information (regardless of date).
    This is useful for backfilling existing permits.
    """
    try:
        from datetime import datetime, timedelta
        from db.session import get_session
        from db.models import Permit
        from services.enrichment.worker import EnrichmentWorker
        
        # Get all permits that need enrichment (missing field_name, acres, or section)
        with get_session() as session:
            permits = session.query(Permit).filter(
                (Permit.field_name == None) | 
                (Permit.field_name == '') |
                (Permit.acres == None) |
                (Permit.section == None)
            ).limit(50).all()  # Limit to 50 to prevent overload
            
            if not permits:
                return {
                    "success": True,
                    "message": "No permits need enrichment",
                    "enriched_count": 0
                }
            
            logger.info(f"🔄 Starting enrichment for {len(permits)} permits missing data")
            
            # Initialize enrichment worker
            worker = EnrichmentWorker()
            enriched_count = 0
            
            # Enrich each permit
            for permit in permits:
                try:
                    # Enrich the permit
                    success = await worker.enrich_permit(permit.id)
                    if success:
                        enriched_count += 1
                        logger.info(f"✅ Enriched permit {permit.status_no}")
                    else:
                        logger.warning(f"⚠️ Failed to enrich permit {permit.status_no}")
                        
                except Exception as e:
                    logger.error(f"❌ Error enriching permit {permit.status_no}: {e}")
                    continue
            
            return {
                "success": True,
                "message": f"Backfill enrichment completed",
                "total_permits": len(permits),
                "enriched_count": enriched_count
            }
            
    except Exception as e:
        logger.error(f"Backfill enrichment error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to enrich missing permits: {str(e)}")

@app.post("/enrich/today")
async def enrich_today_permits():
    """
    Enrich all permits from today with detailed information.
    This extracts field names, acres, location data, etc. from detail pages.
    """
    try:
        from datetime import datetime, timedelta
        from db.session import get_session
        from db.models import Permit
        from services.enrichment.worker import EnrichmentWorker
        
        today = datetime.now().date()
        
        # Get today's permits that need enrichment
        with get_session() as session:
            permits = session.query(Permit).filter(
                Permit.status_date >= today,
                Permit.status_date < today + timedelta(days=1)
            ).all()
            
            if not permits:
                return {
                    "success": True,
                    "message": "No permits found for today",
                    "enriched_count": 0,
                    "date": today.isoformat()
                }
            
            logger.info(f"🔄 Starting enrichment for {len(permits)} permits from {today}")
            
            # Initialize enrichment worker
            worker = EnrichmentWorker()
            enriched_count = 0
            
            # Enrich each permit
            for permit in permits:
                try:
                    # Check if already enriched
                    if permit.field_name or permit.acres or permit.section:
                        continue  # Skip already enriched permits
                    
                    # Enrich the permit
                    success = await worker.enrich_permit(permit.id)
                    if success:
                        enriched_count += 1
                        logger.info(f"✅ Enriched permit {permit.status_no}")
                    else:
                        logger.warning(f"⚠️ Failed to enrich permit {permit.status_no}")
                        
                except Exception as e:
                    logger.error(f"❌ Error enriching permit {permit.status_no}: {e}")
                    continue
            
            return {
                "success": True,
                "message": f"Enrichment completed for {today}",
                "total_permits": len(permits),
                "enriched_count": enriched_count,
                "date": today.isoformat()
            }
            
    except Exception as e:
        logger.error(f"Enrichment error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to enrich today's permits: {str(e)}")

@app.post("/api/v1/permits/reenrich")
async def reenrich_permits(request_data: dict):
    """
    Queue permits for re-enrichment (more reliable than parsing).
    Expected payload: {
        "status_numbers": ["123456", "789012"],
        "reason": "Manual flag from dashboard"
    }
    """
    try:
        status_numbers = request_data.get("status_numbers", [])
        reason = request_data.get("reason", "Manual re-enrichment request")
        
        if not status_numbers:
            raise HTTPException(status_code=400, detail="status_numbers is required")
        
        if not isinstance(status_numbers, list):
            raise HTTPException(status_code=400, detail="status_numbers must be a list")
        
        # Limit to prevent overwhelming the system
        if len(status_numbers) > 50:
            raise HTTPException(status_code=400, detail="Cannot re-enrich more than 50 permits at once")
        
        # Import enrichment components
        from services.enrichment.worker import EnrichmentWorker
        from db.session import get_session
        from db.models import Permit
        
        results = []
        worker = EnrichmentWorker()
        
        for status_no in status_numbers:
            try:
                # Find the permit in the database
                with get_session() as session:
                    permit = session.query(Permit).filter(Permit.status_no == status_no).first()
                    if not permit:
                        results.append({
                            "status_no": status_no,
                            "status": "error",
                            "error": "Permit not found"
                        })
                        continue
                    
                    permit_id = permit.id
                
                # Trigger enrichment for this permit
                success = await worker.enrich_permit(permit_id)
                
                if success:
                    results.append({
                        "status_no": status_no,
                        "status": "enriched",
                        "message": "Successfully re-enriched"
                    })
                    logger.info(f"Successfully re-enriched permit {status_no}: {reason}")
                else:
                    results.append({
                        "status_no": status_no,
                        "status": "failed",
                        "error": "Enrichment failed"
                    })
                    logger.warning(f"Failed to re-enrich permit {status_no}: {reason}")
                
            except Exception as e:
                logger.error(f"Failed to re-enrich permit {status_no}: {e}")
                results.append({
                    "status_no": status_no,
                    "status": "error",
                    "error": str(e)
                })
        
        successful_enriched = len([r for r in results if r["status"] == "enriched"])
        
        return {
            "success": True,
            "message": f"Re-enriched {successful_enriched} of {len(status_numbers)} permits",
            "results": results,
            "reason": reason
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Re-enrichment API error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to re-enrich permits: {str(e)}")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up background cron on shutdown."""
    try:
        from background_cron import background_cron
        background_cron.stop()
        logger.info("🛑 Background permit scraper stopped")
    except Exception as e:
        logger.error(f"❌ Failed to stop background cron: {e}")

@app.get("/api/v1/parsing/status")
async def get_parsing_status():
    """Get current parsing queue status and statistics."""
    try:
        # Import here to avoid initialization issues
        from services.parsing.queue import parsing_queue
        stats = parsing_queue.get_statistics()
        
        return {
            "success": True,
            "stats": stats,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Parsing status error: {e}")
        # Return default stats instead of failing completely
        return {
            "success": True,
            "stats": {
                "total_jobs": 0,
                "pending": 0,
                "in_progress": 0,
                "success": 0,
                "failed": 0,
                "manual_review": 0,
                "success_rate": 0.0,
                "avg_confidence": 0.0
            },
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }

@app.get("/api/v1/parsing/failed")
async def get_failed_parsing_jobs(limit: int = Query(default=20, description="Number of failed jobs to return")):
    """Get failed parsing jobs for manual review."""
    try:
        # Import here to avoid initialization issues
        from services.parsing.queue import parsing_queue
        failed_jobs = parsing_queue.get_failed_jobs(limit)
        
        # Convert jobs to serializable format
        jobs_data = []
        for job in failed_jobs:
            jobs_data.append({
                "permit_id": job.permit_id,
                "status_no": job.status_no,
                "status": job.status.value,
                "attempt_count": job.attempt_count,
                "error_message": job.error_message,
                "last_attempt": job.last_attempt.isoformat() if job.last_attempt else None,
                "confidence_score": job.confidence_score
            })
        
        return {
            "success": True,
            "failed_jobs": jobs_data,
            "total_count": len(failed_jobs)
        }
        
    except Exception as e:
        logger.error(f"Failed jobs error: {e}")
        # Return empty results instead of failing completely
        return {
            "success": True,
            "failed_jobs": [],
            "total_count": 0,
            "error": str(e)
        }

@app.post("/api/v1/parsing/retry/{permit_id}")
async def retry_parsing_job(permit_id: str):
    """Manually retry a failed parsing job."""
    try:
        # Import here to avoid initialization issues
        from services.parsing.queue import parsing_queue
        success = parsing_queue.retry_job(permit_id)
        
        if success:
            return {
                "success": True,
                "message": f"Job {permit_id} queued for retry"
            }
        else:
            return {
                "success": False,
                "message": "Job not found or not eligible for retry"
            }
            
    except Exception as e:
        logger.error(f"Retry job error: {e}")
        return {
            "success": False,
            "message": f"Failed to retry parsing job: {str(e)}"
        }

@app.post("/api/v1/parsing/process")
async def process_parsing_queue():
    """Manually trigger parsing queue processing."""
    try:
        # Import here to avoid initialization issues
        from services.parsing.worker import parsing_worker
        await parsing_worker.process_queue(batch_size=5)
        
        return {
            "success": True,
            "message": "Parsing queue processed"
        }
        
    except Exception as e:
        logger.error(f"Process queue error: {e}")
        return {
            "success": False,
            "message": f"Failed to process parsing queue: {str(e)}"
        }

@app.post("/api/v1/permits/{status_no}/flag-injection-well")
async def flag_injection_well(status_no: str, request: Request):
    """
    Flag a permit as an injection well (excludes it from trend analysis).
    This is typically called when dismissing an injection well.
    """
    try:
        # Get org_id for tenant isolation
        org_id = request.query_params.get('org_id') or request.headers.get('X-Org-ID') or 'default_org'
        
        logger.info(f"🚫 Flagging injection well: {status_no} (org: {org_id})")
        
        with get_session() as session:
            # Find the permit with tenant isolation
            permit = session.query(Permit).filter(
                Permit.status_no == status_no,
                Permit.org_id == org_id
            ).first()
            
            # If not found with org_id, try without for legacy data
            if not permit and org_id == 'default_org':
                permit = session.query(Permit).filter(
                    Permit.status_no == status_no
                ).first()
            
            if not permit:
                raise HTTPException(status_code=404, detail=f"Permit {status_no} not found")
            
            # Flag as injection well - TEMPORARILY DISABLED UNTIL MIGRATION RUNS
            # permit.is_injection_well = True
            # session.commit()
            pass  # Temporarily do nothing
            
            logger.info(f"✅ Flagged permit {status_no} as injection well - will be excluded from trend analysis")
            
            return {
                "success": True,
                "message": f"Permit {status_no} flagged as injection well",
                "status_no": permit.status_no,
                "operator_name": permit.operator_name,
                "lease_name": permit.lease_name
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Flag injection well error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to flag injection well: {str(e)}")

@app.post("/api/v1/field-corrections/correct")
async def correct_field_name(request: Request, request_data: dict):
    """
    Record a field name correction for learning with tenant isolation and optimistic concurrency.
    Expected payload: {
        "permit_id": 123,
        "status_no": "910767",
        "wrong_field": "LEASE NAME ABC",
        "correct_field": "SPRABERRY (TREND AREA)",
        "detail_url": "https://...",
        "html_context": "...",
        "if_version": 5  # Optional: for optimistic concurrency control
    }
    """
    try:
        # Get org_id manually to avoid dependency issues
        org_id = request.query_params.get('org_id') or request.headers.get('X-Org-ID') or 'default_org'
        
        permit_id = request_data.get("permit_id")
        status_no = request_data.get("status_no")
        wrong_field = request_data.get("wrong_field")
        correct_field = request_data.get("correct_field")
        correct_reservoir = request_data.get("correct_reservoir")
        detail_url = request_data.get("detail_url")
        html_context = request_data.get("html_context")
        if_version = request_data.get("if_version")  # Optional optimistic concurrency control
        
        # Debug logging for troubleshooting
        logger.info(f"Field correction request: status_no='{status_no}', wrong_field='{wrong_field}', correct_field='{correct_field}'")
        
        if not all([status_no, wrong_field, correct_field]):
            missing_fields = []
            if not status_no: missing_fields.append("status_no")
            if not wrong_field: missing_fields.append("wrong_field") 
            if not correct_field: missing_fields.append("correct_field")
            error_msg = f"Missing required fields: {', '.join(missing_fields)}"
            logger.error(f"HTTP 400 - {error_msg}")
            raise HTTPException(status_code=400, detail=error_msg)
        
        # If permit_id is not provided, try to find it by status_no (with tenant isolation)
        if not permit_id:
            with get_session() as session:
                # First try with org_id filtering
                permit = session.query(Permit).filter(
                    Permit.status_no == status_no,
                    Permit.org_id == org_id  # Tenant isolation
                ).first()
                
                # If not found and using default_org, try without org_id filtering (for legacy data)
                if not permit and org_id == 'default_org':
                    permit = session.query(Permit).filter(
                        Permit.status_no == status_no
                    ).first()
                
                if permit:
                    permit_id = permit.id
                else:
                    raise HTTPException(status_code=404, detail=f"Permit with status {status_no} not found")
        
        # Update the permit's field name (simplified approach)
        with get_session() as session:
            # Find the permit
            permit = session.query(Permit).filter(
                Permit.id == permit_id,
                Permit.org_id == org_id
            ).first()
            
            # If not found and using default_org, try without org_id filtering (for legacy data)
            if not permit and org_id == 'default_org':
                permit = session.query(Permit).filter(
                    Permit.id == permit_id
                ).first()
            
            if not permit:
                raise HTTPException(status_code=404, detail=f"Permit {permit_id} not found")
            
            # Update the field name (version will be auto-incremented by SQLAlchemy event listener)
            permit.field_name = correct_field
            session.commit()
            logger.info(f"Updated permit {status_no} (org: {org_id}) field name: '{wrong_field}' → '{correct_field}'")
        
        # Record the correction for machine learning and future enhancement
        try:
            with get_session() as learning_session:
                # Determine pattern category based on wrong field characteristics
                pattern_category = "unknown"
                if any(pattern in wrong_field.lower() for pattern in ['commission staff', 'expresses no opinion']):
                    pattern_category = "commission_comment"
                elif any(pattern in wrong_field for pattern in ['/202', ':', 'AM', 'PM']):
                    pattern_category = "timestamp"
                elif len(wrong_field) > 100:
                    pattern_category = "long_comment"
                elif any(term in wrong_field.lower() for term in ['application', 'amend', 'surface']):
                    pattern_category = "application_text"
                else:
                    pattern_category = "geological"
                
                correction = FieldCorrection(
                    org_id=org_id,
                    permit_id=permit_id,
                    status_no=status_no,
                    wrong_field_name=wrong_field,
                    correct_field_name=correct_field,
                    detail_url=detail_url,
                    html_context=html_context or "",
                    corrected_by="manual_correction"
                )
                learning_session.add(correction)
                learning_session.commit()
                logger.info(f"Recorded field correction for learning: {status_no} ({pattern_category})")
        except Exception as e:
            logger.warning(f"Failed to record correction for learning: {e}")
            # Don't fail the whole request if learning fails
        
        return {
            "success": True,
            "message": f"Field name corrected: '{wrong_field}' → '{correct_field}'",
            "status_no": status_no
        }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Field correction error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to record correction: {str(e)}")

@app.get("/api/v1/field-corrections/suggest/{permit_id}")
async def suggest_field_name(permit_id: int):
    """Get field name suggestion based on learned patterns."""
    try:
        from db.session import get_session
        from db.models import Permit
        
        with get_session() as session:
            permit = session.query(Permit).filter(Permit.id == permit_id).first()
            if not permit:
                raise HTTPException(status_code=404, detail="Permit not found")
            
            # TODO: Implement field learning suggestion
            suggestion = None  # field_learning.suggest_field_name(...)
            
            return {
                "permit_id": permit_id,
                "status_no": permit.status_no,
                "current_field": permit.field_name,
                "suggested_field": suggestion,
                "has_suggestion": suggestion is not None
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Field suggestion error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get suggestion: {str(e)}")

@app.get("/api/v1/field-corrections/stats")
async def get_correction_stats():
    """Get statistics about field name corrections."""
    try:
        # TODO: Implement correction stats
        stats = {"corrections": 0, "patterns": 0}  # field_learning.get_correction_stats()
        return stats
        
    except Exception as e:
        logger.error(f"Correction stats error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")

@app.post("/api/v1/field-corrections/apply-learned")
async def apply_learned_corrections(limit: int = 20):
    """Apply learned corrections to similar permits."""
    try:
        # TODO: Implement bulk apply corrections
        result = {"applied": 0, "message": "Feature not implemented"}  # field_learning.apply_learned_corrections(limit=limit)
        return result
        
    except Exception as e:
        logger.error(f"Apply corrections error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to apply corrections: {str(e)}")

@app.delete("/api/v1/permits/{status_no}")
async def delete_permit(status_no: str, request: Request):
    """
    Delete a permit from the database (typically used for injection wells).
    This is a permanent deletion and cannot be undone.
    """
    print(f"🚨 DELETE ENDPOINT REACHED: {status_no}")
    try:
        # Get org_id for tenant isolation
        org_id = request.query_params.get('org_id') or request.headers.get('X-Org-ID') or 'default_org'
        
        print(f"🗑️ DELETE REQUEST: status_no='{status_no}', org_id='{org_id}'")
        logger.error(f"🗑️ DELETE REQUEST: status_no='{status_no}', org_id='{org_id}'")
        
        with get_session() as session:
            # Find the permit with tenant isolation
            permit = session.query(Permit).filter(
                Permit.status_no == status_no,
                Permit.org_id == org_id
            ).first()
            
            # If not found with org_id, try without for legacy data
            if not permit and org_id == 'default_org':
                permit = session.query(Permit).filter(
                    Permit.status_no == status_no
                ).first()
            
            if not permit:
                raise HTTPException(status_code=404, detail=f"Permit {status_no} not found")
            
            # Log the deletion for audit purposes
            print(f"🎯 FOUND PERMIT TO DELETE: {status_no} (org: {org_id}) - {permit.operator_name} - {permit.lease_name} - ID: {permit.id}")
            logger.error(f"🎯 FOUND PERMIT TO DELETE: {status_no} (org: {org_id}) - {permit.operator_name} - {permit.lease_name} - ID: {permit.id}")
            
            # Store permit info before deletion
            permit_info = {
                "status_no": permit.status_no,
                "operator_name": permit.operator_name,
                "lease_name": permit.lease_name,
                "id": permit.id
            }
            
            # Delete the permit
            print(f"🗑️ DELETING PERMIT: {status_no} from database...")
            session.delete(permit)
            print(f"🔄 COMMITTING TRANSACTION...")
            session.commit()
            print(f"✅ TRANSACTION COMMITTED")
            
            # Verify deletion by trying to find the permit again
            print(f"🔍 VERIFYING DELETION...")
            verification = session.query(Permit).filter(Permit.status_no == status_no).first()
            if verification:
                print(f"❌ DELETION FAILED: Permit {status_no} still exists after deletion! Found permit ID: {verification.id}")
                logger.error(f"❌ DELETION FAILED: Permit {status_no} still exists after deletion! Found permit ID: {verification.id}")
                raise HTTPException(status_code=500, detail="Deletion failed - permit still exists")
            else:
                print(f"✅ DELETION VERIFIED: Permit {status_no} successfully removed from database")
                logger.error(f"✅ DELETION VERIFIED: Permit {status_no} successfully removed from database")
            
            return {
                "success": True,
                "message": f"Permit {status_no} deleted successfully",
                "status_no": permit_info["status_no"],
                "operator_name": permit_info["operator_name"],
                "lease_name": permit_info["lease_name"]
            }
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"🚨 DELETE EXCEPTION: {e}")
        logger.error(f"Delete permit error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to delete permit: {str(e)}")

@app.post("/api/v1/sync/data")
async def sync_data(request_data: dict):
    """Sync data between desktop and mobile devices."""
    try:
        # For now, just acknowledge the sync request
        # In the future, this could store sync data in the database
        # and provide cross-device synchronization
        
        mappings = request_data.get("mappings", {})
        review_queue = request_data.get("reviewQueue", [])
        timestamp = request_data.get("timestamp", 0)
        
        logger.info(f"Data sync request received: {len(mappings)} mappings, {len(review_queue)} review items")
        
        return {
            "success": True,
            "message": "Data sync acknowledged",
            "synced_at": timestamp,
            "server_time": int(time.time() * 1000)
        }
        
    except Exception as e:
        logger.error(f"Data sync error: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to sync data: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
