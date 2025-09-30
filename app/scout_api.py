"""
Scout v2.1 API Endpoints
Additive endpoints for Scout insights system
"""

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc, func
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel
import json
import uuid
import psycopg2

from db.session import get_session
from db.scout_models import Signal, ScoutInsight, ScoutInsightUserState, InsightUserState, ConfidenceLevel

router = APIRouter(prefix="/api/v1/scout", tags=["scout"])

# Pydantic models for API
class InsightResponse(BaseModel):
    id: str
    title: str
    what_happened: List[str]
    why_it_matters: List[str]
    confidence: str
    confidence_reasons: List[str]
    next_checks: List[str]
    source_urls: List[Dict[str, str]]
    related_permit_ids: List[str]
    county: Optional[str]
    state: Optional[str]
    operator_keys: List[str]
    analytics: Dict[str, Any]
    created_at: datetime
    user_state: str
    user_state_updated_at: Optional[datetime]
    dismiss_reason: Optional[str]

class UserStateUpdate(BaseModel):
    state: str  # 'kept' or 'dismissed'
    dismiss_reason: Optional[str] = None

class UndoRequest(BaseModel):
    undo_token: str

def get_current_user_id() -> str:
    """Get current user ID - placeholder for future auth integration"""
    return "default_user"  # For now, single user system

def parse_json_field(field_value: str) -> List:
    """Safely parse JSON field, return empty list if invalid"""
    try:
        return json.loads(field_value) if field_value else []
    except (json.JSONDecodeError, TypeError):
        return []

def parse_json_dict_field(field_value: str) -> List[Dict]:
    """Safely parse JSON field expecting list of dicts"""
    try:
        result = json.loads(field_value) if field_value else []
        return result if isinstance(result, list) else []
    except (json.JSONDecodeError, TypeError):
        return []

@router.get("/insights")
async def get_insights(
    org_id: str = Query("default_org"),
    county: Optional[str] = Query(None),
    operator: Optional[str] = Query(None),
    confidence: Optional[str] = Query(None),  # low, medium, high
    days: int = Query(30, ge=1, le=365),
    breakouts_only: bool = Query(False),
    state_filter: str = Query("default"),  # default, kept, dismissed, archived, all
):
    """Get Scout insights with filters"""
    
    user_id = get_current_user_id()
    
    try:
        with get_session() as session:
            # Base query with user state join
            query = session.query(ScoutInsight, ScoutInsightUserState).outerjoin(
                ScoutInsightUserState,
                and_(
                    ScoutInsightUserState.insight_id == ScoutInsight.id,
                    ScoutInsightUserState.org_id == org_id,
                    ScoutInsightUserState.user_id == user_id
                )
            ).filter(ScoutInsight.org_id == org_id)
            
            # Date filter
            since_date = datetime.now(timezone.utc) - timedelta(days=days)
            query = query.filter(ScoutInsight.created_at >= since_date)
            
            # County filter
            if county:
                query = query.filter(ScoutInsight.county.ilike(f"%{county}%"))
            
            # Operator filter
            if operator:
                query = query.filter(ScoutInsight.operator_keys.any(operator.upper()))
            
            # Confidence filter
            if confidence:
                try:
                    conf_level = ConfidenceLevel(confidence.lower())
                    query = query.filter(ScoutInsight.confidence == conf_level)
                except ValueError:
                    pass  # Invalid confidence level, ignore filter
            
            # Breakouts only filter
            if breakouts_only:
                query = query.filter(ScoutInsight.analytics['is_breakout'].astext.cast(bool) == True)
            
            # State filter
            if state_filter == "kept":
                query = query.filter(ScoutInsightUserState.state == InsightUserState.KEPT)
            elif state_filter == "dismissed":
                query = query.filter(ScoutInsightUserState.state == InsightUserState.DISMISSED)
            elif state_filter == "archived":
                query = query.filter(ScoutInsightUserState.state == InsightUserState.ARCHIVED)
            elif state_filter == "default":
                # Show default state OR no user state record (which defaults to default)
                query = query.filter(
                    or_(
                        ScoutInsightUserState.state == InsightUserState.DEFAULT,
                        ScoutInsightUserState.state.is_(None)
                    )
                )
            # "all" shows everything, no additional filter
            
            # Order by created_at desc, with kept items pinned to top within date groups
            query = query.order_by(
                desc(func.coalesce(ScoutInsightUserState.kept_at, datetime.min)),
                desc(ScoutInsight.created_at)
            )
            
            results = query.all()
            
            # Convert to response format
            insights = []
            for insight, user_state in results:
                # Parse JSON fields
                what_happened = parse_json_field(insight.what_happened)
                why_it_matters = parse_json_field(insight.why_it_matters)
                confidence_reasons = parse_json_field(insight.confidence_reasons)
                next_checks = parse_json_field(insight.next_checks)
                source_urls = parse_json_dict_field(insight.source_urls)
                
                # Determine user state
                current_state = "default"
                state_updated_at = None
                dismiss_reason = None
                
                if user_state:
                    current_state = user_state.state.value
                    if user_state.kept_at:
                        state_updated_at = user_state.kept_at
                    elif user_state.dismissed_at:
                        state_updated_at = user_state.dismissed_at
                        dismiss_reason = user_state.dismiss_reason
                    elif user_state.archived_at:
                        state_updated_at = user_state.archived_at
                
                insights.append({
                    "id": str(insight.id),
                    "title": insight.title,
                    "what_happened": what_happened,
                    "why_it_matters": why_it_matters,
                    "confidence": insight.confidence.value,
                    "confidence_reasons": confidence_reasons,
                    "next_checks": next_checks,
                    "source_urls": source_urls,
                    "related_permit_ids": insight.related_permit_ids or [],
                    "county": insight.county,
                    "state": insight.state,
                    "operator_keys": insight.operator_keys or [],
                    "analytics": insight.analytics or {},
                    "created_at": insight.created_at,
                    "user_state": current_state,
                    "user_state_updated_at": state_updated_at,
                    "dismiss_reason": dismiss_reason
                })
            
            return {"success": True, "insights": insights}
    
    except Exception as e:
        # If Scout tables don't exist yet, return empty results
        if "does not exist" in str(e) or "UndefinedTable" in str(e):
            return {"success": True, "insights": []}
        else:
            raise HTTPException(status_code=500, detail=f"Failed to get insights: {e}")

@router.post("/insights/{insight_id}/state")
async def update_insight_state(
    insight_id: str,
    update: UserStateUpdate,
    org_id: str = Query("default_org")
):
    """Update user state for an insight (Keep/Dismiss)"""
    
    user_id = get_current_user_id()
    
    try:
        with get_session() as session:
            # Validate insight exists
            insight = session.query(ScoutInsight).filter(
                ScoutInsight.id == insight_id,
                ScoutInsight.org_id == org_id
            ).first()
            
            if not insight:
                raise HTTPException(status_code=404, detail="Insight not found")
        
        # Validate state
        if update.state not in ["kept", "dismissed"]:
            raise HTTPException(status_code=400, detail="State must be 'kept' or 'dismissed'")
        
        # Get or create user state record
        user_state = session.query(ScoutInsightUserState).filter(
            ScoutInsightUserState.org_id == org_id,
            ScoutInsightUserState.user_id == user_id,
            ScoutInsightUserState.insight_id == insight_id
        ).first()
        
        now = datetime.now(timezone.utc)
        undo_token = uuid.uuid4()
        undo_expires = now + timedelta(seconds=8)  # 8 second undo window
        
        if not user_state:
            # Create new user state record
            user_state = ScoutInsightUserState(
                org_id=org_id,
                user_id=user_id,
                insight_id=insight_id,
                state=InsightUserState(update.state),
                undo_token=undo_token,
                undo_expires_at=undo_expires,
                created_at=now,
                updated_at=now
            )
            
            if update.state == "kept":
                user_state.kept_at = now
            elif update.state == "dismissed":
                user_state.dismissed_at = now
                user_state.dismiss_reason = update.dismiss_reason
                
            session.add(user_state)
        else:
            # Update existing record
            user_state.state = InsightUserState(update.state)
            user_state.undo_token = undo_token
            user_state.undo_expires_at = undo_expires
            user_state.updated_at = now
            
            # Clear previous state timestamps
            user_state.kept_at = None
            user_state.dismissed_at = None
            user_state.archived_at = None
            user_state.dismiss_reason = None
            
            if update.state == "kept":
                user_state.kept_at = now
            elif update.state == "dismissed":
                user_state.dismissed_at = now
                user_state.dismiss_reason = update.dismiss_reason
        
            session.commit()
            
            return {
                "success": True,
                "user_state": {
                    "id": str(user_state.id),
                    "state": user_state.state.value,
                    "undo_token": str(user_state.undo_token),
                    "undo_expires_at": user_state.undo_expires_at.isoformat()
                }
            }
    
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        # If Scout tables don't exist yet, return appropriate error
        if "does not exist" in str(e) or "UndefinedTable" in str(e):
            raise HTTPException(status_code=503, detail="Scout system not yet initialized")
        else:
            raise HTTPException(status_code=500, detail=f"Failed to update insight state: {e}")

@router.post("/insights/{insight_id}/undo")
async def undo_insight_state(
    insight_id: str,
    undo: UndoRequest,
    org_id: str = Query("default_org")
):
    """Undo the last user state change for an insight"""
    
    user_id = get_current_user_id()
    
    try:
        with get_session() as session:
            # Find user state with matching undo token
            user_state = session.query(ScoutInsightUserState).filter(
                ScoutInsightUserState.org_id == org_id,
                ScoutInsightUserState.user_id == user_id,
                ScoutInsightUserState.insight_id == insight_id,
                ScoutInsightUserState.undo_token == undo.undo_token,
                ScoutInsightUserState.undo_expires_at > datetime.now(timezone.utc)
            ).first()
            
            if not user_state:
                raise HTTPException(status_code=404, detail="Undo token invalid or expired")
            
            # Revert to default state
            user_state.state = InsightUserState.DEFAULT
            user_state.kept_at = None
            user_state.dismissed_at = None
            user_state.archived_at = None
            user_state.dismiss_reason = None
            user_state.undo_token = None
            user_state.undo_expires_at = None
            user_state.updated_at = datetime.now(timezone.utc)
            
            session.commit()
            
            return {
                "success": True,
                "user_state": {
                    "id": str(user_state.id),
                    "state": user_state.state.value
                }
            }
    
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        # If Scout tables don't exist yet, return appropriate error
        if "does not exist" in str(e) or "UndefinedTable" in str(e):
            raise HTTPException(status_code=503, detail="Scout system not yet initialized")
        else:
            raise HTTPException(status_code=500, detail=f"Failed to undo insight state: {e}")

@router.get("/stats")
async def get_scout_stats(org_id: str = Query("default_org")):
    """Get Scout statistics"""
    
    try:
        with get_session() as session:
            # Count insights by state
            total_insights = session.query(ScoutInsight).filter(
                ScoutInsight.org_id == org_id
            ).count()
            
            # For now, return basic stats
            # In a real implementation, you'd calculate more detailed statistics
            return {
                "success": True,
                "stats": {
                    "total_insights": total_insights,
                    "signals_processed": 0,  # Placeholder
                    "breakouts_detected": 0,  # Placeholder
                    "active_crawlers": 0     # Placeholder
                }
            }
    
    except Exception as e:
        # If Scout tables don't exist yet, return zero stats
        if "does not exist" in str(e) or "UndefinedTable" in str(e):
            return {
                "success": True,
                "stats": {
                    "total_insights": 0,
                    "signals_processed": 0,
                    "breakouts_detected": 0,
                    "active_crawlers": 0
                }
            }
        else:
            raise HTTPException(status_code=500, detail=f"Failed to get stats: {e}")

@router.get("/test-db")
async def test_database_connection():
    """Test basic database connectivity"""
    try:
        import os
        from sqlalchemy import create_engine, text
        
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            return {"success": False, "error": "No DATABASE_URL"}
        
        engine = create_engine(database_url)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            test_value = result.fetchone()[0]
            
            # Check existing tables
            tables_result = conn.execute(text("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """))
            tables = [row[0] for row in tables_result]
            
            return {
                "success": True,
                "database_connected": test_value == 1,
                "existing_tables": tables,
                "database_url_set": bool(database_url)
            }
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        }

@router.post("/setup")
async def setup_scout_tables(org_id: str = Query("default_org")):
    """Robust Scout v2.2 database setup using psycopg2"""
    
    try:
        import os
        import psycopg2
        
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            logger.error("❌ No DATABASE_URL environment variable found")
            return {
                "success": False,
                "message": "No DATABASE_URL configured",
                "error": "DATABASE_URL environment variable not set"
            }
        
        # Convert SQLAlchemy URL to psycopg2 format
        if database_url.startswith("postgresql+psycopg"):
            database_url = database_url.replace("postgresql+psycopg", "postgresql")
        
        logger.info("🔧 Starting robust Scout v2.2 database setup...")
        
        # Idempotent DDL SQL - safe to run multiple times
        DDL_SQL = """
        -- Create Scout v2.2 tables with CHECK constraints instead of enums
        CREATE TABLE IF NOT EXISTS public.signals (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id TEXT NOT NULL DEFAULT 'default_org',
            found_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            source_url TEXT NOT NULL,
            source_type TEXT CHECK (source_type IN ('forum','news','pr','filing','gov_bulletin','blog','social','other')) NOT NULL DEFAULT 'other',
            state TEXT,
            county TEXT,
            play_basin TEXT,
            operators TEXT[] DEFAULT '{}',
            unit_tokens TEXT[] DEFAULT '{}',
            keywords TEXT[] DEFAULT '{}',
            claim_type TEXT CHECK (claim_type IN ('rumor','likely','confirmed')) NOT NULL DEFAULT 'likely',
            timeframe TEXT CHECK (timeframe IN ('past','now','next_90d')) NOT NULL DEFAULT 'now',
            summary TEXT NOT NULL,
            raw_excerpt TEXT
        );

        CREATE INDEX IF NOT EXISTS ix_signals_org_found ON public.signals (org_id, found_at DESC);
        CREATE INDEX IF NOT EXISTS ix_signals_org_county ON public.signals (org_id, county);
        CREATE INDEX IF NOT EXISTS ix_signals_org_ops ON public.signals USING gin (operators);
        CREATE INDEX IF NOT EXISTS ix_signals_keywords ON public.signals USING gin (keywords);

        CREATE TABLE IF NOT EXISTS public.scout_insights (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id TEXT NOT NULL DEFAULT 'default_org',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            title TEXT NOT NULL,
            what_happened TEXT NOT NULL,     -- markdown bullet list
            why_it_matters TEXT NOT NULL,    -- markdown bullet list
            confidence TEXT CHECK (confidence IN ('low','medium','high')) NOT NULL DEFAULT 'medium',
            confidence_reasons TEXT,
            next_checks TEXT[] DEFAULT '{}',
            source_urls TEXT[] NOT NULL DEFAULT '{}',
            related_permit_ids TEXT[] DEFAULT '{}',
            county TEXT,
            state TEXT,
            operator_keys TEXT[] DEFAULT '{}',
            analytics JSONB DEFAULT '{}'::jsonb,
            dedup_key TEXT UNIQUE
        );

        CREATE INDEX IF NOT EXISTS ix_insights_org_created ON public.scout_insights (org_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS ix_insights_org_county ON public.scout_insights (org_id, county);
        CREATE INDEX IF NOT EXISTS ix_insights_org_ops ON public.scout_insights USING gin (operator_keys);
        CREATE INDEX IF NOT EXISTS ix_insights_org_analytics ON public.scout_insights USING gin ((analytics));

        CREATE TABLE IF NOT EXISTS public.scout_insight_user_state (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            org_id TEXT NOT NULL DEFAULT 'default_org',
            user_id TEXT NOT NULL DEFAULT 'default_user',
            insight_id UUID NOT NULL REFERENCES public.scout_insights(id) ON DELETE CASCADE,
            state TEXT CHECK (state IN ('default','kept','dismissed','archived')) NOT NULL DEFAULT 'default',
            kept_at TIMESTAMPTZ,
            dismissed_at TIMESTAMPTZ,
            dismiss_reason TEXT,
            archived_at TIMESTAMPTZ,
            undo_token UUID,
            undo_expires_at TIMESTAMPTZ
        );

        CREATE UNIQUE INDEX IF NOT EXISTS ux_state_org_user_insight
            ON public.scout_insight_user_state (org_id, user_id, insight_id);
        CREATE INDEX IF NOT EXISTS ix_state_filter
            ON public.scout_insight_user_state (org_id, user_id, state, insight_id);
        CREATE INDEX IF NOT EXISTS ix_state_archived
            ON public.scout_insight_user_state (org_id, user_id, archived_at);

        -- Ensure field_corrections has org_id column
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'field_corrections' AND column_name = 'org_id'
            ) THEN
                ALTER TABLE field_corrections 
                ADD COLUMN org_id TEXT NOT NULL DEFAULT 'default_org';
                
                CREATE INDEX IF NOT EXISTS idx_field_corrections_org_id 
                ON field_corrections(org_id);
            END IF;
        END $$;
        """
        
        # Execute DDL with psycopg2
        with psycopg2.connect(database_url) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                logger.info("📝 Executing Scout v2.2 DDL...")
                cur.execute(DDL_SQL)
                logger.info("✅ DDL execution completed")
        
        # Verify tables were created
        with psycopg2.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('signals', 'scout_insights', 'scout_insight_user_state')
                    ORDER BY table_name
                """)
                tables = [row[0] for row in cur.fetchall()]
        
        if len(tables) >= 2:  # At least signals and scout_insights
            logger.info(f"🎉 SUCCESS: Scout v2.2 tables created! Found: {', '.join(tables)}")
            return {
                "success": True,
                "message": f"Scout v2.2 setup completed successfully! Found {len(tables)} tables: {', '.join(tables)}. Real insights are now enabled.",
                "tables_created": tables,
                "org_id": org_id
            }
        else:
            logger.warning(f"⚠️ Only {len(tables)} Scout tables found: {tables}")
            return {
                "success": False,
                "message": f"Partial setup: Only {len(tables)} Scout tables found: {tables}",
                "tables_created": tables,
                "org_id": org_id
            }
                
    except Exception as e:
        logger.error(f"❌ Scout setup failed: {e}", exc_info=True)
        return {
            "success": False,
            "message": f"Setup failed: {str(e)}",
            "error": str(e),
            "error_type": type(e).__name__,
            "org_id": org_id
        }

@router.post("/crawl/all")
async def trigger_all_sources_crawl(org_id: str = Query("default_org")):
    """Scout v2.2: Crawl all sources (news, PR, SEC, social, forums, gov bulletins)"""
    try:
        from services.scout.scout_service import ScoutService
        
        scout_service = ScoutService(org_id)
        results = await scout_service.crawl_all_sources()
        
        return {
            "success": True,
            "results": results,
            "message": f"Crawled {results['total_crawled']} items from all sources, created {results['signals_created']} signals, generated {results['insights_created']} insights"
        }
    except Exception as e:
        logger.error(f"Error during all-sources crawl: {e}", exc_info=True)
        
        # Fallback to compatibility mode
        try:
            from services.scout.compatibility import CompatibilityService
            compat_service = CompatibilityService(org_id)
            results = await compat_service.simulate_crawl_all_sources()
            
            return {
                "success": True,
                "results": results,
                "message": f"Compatibility mode: Simulated crawl of {results['total_crawled']} items, generated {results['insights_created']} demo insights",
                "compatibility_mode": True
            }
        except Exception as fallback_error:
            logger.error(f"Fallback also failed: {fallback_error}")
            raise HTTPException(status_code=500, detail=f"Crawl failed: {e}")

@router.get("/insights/demo")
async def get_demo_insights(org_id: str = Query("default_org")):
    """Get Scout v2.2 demo insights (compatibility mode)"""
    try:
        from services.scout.compatibility import CompatibilityService
        
        compat_service = CompatibilityService(org_id)
        demo_insights = await compat_service.create_demo_insights_v22()
        
        return {
            "success": True,
            "insights": demo_insights,
            "total": len(demo_insights),
            "message": "Scout v2.2 demo insights with enhanced analytics"
        }
    except Exception as e:
        logger.error(f"Error creating demo insights: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Demo insights failed: {e}")

@router.post("/crawl/mrf")
async def trigger_mrf_crawl(org_id: str = Query("default_org")):
    """Legacy MRF-only crawling"""
    
    try:
        from services.scout.scout_service import ScoutService
        
        scout_service = ScoutService(org_id)
        results = await scout_service.crawl_and_process_mrf()
        
        return {
            "success": True,
            "results": results,
            "message": f"Crawled {results['crawled_discussions']} discussions, created {results['signals_created']} signals, generated {results['insights_created']} insights"
        }
        
    except Exception as e:
        logger.error(f"Error during manual MRF crawl: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Crawl failed: {e}")