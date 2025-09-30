"""
Scout v2.1 API Endpoints
Additive endpoints for Scout insights system
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, desc, func
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from pydantic import BaseModel
import json
import uuid

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

@router.get("/insights", response_model=List[InsightResponse])
async def get_insights(
    org_id: str = Query("default_org"),
    county: Optional[str] = Query(None),
    operator: Optional[str] = Query(None),
    confidence: Optional[str] = Query(None),  # low, medium, high
    days: int = Query(30, ge=1, le=365),
    breakouts_only: bool = Query(False),
    state_filter: str = Query("default"),  # default, kept, dismissed, archived, all
    session: Session = Depends(get_session)
):
    """Get Scout insights with filters"""
    
    user_id = get_current_user_id()
    
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
        
        insights.append(InsightResponse(
            id=str(insight.id),
            title=insight.title,
            what_happened=what_happened,
            why_it_matters=why_it_matters,
            confidence=insight.confidence.value,
            confidence_reasons=confidence_reasons,
            next_checks=next_checks,
            source_urls=source_urls,
            related_permit_ids=insight.related_permit_ids or [],
            county=insight.county,
            state=insight.state,
            operator_keys=insight.operator_keys or [],
            analytics=insight.analytics or {},
            created_at=insight.created_at,
            user_state=current_state,
            user_state_updated_at=state_updated_at,
            dismiss_reason=dismiss_reason
        ))
    
    return insights

@router.post("/insights/{insight_id}/state")
async def update_insight_state(
    insight_id: str,
    update: UserStateUpdate,
    org_id: str = Query("default_org"),
    session: Session = Depends(get_session)
):
    """Update user state for an insight (Keep/Dismiss)"""
    
    user_id = get_current_user_id()
    
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
    
    # TODO: Emit WebSocket event for real-time updates
    # emit_scout_insight_user_state_updated(org_id, user_id, insight_id, user_state)
    
    return {
        "success": True,
        "state": update.state,
        "undo_token": str(undo_token),
        "undo_expires_at": undo_expires
    }

@router.post("/insights/{insight_id}/undo")
async def undo_state_change(
    insight_id: str,
    undo_request: UndoRequest,
    org_id: str = Query("default_org"),
    session: Session = Depends(get_session)
):
    """Undo a recent state change"""
    
    user_id = get_current_user_id()
    
    # Find user state with matching undo token
    user_state = session.query(ScoutInsightUserState).filter(
        ScoutInsightUserState.org_id == org_id,
        ScoutInsightUserState.user_id == user_id,
        ScoutInsightUserState.insight_id == insight_id,
        ScoutInsightUserState.undo_token == undo_request.undo_token
    ).first()
    
    if not user_state:
        raise HTTPException(status_code=404, detail="Undo token not found")
    
    # Check if undo token is still valid
    now = datetime.now(timezone.utc)
    if user_state.undo_expires_at and now > user_state.undo_expires_at:
        raise HTTPException(status_code=400, detail="Undo token expired")
    
    # Revert to default state
    user_state.state = InsightUserState.DEFAULT
    user_state.kept_at = None
    user_state.dismissed_at = None
    user_state.dismiss_reason = None
    user_state.undo_token = None
    user_state.undo_expires_at = None
    user_state.updated_at = now
    
    session.commit()
    
    # TODO: Emit WebSocket event
    # emit_scout_insight_user_state_updated(org_id, user_id, insight_id, user_state)
    
    return {"success": True, "state": "default"}

@router.get("/stats")
async def get_scout_stats(
    org_id: str = Query("default_org"),
    session: Session = Depends(get_session)
):
    """Get Scout statistics for the widget"""
    
    user_id = get_current_user_id()
    
    # Count insights by state
    stats_query = session.query(
        ScoutInsightUserState.state,
        func.count(ScoutInsightUserState.id).label('count')
    ).filter(
        ScoutInsightUserState.org_id == org_id,
        ScoutInsightUserState.user_id == user_id
    ).group_by(ScoutInsightUserState.state)
    
    state_counts = {state.value: 0 for state in InsightUserState}
    for state, count in stats_query.all():
        state_counts[state.value] = count
    
    # Count total insights (including those without user state)
    total_insights = session.query(func.count(ScoutInsight.id)).filter(
        ScoutInsight.org_id == org_id
    ).scalar()
    
    # Count insights with no user state (defaults to "default")
    insights_with_state = session.query(func.count(ScoutInsightUserState.id)).filter(
        ScoutInsightUserState.org_id == org_id,
        ScoutInsightUserState.user_id == user_id
    ).scalar()
    
    state_counts['default'] += (total_insights - insights_with_state)
    
    return {
        "total_insights": total_insights,
        "state_counts": state_counts,
        "last_updated": datetime.now(timezone.utc)
    }
