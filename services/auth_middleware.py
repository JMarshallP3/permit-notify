"""
Authentication middleware and RBAC enforcement for FastAPI.
"""

from typing import Optional, Dict, Any, List
from fastapi import HTTPException, status, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from services.auth import auth_service
from db.session import get_session
from db.auth_models import User, OrgMembership


class AuthMiddleware:
    """Authentication middleware that injects user context into requests."""
    
    def __init__(self):
        self.security = HTTPBearer(auto_error=False)
    
    async def get_current_user(self, request: Request) -> Optional[User]:
        """Get current authenticated user from request."""
        # Try to get JWT from cookies first
        access_token = request.cookies.get("access_token")
        if not access_token:
            return None
        
        try:
            # Verify the JWT token
            payload = auth_service.verify_token(access_token)
            user_id = payload.get("sub")
            if not user_id:
                return None
            
            # Get user from database
            return auth_service.get_user_by_id(user_id)
        except Exception as e:
            # Token is invalid or expired
            return None
    
    async def require_auth(self, request: Request) -> User:
        """Require authentication - raise 401 if not authenticated."""
        user = await self.get_current_user(request)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required"
            )
        return user
    
    async def get_user_org_context(self, request: Request, user: User) -> Dict[str, Any]:
        """Get user's organization context for the request."""
        # Get org_id from query params or headers (for backward compatibility)
        org_id = request.query_params.get('org_id') or request.headers.get('X-Org-ID')
        
        if not org_id:
            # Default to first org the user belongs to
            user_orgs = auth_service.get_user_orgs(user.id)
            if user_orgs:
                org_id = user_orgs[0]["org_id"]
            else:
                org_id = "default_org"
        
        # Verify user has access to this org
        with get_session() as session:
            membership = session.query(OrgMembership).filter(
                OrgMembership.user_id == user.id,
                OrgMembership.org_id == org_id
            ).first()
            
            if not membership:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Access denied to organization {org_id}"
                )
            
            return {
                "org_id": org_id,
                "role": membership.role,
                "user_id": user.id,
                "user_email": user.email,
                "user_username": user.username
            }


class RBACMiddleware:
    """Role-Based Access Control middleware."""
    
    def __init__(self, auth_middleware: AuthMiddleware):
        self.auth = auth_middleware
    
    def require_role(self, required_roles: List[str]):
        """Decorator to require specific roles."""
        def role_checker(request: Request, org_context: Dict[str, Any] = Depends(self.get_org_context)):
            user_role = org_context.get("role")
            if user_role not in required_roles:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Required roles: {required_roles}, user role: {user_role}"
                )
            return org_context
        return role_checker
    
    def require_admin(self):
        """Require admin or owner role."""
        return self.require_role(["admin", "owner"])
    
    def require_owner(self):
        """Require owner role only."""
        return self.require_role(["owner"])
    
    async def get_org_context(self, request: Request) -> Dict[str, Any]:
        """Get authenticated user's organization context."""
        user = await self.auth.require_auth(request)
        return await self.auth.get_user_org_context(request, user)


# Global middleware instances
auth_middleware = AuthMiddleware()
rbac_middleware = RBACMiddleware(auth_middleware)

# Convenience dependencies
async def get_current_user(request: Request) -> Optional[User]:
    """Get current authenticated user (optional)."""
    return await auth_middleware.get_current_user(request)

async def require_auth(request: Request) -> User:
    """Require authentication."""
    return await auth_middleware.require_auth(request)

async def get_org_context(request: Request) -> Dict[str, Any]:
    """Get authenticated user's organization context."""
    return await rbac_middleware.get_org_context(request)

# Role-based dependencies
def require_admin():
    """Require admin or owner role."""
    return rbac_middleware.require_admin()

def require_owner():
    """Require owner role only."""
    return rbac_middleware.require_owner()

def require_role(roles: List[str]):
    """Require specific roles."""
    return rbac_middleware.require_role(roles)


class AuthContext:
    """Context object for authenticated requests."""
    
    def __init__(self, user: User, org_context: Dict[str, Any]):
        self.user = user
        self.user_id = user.id
        self.user_email = user.email
        self.user_username = user.username
        self.org_id = org_context["org_id"]
        self.role = org_context["role"]
    
    @property
    def is_admin(self) -> bool:
        """Check if user has admin privileges."""
        return self.role in ["admin", "owner"]
    
    @property
    def is_owner(self) -> bool:
        """Check if user is owner."""
        return self.role == "owner"
    
    def has_role(self, role: str) -> bool:
        """Check if user has specific role."""
        return self.role == role
    
    def has_any_role(self, roles: List[str]) -> bool:
        """Check if user has any of the specified roles."""
        return self.role in roles


async def get_auth_context(request: Request) -> AuthContext:
    """Get full authentication context."""
    user = await require_auth(request)
    org_context = await get_org_context(request)
    return AuthContext(user, org_context)


# FastAPI Dependencies
async def get_current_user(request: Request) -> Optional[User]:
    """FastAPI dependency to get current authenticated user."""
    middleware = AuthMiddleware()
    return await middleware.get_current_user(request)


async def require_authenticated_user(request: Request) -> User:
    """FastAPI dependency that requires authentication."""
    middleware = AuthMiddleware()
    return await middleware.require_auth(request)


async def get_user_auth_context(request: Request) -> AuthContext:
    """FastAPI dependency to get full auth context."""
    return await get_auth_context(request)
