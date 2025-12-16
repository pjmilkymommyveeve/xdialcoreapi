# core/dependencies.py
from fastapi import Depends, HTTPException, Header, status
from typing import List
from jose import JWTError
from core.security import bearer_auth
from core.auth import decode_token
from core.settings import settings

async def get_current_user_id(token: str = Depends(bearer_auth)) -> int:
    """
    Extracts the user_id from the JWT and returns it.
    """
    try:
        payload = decode_token(token)
        user_id = payload.get("sub")

        if user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token missing user ID",
            )

        return int(user_id)

    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )


def require_roles(allowed_roles: List[str]):
    """
    Returns a dependency that checks if the user has at least one of the allowed roles.
    Usage:
        @router.get("/admin")
        async def admin_route(user = Depends(require_roles(["admin", "superadmin"]))):
            return {"ok": True}
    """

    async def role_checker(token: str = Depends(bearer_auth)):
        try:
            payload = decode_token(token)
            user_id = payload.get("sub")
            roles = payload.get("roles", [])

            if user_id is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Token missing user ID",
                )

            # roles in token must contain at least one allowed role
            if not any(r in roles for r in allowed_roles):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="You do not have permission for this action",
                )

            return {
                "user_id": int(user_id),
                "roles": roles
            }

        except JWTError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
            )

    return role_checker
