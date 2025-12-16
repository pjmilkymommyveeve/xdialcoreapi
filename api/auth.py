from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from datetime import timedelta
from core.auth import hash_password, verify_password, create_access_token
from database.db import get_db
from core.settings import settings


router = APIRouter(prefix="/auth", tags=["Authentication"])


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str
    user_id: int
    username: str
    role: str


@router.post("/login", response_model=LoginResponse)
async def login(request: LoginRequest):
    """LOGIN - Authenticate user and return JWT token"""
    pool = await get_db()
    
    # fetch user with role information
    query = """
        SELECT u.id, u.username, u.password, u.is_active, r.name as role_name
        FROM users u
        JOIN roles r ON u.role_id = r.id
        WHERE u.username = $1
    """
    
    async with pool.acquire() as conn:
        user = await conn.fetchrow(query, request.username)
    
    # validate user exists
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password"
        )
    
    # check if user is active
    if not user['is_active']:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is disabled"
        )
    
    # verify password
    if not verify_password(request.password, user['password']):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password"
        )
    
    # create access token with user info
    token_data = {
        "sub": str(user['id']),
        "username": user['username'],
        "roles": [user['role_name']]
    }
    
    access_token = create_access_token(
        data=token_data,
        expires_delta=timedelta(minutes=settings.auth.access_token_expire_minutes)
    )
    
    return LoginResponse(
        access_token=access_token,
        token_type="bearer",
        user_id=user['id'],
        username=user['username'],
        role=user['role_name']
    )