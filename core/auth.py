# core/auth.py
from core.django_settings import *  
from datetime import datetime, timedelta, timezone
from django.contrib.auth.hashers import make_password, check_password
from jose import jwt, JWTError
from core.settings import settings

# PASSWORD HASHING USING DJANGO

def hash_password(password: str) -> str:
    """
    Hash a plaintext password using Django's default hasher (PBKDF2 + SHA256 by default).
    This ensures compatibility with Django-stored passwords.
    """
    return make_password(password)


def verify_password(password: str, hashed: str) -> bool:
    """
    Verify a plaintext password against a Django hashed password.
    Works with all Django-supported hashers (PBKDF2, Argon2, bcrypt, etc.).
    """
    return check_password(password, hashed)


# JWT CREATION

def create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    """
    Creates a signed JWT token.
    Adds exp and iat fields automatically.
    """
    now = datetime.now(timezone.utc)
    expire = now + (expires_delta or timedelta(
        minutes=settings.auth.access_token_expire_minutes
    ))

    payload = data.copy()
    payload.update({
        "exp": expire,
        "iat": now
    })

    return jwt.encode(
        payload,
        settings.auth.secret_key,
        algorithm=settings.auth.algorithm
    )


# JWT DECODING

def decode_token(token: str) -> dict | None:
    """
    Decodes a JWT token and returns the payload.
    Returns None if invalid or expired.
    """
    try:
        payload = jwt.decode(
            token,
            settings.auth.secret_key,
            algorithms=[settings.auth.algorithm]
        )
        return payload
    except JWTError:
        return None
