import os
from dotenv import load_dotenv

load_dotenv()

class DatabaseConfig:
    """Database configuration"""
    def __init__(self):
        self.url = os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:password@localhost:5432/campaign_db"
        )


class AuthConfig:
    """Authentication configuration"""
    def __init__(self):
        self.secret_key = os.getenv(
            "JWT_SECRET_KEY",
            "your-secret-key-change-in-production-min-32-chars-long"
        )
        self.algorithm = os.getenv("JWT_ALGORITHM", "HS256")
        self.access_token_expire_minutes = int(
            os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 1440)
        )


class AppConfig:
    """Application configuration"""
    def __init__(self):
        self.name = os.getenv("APP_NAME", "Campaign Management API")
        self.debug = os.getenv("DEBUG", "False").lower() in ("true", "1", "yes")
        self.api_prefix = os.getenv("API_PREFIX", "/api/v1")
        self.allowed_origins = os.getenv(
            "ALLOWED_ORIGINS",
            "http://localhost:3000,http://localhost:8000"
        )

    @property
    def origins_list(self):
        return [origin.strip() for origin in self.allowed_origins.split(",")]


class Settings:
    """Main settings class"""
    def __init__(self):
        self.db = DatabaseConfig()
        self.auth = AuthConfig()
        self.app = AppConfig()


# global settings instance
settings = Settings()
