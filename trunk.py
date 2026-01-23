from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from core.settings import settings
from database.db import init_db_pool, close_db_pool

# import routers
from api.stats import campaign_stats, server_stats, voice_stats, c
from api import auth, campaign_metrics, export, integration, recordings, client, client_employees, call_lookup, response_categories

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown"""
    # startup
    await init_db_pool()
    yield
    # shutdown
    await close_db_pool()


# create fastapi app
app = FastAPI(
    title=settings.app.name,
    description="Campaign Management API for AI calling campaigns",
    version="1.0.0",
    lifespan=lifespan
)

# configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.app.origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# include routers
app.include_router(auth.router, prefix=settings.app.api_prefix)
app.include_router(client.router, prefix=settings.app.api_prefix)
app.include_router(client_employees.router, prefix=settings.app.api_prefix)
app.include_router(campaign_metrics.router, prefix=settings.app.api_prefix)
app.include_router(recordings.router, prefix=settings.app.api_prefix)
app.include_router(export.router, prefix=settings.app.api_prefix)
app.include_router(integration.router, prefix=settings.app.api_prefix)
app.include_router(response_categories.router, prefix=settings.app.api_prefix)
app.include_router(call_lookup.router, prefix=settings.app.api_prefix)
app.include_router(voice_stats.router, prefix=settings.app.api_prefix)
app.include_router(server_stats.router, prefix=settings.app.api_prefix)
app.include_router(campaign_stats.router, prefix=settings.app.api_prefix)


@app.get("/")
async def root():
    """ROOT - Health check endpoint"""
    return {
        "message": "Campaign Management API",
        "status": "healthy",
        "version": "1.0.0"
    }


@app.get("/health")
async def health_check():
    """HEALTH CHECK - Verify API is running"""
    return {
        "status": "ok",
        "timestamp": "2025-01-01T00:00:00Z"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "trunk:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.app.debug
    )