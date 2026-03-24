"""
dark-store-api: Stateless raw content storage API.

FastAPI application entry point.
"""

import logging
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from .api.store import router as store_router, health_router
from .config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events."""
    settings = get_settings()
    logger = logging.getLogger(__name__)
    logger.info(f"Starting dark-store-api with backend: {settings.storage_backend}")
    yield
    logger.info("Shutting down dark-store-api")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )

    app = FastAPI(
        title="dark-store-api",
        description="Stateless raw content storage API for the dARK project",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # Include routers
    app.include_router(store_router)
    app.include_router(health_router)

    return app


# Application instance
app = create_app()


def run():
    """CLI entry point for running the API server."""
    settings = get_settings()
    uvicorn.run(
        "app.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )


if __name__ == "__main__":
    run()
