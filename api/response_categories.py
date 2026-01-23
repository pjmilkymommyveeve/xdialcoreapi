from fastapi import APIRouter
from pydantic import BaseModel
from typing import List
from database.db import get_db

router = APIRouter(prefix="/response-categories", tags=["Response Categories"])


# ============== MODELS ==============

class ResponseCategoryInfo(BaseModel):
    id: int
    name: str
    color: str


class ResponseCategoriesResponse(BaseModel):
    categories: List[ResponseCategoryInfo]
    total_count: int


# ============== ENDPOINT ==============

@router.get("/", response_model=ResponseCategoriesResponse)
async def get_response_categories():
    """
    Get all response categories from the database.
    Returns category ID, name, and color.
    """
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Fetch all response categories ordered by name
        query = """
            SELECT id, name, color
            FROM response_categories
            ORDER BY name
        """
        categories = await conn.fetch(query)
        
        # Format response
        categories_list = [
            ResponseCategoryInfo(
                id=cat['id'],
                name=cat['name'],
                color=cat['color'] or '#6B7280'  # Default color if null
            )
            for cat in categories
        ]
        
        return ResponseCategoriesResponse(
            categories=categories_list,
            total_count=len(categories_list)
        )