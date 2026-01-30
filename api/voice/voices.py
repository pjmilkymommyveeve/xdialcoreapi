from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
from typing import List
from database.db import get_db

router = APIRouter(prefix="/voices", tags=["Voices"])


# ============== PYDANTIC SCHEMAS ==============

class VoiceCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)


class VoiceUpdate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)


class VoiceResponse(BaseModel):
    id: int
    name: str


class BulkVoiceCreate(BaseModel):
    voices: List[VoiceCreate]


class BulkVoiceCreateResponse(BaseModel):
    success: bool
    created_count: int
    failed_count: int
    voices: List[VoiceResponse]
    errors: List[str] = []


class BulkVoiceDeleteResponse(BaseModel):
    success: bool
    deleted_count: int
    failed_count: int
    errors: List[str] = []


# ============== VOICE CRUD ENDPOINTS ==============

@router.get("/", response_model=List[VoiceResponse])
async def get_all_voices():
    """Get all available voices."""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        query = "SELECT id, name FROM voices ORDER BY name"
        voices = await conn.fetch(query)
        
        return [{'id': v['id'], 'name': v['name']} for v in voices]


@router.get("/{voice_id}", response_model=VoiceResponse)
async def get_voice(voice_id: int):
    """Get a specific voice by ID."""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        query = "SELECT id, name FROM voices WHERE id = $1"
        voice = await conn.fetchrow(query, voice_id)
        
        if not voice:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Voice with ID {voice_id} not found"
            )
        
        return {'id': voice['id'], 'name': voice['name']}


@router.post("/bulk", response_model=BulkVoiceCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_voices_bulk(bulk_data: BulkVoiceCreate):
    """Create multiple voices at once."""
    pool = await get_db()
    
    created_voices = []
    errors = []
    
    async with pool.acquire() as conn:
        for voice_data in bulk_data.voices:
            try:
                # Check if voice name already exists
                check_query = "SELECT id FROM voices WHERE name = $1"
                existing = await conn.fetchrow(check_query, voice_data.name)
                
                if existing:
                    errors.append(f"Voice '{voice_data.name}' already exists")
                    continue
                
                # Insert new voice
                insert_query = "INSERT INTO voices (name) VALUES ($1) RETURNING id, name"
                voice = await conn.fetchrow(insert_query, voice_data.name)
                
                created_voices.append({'id': voice['id'], 'name': voice['name']})
                
            except Exception as e:
                errors.append(f"Voice '{voice_data.name}': {str(e)}")
    
    return BulkVoiceCreateResponse(
        success=len(errors) == 0,
        created_count=len(created_voices),
        failed_count=len(errors),
        voices=created_voices,
        errors=errors
    )


@router.post("/bulk-delete", response_model=BulkVoiceDeleteResponse)
async def delete_voices_bulk(voice_ids: List[int]):
    """Delete multiple voices at once. Skips voices that are assigned to campaign models."""
    pool = await get_db()
    
    deleted_count = 0
    errors = []
    
    async with pool.acquire() as conn:
        for voice_id in voice_ids:
            try:
                # Check if voice exists
                check_query = "SELECT id, name FROM voices WHERE id = $1"
                existing = await conn.fetchrow(check_query, voice_id)
                
                if not existing:
                    errors.append(f"Voice ID {voice_id}: Not found")
                    continue
                
                # Check if voice is assigned
                usage_query = "SELECT COUNT(*) as count FROM campaign_model_voice WHERE voice_id = $1"
                usage = await conn.fetchrow(usage_query, voice_id)
                
                if usage['count'] > 0:
                    errors.append(f"Voice '{existing['name']}': assigned to {usage['count']} campaign model(s)")
                    continue
                
                # Delete voice
                delete_query = "DELETE FROM voices WHERE id = $1"
                await conn.execute(delete_query, voice_id)
                deleted_count += 1
                
            except Exception as e:
                errors.append(f"Voice ID {voice_id}: {str(e)}")
    
    return BulkVoiceDeleteResponse(
        success=len(errors) == 0,
        deleted_count=deleted_count,
        failed_count=len(errors),
        errors=errors
    )