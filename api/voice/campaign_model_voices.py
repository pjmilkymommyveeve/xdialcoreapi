from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
from typing import List, Optional
from database.db import get_db

router = APIRouter(prefix="/campaign-model-voices", tags=["Campaign Model Voices"])


# ============== PYDANTIC SCHEMAS ==============

class CampaignModelVoiceCreate(BaseModel):
    campaign_model_id: int = Field(..., gt=0)
    voice_id: int = Field(..., gt=0)
    active: bool = Field(default=True)


class CampaignModelVoiceUpdate(BaseModel):
    active: bool


class CampaignModelVoiceResponse(BaseModel):
    id: int
    campaign_model_id: int
    voice_id: int
    voice_name: str
    active: bool


class VoiceCategoryInRecording(BaseModel):
    id: int
    name: str


class VoiceRecordingInList(BaseModel):
    id: int
    name: str
    categories: List[VoiceCategoryInRecording]


class CampaignModelVoiceDetailedResponse(BaseModel):
    id: int
    campaign_model_id: int
    campaign_name: str
    model_name: str
    voice_id: int
    voice_name: str
    active: bool
    recordings: List[VoiceRecordingInList]


class BulkCampaignModelVoiceCreate(BaseModel):
    assignments: List[CampaignModelVoiceCreate]


class BulkCampaignModelVoiceCreateResponse(BaseModel):
    success: bool
    created_count: int
    failed_count: int
    assignments: List[CampaignModelVoiceResponse]
    errors: List[str] = []


class BulkCampaignModelVoiceUpdateRequest(BaseModel):
    cmv_id: int
    active: bool


class BulkCampaignModelVoiceUpdate(BaseModel):
    updates: List[BulkCampaignModelVoiceUpdateRequest]


class BulkCampaignModelVoiceUpdateResponse(BaseModel):
    success: bool
    updated_count: int
    failed_count: int
    assignments: List[CampaignModelVoiceResponse]
    errors: List[str] = []


class BulkCampaignModelVoiceDeleteResponse(BaseModel):
    success: bool
    deleted_count: int
    failed_count: int
    errors: List[str] = []


# ============== ENDPOINTS ==============

@router.get("/campaign-models/{campaign_model_id}/detailed", response_model=List[CampaignModelVoiceDetailedResponse])
async def get_campaign_model_voices_with_recordings(campaign_model_id: int):
    """Get all voices assigned to a campaign model with their recordings and categories."""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Check if campaign model exists and get campaign/model names
        cm_check = """
            SELECT cm.id, c.name as campaign_name, m.name as model_name
            FROM campaign_model cm
            JOIN campaigns c ON cm.campaign_id = c.id
            JOIN models m ON cm.model_id = m.id
            WHERE cm.id = $1
        """
        cm = await conn.fetchrow(cm_check, campaign_model_id)
        
        if not cm:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Campaign model with ID {campaign_model_id} not found"
            )
        
        # Fetch all campaign model voices
        cmv_query = """
            SELECT cmv.id, cmv.campaign_model_id, cmv.voice_id, cmv.active, v.name as voice_name
            FROM campaign_model_voice cmv
            JOIN voices v ON cmv.voice_id = v.id
            WHERE cmv.campaign_model_id = $1
            ORDER BY v.name
        """
        cmvs = await conn.fetch(cmv_query, campaign_model_id)
        
        result = []
        
        for cmv in cmvs:
            # Fetch recordings for this campaign model voice
            recordings_query = """
                SELECT id, name
                FROM voice_recordings
                WHERE campaign_model_voice_id = $1
                ORDER BY name
            """
            recordings = await conn.fetch(recordings_query, cmv['id'])
            
            recordings_with_categories = []
            
            for recording in recordings:
                # Fetch categories for this recording
                categories_query = """
                    SELECT vc.id, vc.name
                    FROM voice_categories vc
                    JOIN voice_recording_categories vrc ON vc.id = vrc.voice_category_id
                    WHERE vrc.voice_recording_id = $1
                    ORDER BY vc.name
                """
                categories = await conn.fetch(categories_query, recording['id'])
                
                recordings_with_categories.append({
                    'id': recording['id'],
                    'name': recording['name'],
                    'categories': [
                        {'id': cat['id'], 'name': cat['name']}
                        for cat in categories
                    ]
                })
            
            result.append({
                'id': cmv['id'],
                'campaign_model_id': cmv['campaign_model_id'],
                'campaign_name': cm['campaign_name'],
                'model_name': cm['model_name'],
                'voice_id': cmv['voice_id'],
                'voice_name': cmv['voice_name'],
                'active': cmv['active'],
                'recordings': recordings_with_categories
            })
        
        return result


@router.post("/bulk", response_model=BulkCampaignModelVoiceCreateResponse, status_code=status.HTTP_201_CREATED)
async def assign_voices_to_campaign_models_bulk(bulk_data: BulkCampaignModelVoiceCreate):
    """Assign multiple voices to campaign models at once."""
    pool = await get_db()
    
    created_assignments = []
    errors = []
    
    async with pool.acquire() as conn:
        for assignment in bulk_data.assignments:
            try:
                # Check if campaign model exists
                cm_check = "SELECT id FROM campaign_model WHERE id = $1"
                cm_exists = await conn.fetchrow(cm_check, assignment.campaign_model_id)
                
                if not cm_exists:
                    errors.append(f"Campaign model ID {assignment.campaign_model_id}: Not found")
                    continue
                
                # Check if voice exists
                voice_check = "SELECT id, name FROM voices WHERE id = $1"
                voice = await conn.fetchrow(voice_check, assignment.voice_id)
                
                if not voice:
                    errors.append(f"Voice ID {assignment.voice_id}: Not found")
                    continue
                
                # Check if assignment already exists
                existing_query = """
                    SELECT id FROM campaign_model_voice 
                    WHERE campaign_model_id = $1 AND voice_id = $2
                """
                existing = await conn.fetchrow(existing_query, assignment.campaign_model_id, assignment.voice_id)
                
                if existing:
                    errors.append(f"Voice '{voice['name']}' already assigned to campaign model {assignment.campaign_model_id}")
                    continue
                
                # Create assignment
                insert_query = """
                    INSERT INTO campaign_model_voice (campaign_model_id, voice_id, active)
                    VALUES ($1, $2, $3)
                    RETURNING id, campaign_model_id, voice_id, active
                """
                result = await conn.fetchrow(insert_query, assignment.campaign_model_id, assignment.voice_id, assignment.active)
                
                created_assignments.append({
                    'id': result['id'],
                    'campaign_model_id': result['campaign_model_id'],
                    'voice_id': result['voice_id'],
                    'voice_name': voice['name'],
                    'active': result['active']
                })
                
            except Exception as e:
                errors.append(f"CM {assignment.campaign_model_id} + Voice {assignment.voice_id}: {str(e)}")
    
    return BulkCampaignModelVoiceCreateResponse(
        success=len(errors) == 0,
        created_count=len(created_assignments),
        failed_count=len(errors),
        assignments=created_assignments,
        errors=errors
    )


@router.put("/bulk", response_model=BulkCampaignModelVoiceUpdateResponse)
async def update_campaign_model_voices_bulk(bulk_data: BulkCampaignModelVoiceUpdate):
    """Update multiple campaign model voice assignments at once."""
    pool = await get_db()
    
    updated_assignments = []
    errors = []
    
    async with pool.acquire() as conn:
        for update_item in bulk_data.updates:
            try:
                # Check if assignment exists
                check_query = """
                    SELECT cmv.id, cmv.campaign_model_id, cmv.voice_id, v.name as voice_name
                    FROM campaign_model_voice cmv
                    JOIN voices v ON cmv.voice_id = v.id
                    WHERE cmv.id = $1
                """
                existing = await conn.fetchrow(check_query, update_item.cmv_id)
                
                if not existing:
                    errors.append(f"Assignment ID {update_item.cmv_id}: Not found")
                    continue
                
                # Update assignment
                update_query = """
                    UPDATE campaign_model_voice SET active = $1 WHERE id = $2
                    RETURNING id, campaign_model_id, voice_id, active
                """
                result = await conn.fetchrow(update_query, update_item.active, update_item.cmv_id)
                
                updated_assignments.append({
                    'id': result['id'],
                    'campaign_model_id': result['campaign_model_id'],
                    'voice_id': result['voice_id'],
                    'voice_name': existing['voice_name'],
                    'active': result['active']
                })
                
            except Exception as e:
                errors.append(f"Assignment ID {update_item.cmv_id}: {str(e)}")
    
    return BulkCampaignModelVoiceUpdateResponse(
        success=len(errors) == 0,
        updated_count=len(updated_assignments),
        failed_count=len(errors),
        assignments=updated_assignments,
        errors=errors
    )


@router.post("/bulk-delete", response_model=BulkCampaignModelVoiceDeleteResponse)
async def remove_voices_from_campaign_models_bulk(cmv_ids: List[int]):
    """Remove multiple voice assignments from campaign models at once. Deletes all associated recordings."""
    pool = await get_db()
    
    deleted_count = 0
    errors = []
    
    async with pool.acquire() as conn:
        for cmv_id in cmv_ids:
            try:
                # Check if assignment exists
                check_query = "SELECT id FROM campaign_model_voice WHERE id = $1"
                existing = await conn.fetchrow(check_query, cmv_id)
                
                if not existing:
                    errors.append(f"Assignment ID {cmv_id}: Not found")
                    continue
                
                # Delete assignment (cascade deletes recordings)
                delete_query = "DELETE FROM campaign_model_voice WHERE id = $1"
                await conn.execute(delete_query, cmv_id)
                deleted_count += 1
                
            except Exception as e:
                errors.append(f"Assignment ID {cmv_id}: {str(e)}")
    
    return BulkCampaignModelVoiceDeleteResponse(
        success=len(errors) == 0,
        deleted_count=deleted_count,
        failed_count=len(errors),
        errors=errors
    )