from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from typing import List, Optional
from database.db import get_db

router = APIRouter(prefix="/campaign-models", tags=["Campaign Models"])


# ============== PYDANTIC SCHEMAS ==============

class VoiceSchema(BaseModel):
    id: int
    name: str
    
    class Config:
        from_attributes = True


class CampaignModelVoiceSchema(BaseModel):
    id: int
    voice: VoiceSchema
    active: bool
    
    class Config:
        from_attributes = True


class CampaignSchema(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    
    class Config:
        from_attributes = True


class ModelSchema(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    
    class Config:
        from_attributes = True


class CampaignModelResponse(BaseModel):
    id: int
    campaign: CampaignSchema
    model: ModelSchema
    voices: List[CampaignModelVoiceSchema] = []
    
    class Config:
        from_attributes = True


# ============== ENDPOINTS ==============

@router.get("/", response_model=List[CampaignModelResponse])
async def get_all_campaign_models():
    """
    Get all campaign models with their associated campaigns, models, and voices.
    """
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Fetch all campaign models with their relationships
        query = """
            SELECT 
                cm.id as cm_id,
                c.id as campaign_id, c.name as campaign_name, c.description as campaign_desc,
                m.id as model_id, m.name as model_name, m.description as model_desc
            FROM campaign_model cm
            JOIN campaigns c ON cm.campaign_id = c.id
            JOIN models m ON cm.model_id = m.id
            ORDER BY c.name, m.name
        """
        campaign_models = await conn.fetch(query)
        
        if not campaign_models:
            return []
        
        # Fetch all voices for these campaign models
        cm_ids = [cm['cm_id'] for cm in campaign_models]
        voices_query = """
            SELECT 
                cmv.id as cmv_id,
                cmv.campaign_model_id,
                cmv.active,
                v.id as voice_id,
                v.name as voice_name
            FROM campaign_model_voice cmv
            JOIN voices v ON cmv.voice_id = v.id
            WHERE cmv.campaign_model_id = ANY($1)
            ORDER BY v.name
        """
        voices_data = await conn.fetch(voices_query, cm_ids)
        
        # Group voices by campaign_model_id
        voices_by_cm = {}
        for voice in voices_data:
            cm_id = voice['campaign_model_id']
            if cm_id not in voices_by_cm:
                voices_by_cm[cm_id] = []
            voices_by_cm[cm_id].append({
                'id': voice['cmv_id'],
                'voice': {
                    'id': voice['voice_id'],
                    'name': voice['voice_name']
                },
                'active': voice['active']
            })
        
        # Build response
        result = []
        for cm in campaign_models:
            cm_id = cm['cm_id']
            result.append({
                'id': cm_id,
                'campaign': {
                    'id': cm['campaign_id'],
                    'name': cm['campaign_name'],
                    'description': cm['campaign_desc']
                },
                'model': {
                    'id': cm['model_id'],
                    'name': cm['model_name'],
                    'description': cm['model_desc']
                },
                'voices': voices_by_cm.get(cm_id, [])
            })
        
        return result


