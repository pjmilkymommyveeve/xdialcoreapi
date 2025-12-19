from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from core.dependencies import get_current_user_id
from database.db import get_db


router = APIRouter(prefix="/client/campaigns", tags=["Client Campaigns"])


# ============== MODELS ==============

class TransferSettingsInfo(BaseModel):
    id: int
    name: str
    description: Optional[str]
    is_recommended: bool
    quality_score: int
    volume_score: int


class ModelInfo(BaseModel):
    id: int
    name: str
    description: Optional[str]
    transfer_settings: List[TransferSettingsInfo]


class CampaignInfo(BaseModel):
    id: int
    name: str
    description: Optional[str]


class StatusInfo(BaseModel):
    id: int
    status_name: str
    start_date: datetime
    end_date: Optional[datetime]


class CampaignCallStats(BaseModel):
    total_calls: int
    calls_transferred: int
    transfer_percentage: int


class ClientCampaignResponse(BaseModel):
    id: int
    campaign: CampaignInfo
    model: ModelInfo
    start_date: datetime
    end_date: Optional[datetime]
    is_active: bool
    bot_count: int
    status: Optional[StatusInfo]
    call_stats: CampaignCallStats


class ClientCampaignsListResponse(BaseModel):
    client_id: int
    client_name: str
    campaigns: List[ClientCampaignResponse]
    total_campaigns: int
    active_campaigns: int
    inactive_campaigns: int


# ============== ENDPOINT ==============

@router.get("/{client_id}", response_model=ClientCampaignsListResponse)
async def get_client_campaigns(
    client_id: int,
    user_id: int = Depends(get_current_user_id)
):
    """
    GET CLIENT CAMPAIGNS - Returns all campaigns for the specified client.
    - Regular clients can only access their own enabled campaigns
    - Admin, onboarding, and QA roles can access any client's campaigns (excluding archived)
    """
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Get user's role
        user_role_query = """
            SELECT r.name
            FROM users u
            JOIN roles r ON u.role_id = r.id
            WHERE u.id = $1
        """
        user_data = await conn.fetchrow(user_role_query, user_id)
        
        if not user_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        user_role = user_data['name']
        is_privileged = user_role in ['admin', 'onboarding', 'qa']
        
        # Check access: if not privileged role, must be requesting own campaigns
        if not is_privileged and client_id != user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied. You can only view your own campaigns."
            )
        
        # Get client information
        client_query = """
            SELECT c.client_id, c.name
            FROM clients c
            WHERE c.client_id = $1
        """
        
        client_data = await conn.fetchrow(client_query, client_id)
        
        if not client_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Client not found"
            )
        
        client_name = client_data['name']
        
        # Build query based on role - matching Django view logic
        if is_privileged:
            # Admin/onboarding/QA see all campaigns except 'Archived'
            campaigns_query = """
                SELECT 
                    ccm.id as campaign_id,
                    ccm.start_date,
                    ccm.end_date,
                    ccm.is_active,
                    ccm.bot_count,
                    ca.id as camp_id,
                    ca.name as camp_name,
                    ca.description as camp_desc,
                    m.id as model_id,
                    m.name as model_name,
                    m.description as model_desc,
                    sh.id as status_history_id,
                    sh.start_date as status_start,
                    sh.end_date as status_end,
                    s.id as status_id,
                    s.status_name
                FROM client_campaign_model ccm
                JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
                JOIN campaigns ca ON cm.campaign_id = ca.id
                JOIN models m ON cm.model_id = m.id
                LEFT JOIN status_history sh ON ccm.id = sh.client_campaign_id 
                    AND sh.end_date IS NULL
                LEFT JOIN status s ON sh.status_id = s.id
                WHERE ccm.client_id = $1 
                    AND (s.status_name != 'Archived' OR s.status_name IS NULL)
                ORDER BY ccm.start_date DESC
            """
        else:
            # Regular clients only see 'Enabled' campaigns
            campaigns_query = """
                SELECT 
                    ccm.id as campaign_id,
                    ccm.start_date,
                    ccm.end_date,
                    ccm.is_active,
                    ccm.bot_count,
                    ca.id as camp_id,
                    ca.name as camp_name,
                    ca.description as camp_desc,
                    m.id as model_id,
                    m.name as model_name,
                    m.description as model_desc,
                    sh.id as status_history_id,
                    sh.start_date as status_start,
                    sh.end_date as status_end,
                    s.id as status_id,
                    s.status_name
                FROM client_campaign_model ccm
                JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
                JOIN campaigns ca ON cm.campaign_id = ca.id
                JOIN models m ON cm.model_id = m.id
                LEFT JOIN status_history sh ON ccm.id = sh.client_campaign_id 
                    AND sh.end_date IS NULL
                LEFT JOIN status s ON sh.status_id = s.id
                WHERE ccm.client_id = $1 AND s.status_name = 'Enabled'
                ORDER BY ccm.start_date DESC
            """
        
        campaigns_data = await conn.fetch(campaigns_query, client_id)
        
        if not campaigns_data:
            return ClientCampaignsListResponse(
                client_id=client_id,
                client_name=client_name,
                campaigns=[],
                total_campaigns=0,
                active_campaigns=0,
                inactive_campaigns=0
            )
        
        # Get all model IDs and campaign IDs for batch queries
        model_ids = list(set([c['model_id'] for c in campaigns_data]))
        campaign_ids = [c['campaign_id'] for c in campaigns_data]
        
        # Fetch transfer settings for all models
        transfer_settings_query = """
            SELECT 
                m.id as model_id,
                ts.id as ts_id,
                ts.name as ts_name,
                ts.description as ts_desc,
                ts.is_recommended,
                ts.quality_score,
                ts.volume_score
            FROM models m
            JOIN models_transfer_settings mts ON m.id = mts.model_id
            JOIN transfer_settings ts ON mts.transfersettings_id = ts.id
            WHERE m.id = ANY($1)
            ORDER BY ts.display_order, ts.name
        """
        transfer_settings_data = await conn.fetch(transfer_settings_query, model_ids)
        
        # Fetch call statistics for all campaigns
        call_stats_query = """
            SELECT 
                client_campaign_model_id,
                COUNT(*) as total_calls,
                SUM(CASE WHEN transferred = true THEN 1 ELSE 0 END) as calls_transferred
            FROM calls
            WHERE client_campaign_model_id = ANY($1)
            GROUP BY client_campaign_model_id
        """
        call_stats_data = await conn.fetch(call_stats_query, campaign_ids)
        
        # Group transfer settings by model_id
        transfer_settings_by_model = {}
        for ts in transfer_settings_data:
            model_id = ts['model_id']
            if model_id not in transfer_settings_by_model:
                transfer_settings_by_model[model_id] = []
            
            transfer_settings_by_model[model_id].append(TransferSettingsInfo(
                id=ts['ts_id'],
                name=ts['ts_name'],
                description=ts['ts_desc'],
                is_recommended=ts['is_recommended'],
                quality_score=ts['quality_score'],
                volume_score=ts['volume_score']
            ))
        
        # Group call stats by campaign_id
        call_stats_by_campaign = {}
        for stats in call_stats_data:
            campaign_id = stats['client_campaign_model_id']
            total_calls = stats['total_calls'] or 0
            calls_transferred = stats['calls_transferred'] or 0
            transfer_percentage = 0
            
            if total_calls > 0:
                transfer_percentage = round((calls_transferred / total_calls) * 100)
            
            call_stats_by_campaign[campaign_id] = CampaignCallStats(
                total_calls=total_calls,
                calls_transferred=calls_transferred,
                transfer_percentage=transfer_percentage
            )
        
        # Build response
        campaigns_list = []
        active_count = 0
        
        for camp in campaigns_data:
            model_transfer_settings = transfer_settings_by_model.get(camp['model_id'], [])
            
            # Get call stats or use defaults
            call_stats = call_stats_by_campaign.get(
                camp['campaign_id'],
                CampaignCallStats(
                    total_calls=0,
                    calls_transferred=0,
                    transfer_percentage=0
                )
            )
            
            # Count active campaigns
            if camp['is_active']:
                active_count += 1
            
            status_info = None
            if camp['status_history_id']:
                status_info = StatusInfo(
                    id=camp['status_id'],
                    status_name=camp['status_name'],
                    start_date=camp['status_start'],
                    end_date=camp['status_end']
                )
            
            campaigns_list.append(ClientCampaignResponse(
                id=camp['campaign_id'],
                campaign=CampaignInfo(
                    id=camp['camp_id'],
                    name=camp['camp_name'],
                    description=camp['camp_desc']
                ),
                model=ModelInfo(
                    id=camp['model_id'],
                    name=camp['model_name'],
                    description=camp['model_desc'],
                    transfer_settings=model_transfer_settings
                ),
                start_date=camp['start_date'],
                end_date=camp['end_date'],
                is_active=camp['is_active'],
                bot_count=camp['bot_count'],
                status=status_info,
                call_stats=call_stats
            ))
        
        total_campaigns = len(campaigns_list)
        inactive_count = total_campaigns - active_count
        
        return ClientCampaignsListResponse(
            client_id=client_id,
            client_name=client_name,
            campaigns=campaigns_list,
            total_campaigns=total_campaigns,
            active_campaigns=active_count,
            inactive_campaigns=inactive_count
        )