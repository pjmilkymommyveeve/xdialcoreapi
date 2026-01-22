from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from core.dependencies import require_roles
from database.db import get_db


router = APIRouter(prefix="/client/campaigns", tags=["Client"])


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
    transfer_setting: Optional[TransferSettingsInfo]
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

class CampaignBasicInfo(BaseModel):
    id: int
    campaign_name: str
    model_name: str
    is_active: bool


class ClientWithCampaignsResponse(BaseModel):
    client_id: int
    client_name: str
    campaigns: List[CampaignBasicInfo]
    total_campaigns: int


class AllClientsResponse(BaseModel):
    total_clients: int
    clients: List[ClientWithCampaignsResponse]

class EmployerInfoResponse(BaseModel):
    client_id: int
    client_name: str
    user_id: int
    username: str

class UserInfo(BaseModel):
    user_id: int
    roles: List[str]



# ============== HELPER FUNCTIONS ==============


async def get_user_client_id(conn, user_id: int, roles: List[str]) -> Optional[int]:
    """
    Get the client_id that the user has access to.
    For 'client' role: returns user_id as client_id
    For 'client_member' role: returns employer's client_id
    For privileged roles: returns None (can access any)
    """
    PRIVILEGED_ROLES = ['admin', 'onboarding', 'qa']
    
    if any(role in PRIVILEGED_ROLES for role in roles):
        return None
    elif 'client' in roles:
        return user_id
    elif 'client_member' in roles:
        employer_query = "SELECT client_id FROM client_employees WHERE user_id = $1"
        employer = await conn.fetchrow(employer_query, user_id)
        if employer:
            return employer['client_id']
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No employer association found"
        )
    return None


# ============== ENDPOINTS ==============

@router.get("/all", response_model=List[Dict[str, Any]])
async def get_all_clients(
    user_info: Dict = Depends(require_roles(["admin", "onboarding", "qa"]))
):
    """
    Get all clients with their basic information (id and name only).
    Only accessible by admin, onboarding, and qa roles.
    """
    pool = await get_db()
    
    async with pool.acquire() as conn:
        clients_query = """
            SELECT client_id, name
            FROM clients
            ORDER BY name
        """
        clients = await conn.fetch(clients_query)
        
        return [
            {"client_id": client['client_id'], "name": client['name']}
            for client in clients
        ]   

@router.get("/employer", response_model=EmployerInfoResponse)
async def get_client_member_employer(
    user_info: Dict[str, Any] = Depends(require_roles(['client_member']))
):
    """
    Get employer client information for the authenticated employee user.
    Only accessible by client_member role.
    """
    user_id = user_info['user_id']
    
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Get employer information
        employer_query = """
            SELECT 
                ce.client_id,
                c.name as client_name,
                u.username
            FROM client_employees ce
            JOIN clients c ON ce.client_id = c.client_id
            JOIN users u ON ce.user_id = u.id
            WHERE ce.user_id = $1
        """
        employer_data = await conn.fetchrow(employer_query, user_id)
        
        if not employer_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No employer association found"
            )
        
        return EmployerInfoResponse(
            client_id=employer_data['client_id'],
            client_name=employer_data['client_name'],
            user_id=user_id,
            username=employer_data['username']
        )

@router.get("/with-campaigns", response_model=AllClientsResponse)
async def get_all_clients_with_campaigns(
    user_info: Dict = Depends(require_roles(["admin", "onboarding", "qa"]))
):
    """
    ADMIN/ONBOARDING/QA: GET ALL CLIENTS WITH THEIR CAMPAIGNS
    
    Returns all clients with their basic information and associated campaigns.
    Only includes campaigns that are not Archived.
    """
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Get all clients
        clients_query = """
            SELECT client_id, name
            FROM clients
            ORDER BY name
        """
        clients = await conn.fetch(clients_query)
        
        if not clients:
            return AllClientsResponse(
                total_clients=0,
                clients=[]
            )
        
        # Get all campaigns for all clients (excluding archived)
        campaigns_query = """
            SELECT 
                ccm.id,
                ccm.client_id,
                ca.name as campaign_name,
                m.name as model_name
            FROM client_campaign_model ccm
            JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
            JOIN campaigns ca ON cm.campaign_id = ca.id
            JOIN models m ON cm.model_id = m.id
            WHERE EXISTS (
                SELECT 1 FROM status_history sh
                JOIN status s ON sh.status_id = s.id
                WHERE sh.client_campaign_id = ccm.id
                AND sh.end_date IS NULL
                AND s.status_name != 'Archived'
            ) OR NOT EXISTS (
                SELECT 1 FROM status_history sh
                WHERE sh.client_campaign_id = ccm.id
            )
            ORDER BY ca.name, m.name
        """
        campaigns = await conn.fetch(campaigns_query)
        
        # Calculate is_active for all campaigns
        campaign_ids = [c['id'] for c in campaigns]
        one_minute_ago = datetime.now() - timedelta(minutes=1)
        
        active_campaigns_query = """
            SELECT DISTINCT client_campaign_model_id
            FROM calls
            WHERE client_campaign_model_id = ANY($1)
            AND timestamp >= $2
        """
        active_campaign_ids = await conn.fetch(active_campaigns_query, campaign_ids, one_minute_ago)
        active_set = {row['client_campaign_model_id'] for row in active_campaign_ids}
        
        # Group campaigns by client
        campaigns_by_client = {}
        for campaign in campaigns:
            client_id = campaign['client_id']
            if client_id not in campaigns_by_client:
                campaigns_by_client[client_id] = []
            
            campaigns_by_client[client_id].append(CampaignBasicInfo(
                id=campaign['id'],
                campaign_name=campaign['campaign_name'],
                model_name=campaign['model_name'],
                is_active=campaign['id'] in active_set
            ))
        
        # Build response
        clients_list = []
        for client in clients:
            client_id = client['client_id']
            client_campaigns = campaigns_by_client.get(client_id, [])
            
            clients_list.append(ClientWithCampaignsResponse(
                client_id=client_id,
                client_name=client['name'],
                campaigns=client_campaigns,
                total_campaigns=len(client_campaigns)
            ))
        
        return AllClientsResponse(
            total_clients=len(clients_list),
            clients=clients_list
        )

@router.get("/{client_id}", response_model=ClientCampaignsListResponse)
async def get_client_campaigns(
    client_id: int,
    user_info: Dict = Depends(require_roles(['admin', 'onboarding', 'qa', 'client', 'client_member']))
):
    """
    Get all campaigns for the specified client.
    Privileged roles: Can access any client's campaigns (excluding archived)
    Client role: Can only access their own enabled campaigns
    Client member role: Can only access their employer's enabled campaigns
    """
    PRIVILEGED_ROLES = ['admin', 'onboarding', 'qa']
    
    user_id = user_info['user_id']
    roles = user_info['roles']
    
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Check access
        allowed_client_id = await get_user_client_id(conn, user_id, roles)
        
        # If allowed_client_id is not None and doesn't match, deny access
        if allowed_client_id is not None and client_id != allowed_client_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
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
        
        # Build campaign query based on role
        is_privileged = any(role in PRIVILEGED_ROLES for role in roles)
        
        if is_privileged:
            # Exclude archived campaigns
            status_filter = "AND (s.status_name != 'Archived' OR s.status_name IS NULL)"
        else:
            # Only show enabled campaigns
            status_filter = "AND s.status_name IN ('Enabled', 'Testing')"
        
        campaigns_query = f"""
            SELECT 
                ccm.id as campaign_id,
                ccm.start_date,
                ccm.end_date,
                ccm.bot_count,
                ca.id as camp_id,
                ca.name as camp_name,
                ca.description as camp_desc,
                m.id as model_id,
                m.name as model_name,
                m.description as model_desc,
                ts.id as ts_id,
                ts.name as ts_name,
                ts.description as ts_desc,
                ts.is_recommended,
                ts.quality_score,
                ts.volume_score,
                sh.id as status_history_id,
                sh.start_date as status_start,
                sh.end_date as status_end,
                s.id as status_id,
                s.status_name
            FROM client_campaign_model ccm
            JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
            JOIN campaigns ca ON cm.campaign_id = ca.id
            JOIN models m ON cm.model_id = m.id
            LEFT JOIN transfer_settings ts ON ccm.selected_transfer_setting_id = ts.id
            LEFT JOIN status_history sh ON ccm.id = sh.client_campaign_id 
                AND sh.end_date IS NULL
            LEFT JOIN status s ON sh.status_id = s.id
            WHERE ccm.client_id = $1 {status_filter}
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
        
        # Get campaign IDs for call stats and is_active calculation
        campaign_ids = [c['campaign_id'] for c in campaigns_data]
        
        # Calculate is_active for each campaign
        one_minute_ago = datetime.now() - timedelta(minutes=1)
        active_campaigns_query = """
            SELECT DISTINCT client_campaign_model_id
            FROM calls
            WHERE client_campaign_model_id = ANY($1)
            AND timestamp >= $2
        """
        active_campaign_ids = await conn.fetch(active_campaigns_query, campaign_ids, one_minute_ago)
        active_set = {row['client_campaign_model_id'] for row in active_campaign_ids}
        
        # Fetch call statistics
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
            call_stats = call_stats_by_campaign.get(
                camp['campaign_id'],
                CampaignCallStats(
                    total_calls=0,
                    calls_transferred=0,
                    transfer_percentage=0
                )
            )
            
            is_active = camp['campaign_id'] in active_set
            if is_active:
                active_count += 1
            
            status_info = None
            if camp['status_history_id']:
                status_info = StatusInfo(
                    id=camp['status_id'],
                    status_name=camp['status_name'],
                    start_date=camp['status_start'],
                    end_date=camp['status_end']
                )
            
            transfer_setting_info = None
            if camp['ts_id']:
                transfer_setting_info = TransferSettingsInfo(
                    id=camp['ts_id'],
                    name=camp['ts_name'],
                    description=camp['ts_desc'],
                    is_recommended=camp['is_recommended'],
                    quality_score=camp['quality_score'],
                    volume_score=camp['volume_score']
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
                    description=camp['model_desc']
                ),
                transfer_setting=transfer_setting_info,
                start_date=camp['start_date'],
                end_date=camp['end_date'],
                is_active=is_active,
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