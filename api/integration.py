from fastapi import APIRouter, HTTPException, status, Depends
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
from core.auth import hash_password
from core.dependencies import get_current_user_id
from database.db import get_db
import asyncpg


router = APIRouter(prefix="/integration", tags=["Integration"])


class TransferSettingInfo(BaseModel):
    id: int
    name: str
    description: str
    is_recommended: bool
    quality_score: int
    volume_score: int
    display_order: int


class IntegrationFormResponse(BaseModel):
    campaigns: List[str]
    campaign_config: Dict[str, Dict[str, List[Dict[str, Any]]]]
    transfer_settings: List[TransferSettingInfo]


class IntegrationRequest(BaseModel):
    company_name: str
    campaign: str
    model_name: str
    transfer_settings_id: int
    number_of_bots: int
    setup_type: str  # 'same' or 'separate'
    primary_ip_validation: str
    primary_admin_link: str
    primary_user: str
    primary_password: str
    primary_bots_campaign: str
    primary_user_series: str
    primary_port: int = 5060
    closer_ip_validation: Optional[str] = None
    closer_admin_link: Optional[str] = None
    closer_user: Optional[str] = None
    closer_password: Optional[str] = None
    closer_campaign: Optional[str] = None
    closer_ingroup: Optional[str] = None
    closer_port: Optional[int] = 5060
    custom_requirements: Optional[str] = None


class IntegrationResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict] = None


@router.get("/form", response_model=IntegrationFormResponse)
async def get_transfer_settings_and_models():
    """GET TRANSFER SETTINGS - Get transfer settings and models (including the avilable transfer settings for each)"""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # get all campaigns
        campaigns_query = "SELECT name FROM campaigns ORDER BY name"
        campaigns_rows = await conn.fetch(campaigns_query)
        campaigns = [row['name'] for row in campaigns_rows]
        
        # build campaign config (campaigns -> models -> transfer settings)
        campaign_config = {}
        for campaign_name in campaigns:
            # get campaign models
            campaign_models_query = """
                SELECT DISTINCT m.id, m.name
                FROM campaign_model cm
                JOIN campaigns c ON cm.campaign_id = c.id
                JOIN models m ON cm.model_id = m.id
                WHERE c.name = $1
            """
            models = await conn.fetch(campaign_models_query, campaign_name)
            
            models_dict = {}
            for model in models:
                # get transfer settings for this model
                transfer_query = """
                    SELECT ts.id, ts.name
                    FROM transfer_settings ts
                    JOIN models_transfer_settings mts ON ts.id = mts.transfersettings_id
                    WHERE mts.model_id = $1
                    ORDER BY ts.display_order
                """
                transfer_settings = await conn.fetch(transfer_query, model['id'])
                
                models_dict[model['name']] = [
                    {'id': ts['id'], 'name': ts['name']} 
                    for ts in transfer_settings
                ]
            
            campaign_config[campaign_name] = models_dict
        
        # get all transfer settings with details
        transfer_settings_query = """
            SELECT id, name, description, is_recommended, 
                   quality_score, volume_score, display_order
            FROM transfer_settings
            ORDER BY display_order
        """
        transfer_settings_rows = await conn.fetch(transfer_settings_query)
        transfer_settings = [
            TransferSettingInfo(**dict(row)) 
            for row in transfer_settings_rows
        ]
        
        return IntegrationFormResponse(
            campaigns=campaigns,
            campaign_config=campaign_config,
            transfer_settings=transfer_settings
        )


@router.post("/request", response_model=IntegrationResponse)
async def submit_integration_request(request: IntegrationRequest):
    """POST INTEGRATION REQUEST - Create new client and campaign setup"""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                # 1. create unique company name
                base_company_name = request.company_name.strip()
                if not base_company_name:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Company name is required"
                    )
                
                company_name = base_company_name
                counter = 1
                
                while True:
                    check_query = "SELECT COUNT(*) as count FROM clients WHERE name = $1"
                    result = await conn.fetchrow(check_query, company_name)
                    if result['count'] == 0:
                        break
                    company_name = f"{counter}{base_company_name}"
                    counter += 1
                
                # 2. create user with client role
                client_role_query = "SELECT id FROM roles WHERE name = 'client'"
                role_row = await conn.fetchrow(client_role_query)
                if not role_row:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Client role not found"
                    )
                client_role_id = role_row['id']
                
                # create unique username
                username = company_name.lower().replace(' ', '_')
                original_username = username
                counter = 1
                
                while True:
                    check_user_query = "SELECT COUNT(*) as count FROM users WHERE username = $1"
                    result = await conn.fetchrow(check_user_query, username)
                    if result['count'] == 0:
                        break
                    username = f"{original_username}{counter}"
                    counter += 1
                
                # hash default password
                hashed_password = hash_password('clientdefault123')
                
                # insert user
                user_insert_query = """
                    INSERT INTO users (username, password, role_id, is_active, is_staff)
                    VALUES ($1, $2, $3, true, false)
                    RETURNING id
                """
                user_row = await conn.fetchrow(
                    user_insert_query, 
                    username, 
                    hashed_password, 
                    client_role_id
                )
                user_id = user_row['id']
                
                # 3. create client profile
                client_insert_query = """
                    INSERT INTO clients (client_id, name, is_archived)
                    VALUES ($1, $2, false)
                """
                await conn.execute(client_insert_query, user_id, company_name)
                
                # 4. get campaign
                campaign_query = "SELECT id FROM campaigns WHERE name = $1"
                campaign_row = await conn.fetchrow(campaign_query, request.campaign)
                if not campaign_row:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Campaign not found"
                    )
                campaign_id = campaign_row['id']
                
                # 5. get transfer settings and model
                ts_query = "SELECT id FROM transfer_settings WHERE id = $1"
                ts_row = await conn.fetchrow(ts_query, request.transfer_settings_id)
                if not ts_row:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Transfer settings not found"
                    )
                
                # get model
                model_query = """
                    SELECT m.id
                    FROM models m
                    JOIN models_transfer_settings mts ON m.id = mts.model_id
                    WHERE m.name = $1 AND mts.transfersettings_id = $2
                """
                model_row = await conn.fetchrow(
                    model_query, 
                    request.model_name, 
                    request.transfer_settings_id
                )
                if not model_row:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Model not found for selected settings"
                    )
                model_id = model_row['id']
                
                # 6. get or create campaign_model
                cm_check_query = """
                    SELECT id FROM campaign_model 
                    WHERE campaign_id = $1 AND model_id = $2
                """
                cm_row = await conn.fetchrow(cm_check_query, campaign_id, model_id)
                
                if cm_row:
                    campaign_model_id = cm_row['id']
                else:
                    cm_insert_query = """
                        INSERT INTO campaign_model (campaign_id, model_id)
                        VALUES ($1, $2)
                        RETURNING id
                    """
                    cm_row = await conn.fetchrow(cm_insert_query, campaign_id, model_id)
                    campaign_model_id = cm_row['id']
                
                # 7. create closer dialer if separate
                closer_dialer_id = None
                if request.setup_type == 'separate':
                    closer_insert_query = """
                        INSERT INTO closer_dialer 
                        (ip_validation_link, admin_link, admin_username, admin_password,
                         closer_campaign, ingroup, port)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        RETURNING id
                    """
                    closer_row = await conn.fetchrow(
                        closer_insert_query,
                        request.closer_ip_validation or '',
                        request.closer_admin_link or '',
                        request.closer_user or '',
                        request.closer_password or '',
                        request.closer_campaign or '',
                        request.closer_ingroup or '',
                        request.closer_port or 5060
                    )
                    closer_dialer_id = closer_row['id']
                
                # 8. create dialer settings
                dialer_settings_query = """
                    INSERT INTO dialer_settings (closer_dialer_id)
                    VALUES ($1)
                    RETURNING id
                """
                ds_row = await conn.fetchrow(dialer_settings_query, closer_dialer_id)
                dialer_settings_id = ds_row['id']
                
                # 9. create primary dialer
                primary_insert_query = """
                    INSERT INTO primary_dialer
                    (ip_validation_link, admin_link, admin_username, admin_password,
                     fronting_campaign, verifier_campaign, port, dialer_settings_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """
                await conn.execute(
                    primary_insert_query,
                    request.primary_ip_validation,
                    request.primary_admin_link,
                    request.primary_user,
                    request.primary_password,
                    request.primary_bots_campaign,
                    request.primary_user_series,
                    request.primary_port,
                    dialer_settings_id
                )
                
                # 10. get or create status
                status_query = """
                    SELECT id FROM status WHERE status_name = 'Pending Approval'
                """
                status_row = await conn.fetchrow(status_query)
                
                if status_row:
                    status_id = status_row['id']
                else:
                    status_insert_query = """
                        INSERT INTO status (status_name)
                        VALUES ('Pending Approval')
                        RETURNING id
                    """
                    status_row = await conn.fetchrow(status_insert_query)
                    status_id = status_row['id']
                
                # 11. create status history
                status_history_query = """
                    INSERT INTO status_history (status_id, start_date, end_date)
                    VALUES ($1, $2, NULL)
                    RETURNING id
                """
                sh_row = await conn.fetchrow(status_history_query, status_id, datetime.now())
                status_history_id = sh_row['id']
                
                # 12. create client campaign model with selected_transfer_setting_id
                ccm_insert_query = """
                    INSERT INTO client_campaign_model
                    (client_id, campaign_model_id, selected_transfer_setting_id, status_history_id, 
                     start_date, end_date, is_custom, custom_comments, current_remote_agents, 
                     is_active, is_enabled, is_approved, dialer_settings_id, bot_count, 
                     long_call_scripts_active, disposition_set)
                    VALUES ($1, $2, $3, $4, $5, NULL, false, '', $6, false, true, false, $7, $8, false, false)
                    RETURNING id
                """
                ccm_row = await conn.fetchrow(
                    ccm_insert_query,
                    user_id,
                    campaign_model_id,
                    request.transfer_settings_id,
                    status_history_id,
                    datetime.now(),
                    request.custom_requirements or '',
                    dialer_settings_id,
                    request.number_of_bots
                )
                ccm_id = ccm_row['id']
                
                return IntegrationResponse(
                    success=True,
                    message="Integration request submitted successfully!",
                    data={
                        "username": username,
                        "client_id": user_id,
                        "client_name": company_name,
                        "campaign": request.campaign,
                        "model": request.model_name,
                        "bot_count": request.number_of_bots,
                        "campaign_model_id": ccm_id
                    }
                )
        
        except asyncpg.exceptions.PostgresError as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Database error: {str(e)}"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An error occurred: {str(e)}"
            )


@router.post("/add-campaign", response_model=IntegrationResponse)
async def add_campaign_to_client(
    request: IntegrationRequest,
    user_id: int = Depends(get_current_user_id)
):
    """POST ADD CAMPAIGN - Add new campaign to existing client | Use setup_type = "separate" for a separate closer dialer"""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                # verify client exists
                client_query = "SELECT client_id FROM clients WHERE client_id = $1"
                client = await conn.fetchrow(client_query, user_id)
                
                if not client:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Client profile not found"
                    )
                
                # get campaign
                campaign_query = "SELECT id FROM campaigns WHERE name = $1"
                campaign_row = await conn.fetchrow(campaign_query, request.campaign)
                if not campaign_row:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Campaign not found"
                    )
                campaign_id = campaign_row['id']
                
                # get model with transfer settings
                model_query = """
                    SELECT m.id
                    FROM models m
                    JOIN models_transfer_settings mts ON m.id = mts.model_id
                    WHERE m.name = $1 AND mts.transfersettings_id = $2
                """
                model_row = await conn.fetchrow(
                    model_query,
                    request.model_name,
                    request.transfer_settings_id
                )
                if not model_row:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Model not found"
                    )
                model_id = model_row['id']
                
                # get or create campaign_model
                cm_check_query = """
                    SELECT id FROM campaign_model 
                    WHERE campaign_id = $1 AND model_id = $2
                """
                cm_row = await conn.fetchrow(cm_check_query, campaign_id, model_id)
                
                if cm_row:
                    campaign_model_id = cm_row['id']
                else:
                    cm_insert_query = """
                        INSERT INTO campaign_model (campaign_id, model_id)
                        VALUES ($1, $2)
                        RETURNING id
                    """
                    cm_row = await conn.fetchrow(cm_insert_query, campaign_id, model_id)
                    campaign_model_id = cm_row['id']
                
                # create closer dialer if needed
                closer_dialer_id = None
                if request.setup_type == 'separate':
                    closer_insert_query = """
                        INSERT INTO closer_dialer 
                        (ip_validation_link, admin_link, admin_username, admin_password,
                         closer_campaign, ingroup, port)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        RETURNING id
                    """
                    closer_row = await conn.fetchrow(
                        closer_insert_query,
                        request.closer_ip_validation or '',
                        request.closer_admin_link or '',
                        request.closer_user or '',
                        request.closer_password or '',
                        request.closer_campaign or '',
                        request.closer_ingroup or '',
                        request.closer_port or 5060
                    )
                    closer_dialer_id = closer_row['id']
                
                # create dialer settings
                dialer_settings_query = """
                    INSERT INTO dialer_settings (closer_dialer_id)
                    VALUES ($1)
                    RETURNING id
                """
                ds_row = await conn.fetchrow(dialer_settings_query, closer_dialer_id)
                dialer_settings_id = ds_row['id']
                
                # create primary dialer
                primary_insert_query = """
                    INSERT INTO primary_dialer
                    (ip_validation_link, admin_link, admin_username, admin_password,
                     fronting_campaign, verifier_campaign, port, dialer_settings_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """
                await conn.execute(
                    primary_insert_query,
                    request.primary_ip_validation,
                    request.primary_admin_link,
                    request.primary_user,
                    request.primary_password,
                    request.primary_bots_campaign,
                    request.primary_user_series,
                    request.primary_port,
                    dialer_settings_id
                )
                
                # get or create status
                status_query = "SELECT id FROM status WHERE status_name = 'Pending Approval'"
                status_row = await conn.fetchrow(status_query)
                
                if status_row:
                    status_id = status_row['id']
                else:
                    status_insert_query = """
                        INSERT INTO status (status_name)
                        VALUES ('Pending Approval')
                        RETURNING id
                    """
                    status_row = await conn.fetchrow(status_insert_query)
                    status_id = status_row['id']
                
                # create status history
                sh_query = """
                    INSERT INTO status_history (status_id, start_date, end_date)
                    VALUES ($1, $2, NULL)
                    RETURNING id
                """
                sh_row = await conn.fetchrow(sh_query, status_id, datetime.now())
                status_history_id = sh_row['id']
                
                # create client campaign model with selected_transfer_setting_id
                ccm_insert_query = """
                    INSERT INTO client_campaign_model
                    (client_id, campaign_model_id, selected_transfer_setting_id, status_history_id, 
                     start_date, end_date, is_custom, custom_comments, current_remote_agents, 
                     is_active, is_enabled, is_approved, dialer_settings_id, bot_count, 
                     long_call_scripts_active, disposition_set)
                    VALUES ($1, $2, $3, $4, $5, NULL, false, '', $6, false, true, false, $7, $8, false, false)
                    RETURNING id
                """
                ccm_row = await conn.fetchrow(
                    ccm_insert_query,
                    user_id,
                    campaign_model_id,
                    request.transfer_settings_id,
                    status_history_id,
                    datetime.now(),
                    request.custom_requirements or '',
                    dialer_settings_id,
                    request.number_of_bots
                )
                ccm_id = ccm_row['id']
                
                return IntegrationResponse(
                    success=True,
                    message="Campaign request submitted successfully!",
                    data={
                        "campaign": request.campaign,
                        "model": request.model_name,
                        "bot_count": request.number_of_bots,
                        "campaign_model_id": ccm_id
                    }
                )
        
        except asyncpg.exceptions.PostgresError as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Database error: {str(e)}"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An error occurred: {str(e)}"
            )