from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, timedelta
from core.dependencies import require_roles
from database.db import get_db


router = APIRouter(prefix="/campaigns/stats", tags=["Campaign Statistics"])


# ============== MODELS ==============

class ServerExtensionGroup(BaseModel):
    server_id: int
    server_ip: str
    server_alias: Optional[str]
    server_domain: Optional[str]
    extension_number: int
    bot_count: int


class CampaignStats(BaseModel):
    client_campaign_model_id: int
    client_id: int
    client_name: str
    client_username: str
    campaign_name: str
    model_name: str
    transfer_setting: Optional[str]
    current_status: Optional[str]
    is_active: bool
    bot_count: int
    total_bots_on_servers: int
    active_bots_on_servers: int
    end_date: Optional[datetime]
    start_date: datetime
    long_call_scripts_active: bool
    disposition_set: bool
    server_extension_groups: List[ServerExtensionGroup]


class AllCampaignsStatsResponse(BaseModel):
    total_campaigns: int
    total_active_campaigns: int
    total_bots_across_all_campaigns: int
    total_active_bots_across_all_campaigns: int
    campaigns: List[CampaignStats]


# ============== ENDPOINTS ==============

@router.get("/all-campaigns", response_model=AllCampaignsStatsResponse)
async def get_all_campaigns_stats(
    user_info: Dict = Depends(require_roles(["admin", "onboarding", "qa"])),
    client_id: Optional[int] = Query(None, description="Filter by specific client"),
    campaign_id: Optional[int] = Query(None, description="Filter by specific campaign"),
    model_id: Optional[int] = Query(None, description="Filter by specific model"),
    active_only: bool = Query(False, description="Show only active campaigns"),
    status_name: Optional[str] = Query(None, description="Filter by status name")
):
    """
    ADMIN: GET STATISTICS FOR ALL CLIENT CAMPAIGNS
    
    Shows for each client campaign:
    - Client Campaign Model ID
    - Client information (ID, name, username)
    - Campaign and Model names
    - Transfer settings
    - Current status
    - Activity status (based on recent calls)
    - Bot count
    - Start and end dates
    - Long call scripts and disposition set flags
    - Server and extension groupings
    
    Filters:
    - client_id: Show only campaigns for a specific client
    - campaign_id: Show only a specific campaign
    - model_id: Show only campaigns using a specific model
    - active_only: Show only campaigns that are currently active (had calls in last minute)
    - status_name: Filter by status name
    
    A campaign is considered "active" if it has had calls in the last 1 minute.
    """
    pool = await get_db()
    
    async with pool.acquire() as conn:
        one_minute_ago = datetime.now() - timedelta(minutes=1)
        
        # Build filters
        where_clauses = []
        params = [one_minute_ago]
        param_count = 1
        
        if client_id:
            param_count += 1
            where_clauses.append(f"ccm.client_id = ${param_count}")
            params.append(client_id)
        
        if campaign_id:
            param_count += 1
            where_clauses.append(f"ca.id = ${param_count}")
            params.append(campaign_id)
        
        if model_id:
            param_count += 1
            where_clauses.append(f"m.id = ${param_count}")
            params.append(model_id)
        
        if status_name:
            param_count += 1
            where_clauses.append(f"st.status_name = ${param_count}")
            params.append(status_name)
        
        where_clause = " AND " + " AND ".join(where_clauses) if where_clauses else ""
        
        # Main query to get all campaign data with server/extension groupings
        query = f"""
            WITH campaign_activity AS (
                SELECT 
                    client_campaign_model_id,
                    MAX(timestamp) as last_call_time
                FROM calls
                GROUP BY client_campaign_model_id
            )
            SELECT 
                ccm.id as client_campaign_model_id,
                ccm.client_id,
                cl.name as client_name,
                u.username as client_username,
                ca.name as campaign_name,
                m.name as model_name,
                ts.name as transfer_setting,
                st.status_name as current_status,
                ccm.bot_count,
                ccm.start_date,
                ccm.end_date,
                ccm.long_call_scripts_active,
                ccm.disposition_set,
                s.id as server_id,
                s.ip as server_ip,
                s.alias as server_alias,
                s.domain as server_domain,
                e.extension_number,
                scb.bot_count as server_bot_count,
                CASE 
                    WHEN ca_data.last_call_time >= $1 THEN true 
                    ELSE false 
                END as is_active
            FROM client_campaign_model ccm
            JOIN clients cl ON ccm.client_id = cl.client_id
            JOIN users u ON cl.client_id = u.id
            JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
            JOIN campaigns ca ON cm.campaign_id = ca.id
            JOIN models m ON cm.model_id = m.id
            LEFT JOIN transfer_settings ts ON ccm.selected_transfer_setting_id = ts.id
            LEFT JOIN status_history sh ON ccm.id = sh.client_campaign_id AND sh.end_date IS NULL
            LEFT JOIN status st ON sh.status_id = st.id
            LEFT JOIN campaign_activity ca_data ON ccm.id = ca_data.client_campaign_model_id
            LEFT JOIN server_campaign_bots scb ON ccm.id = scb.client_campaign_model_id
            LEFT JOIN servers s ON scb.server_id = s.id
            LEFT JOIN extensions e ON scb.extension_id = e.id
            WHERE 1=1 {where_clause}
            ORDER BY ccm.id, s.id, e.extension_number
        """
        
        rows = await conn.fetch(query, *params)
        
        if not rows:
            return AllCampaignsStatsResponse(
                total_campaigns=0,
                total_active_campaigns=0,
                total_bots_across_all_campaigns=0,
                total_active_bots_across_all_campaigns=0,
                campaigns=[]
            )
        
        # Group data by campaign
        campaigns_data = {}
        total_campaigns_set = set()
        total_active_campaigns_set = set()
        total_bots = 0
        total_active_bots = 0
        
        for row in rows:
            campaign_id = row['client_campaign_model_id']
            is_active = row['is_active']
            
            # Skip if active_only filter is enabled and campaign is not active
            if active_only and not is_active:
                continue
            
            # Track unique campaigns
            total_campaigns_set.add(campaign_id)
            if is_active:
                total_active_campaigns_set.add(campaign_id)
            
            # Initialize campaign if not exists
            if campaign_id not in campaigns_data:
                campaigns_data[campaign_id] = {
                    'client_campaign_model_id': campaign_id,
                    'client_id': row['client_id'],
                    'client_name': row['client_name'],
                    'client_username': row['client_username'],
                    'campaign_name': row['campaign_name'],
                    'model_name': row['model_name'],
                    'transfer_setting': row['transfer_setting'],
                    'current_status': row['current_status'],
                    'is_active': is_active,
                    'bot_count': row['bot_count'],
                    'start_date': row['start_date'],
                    'end_date': row['end_date'],
                    'long_call_scripts_active': row['long_call_scripts_active'],
                    'disposition_set': row['disposition_set'],
                    'total_bots_on_servers': 0,
                    'active_bots_on_servers': 0,
                    'server_extension_groups': []
                }
                
                # Track total bots for this campaign
                total_bots += row['bot_count']
                if is_active:
                    total_active_bots += row['bot_count']
            
            # Add server/extension grouping if exists
            if row['server_id'] is not None:
                server_bot_count = row['server_bot_count']
                
                # Update total and active bots on servers
                campaigns_data[campaign_id]['total_bots_on_servers'] += server_bot_count
                if is_active:
                    campaigns_data[campaign_id]['active_bots_on_servers'] += server_bot_count
                
                campaigns_data[campaign_id]['server_extension_groups'].append({
                    'server_id': row['server_id'],
                    'server_ip': row['server_ip'],
                    'server_alias': row['server_alias'],
                    'server_domain': row['server_domain'],
                    'extension_number': row['extension_number'],
                    'bot_count': server_bot_count
                })
        
        # Build response
        campaigns_list = []
        for campaign_data in campaigns_data.values():
            server_groups = [
                ServerExtensionGroup(**group_data)
                for group_data in campaign_data['server_extension_groups']
            ]
            
            campaigns_list.append(CampaignStats(
                client_campaign_model_id=campaign_data['client_campaign_model_id'],
                client_id=campaign_data['client_id'],
                client_name=campaign_data['client_name'],
                client_username=campaign_data['client_username'],
                campaign_name=campaign_data['campaign_name'],
                model_name=campaign_data['model_name'],
                transfer_setting=campaign_data['transfer_setting'],
                current_status=campaign_data['current_status'],
                is_active=campaign_data['is_active'],
                bot_count=campaign_data['bot_count'],
                total_bots_on_servers=campaign_data['total_bots_on_servers'],
                active_bots_on_servers=campaign_data['active_bots_on_servers'],
                start_date=campaign_data['start_date'],
                end_date=campaign_data['end_date'],
                long_call_scripts_active=campaign_data['long_call_scripts_active'],
                disposition_set=campaign_data['disposition_set'],
                server_extension_groups=server_groups
            ))
        
        return AllCampaignsStatsResponse(
            total_campaigns=len(total_campaigns_set),
            total_active_campaigns=len(total_active_campaigns_set),
            total_bots_across_all_campaigns=total_bots,
            total_active_bots_across_all_campaigns=total_active_bots,
            campaigns=campaigns_list
        )


@router.get("/campaign/{campaign_id}", response_model=CampaignStats)
async def get_campaign_stats_by_id(
    campaign_id: int,
    user_info: Dict = Depends(require_roles(["admin", "onboarding", "qa"]))
):
    """
    ADMIN: GET DETAILED STATISTICS FOR A SPECIFIC CAMPAIGN
    
    Shows detailed information for a single client campaign including:
    - All basic campaign information
    - Server and extension groupings
    - Activity status
    - Current status and settings
    """
    pool = await get_db()
    
    async with pool.acquire() as conn:
        one_minute_ago = datetime.now() - timedelta(minutes=1)
        
        query = """
            WITH campaign_activity AS (
                SELECT 
                    client_campaign_model_id,
                    MAX(timestamp) as last_call_time
                FROM calls
                GROUP BY client_campaign_model_id
            )
            SELECT 
                ccm.id as client_campaign_model_id,
                ccm.client_id,
                cl.name as client_name,
                u.username as client_username,
                ca.name as campaign_name,
                m.name as model_name,
                ts.name as transfer_setting,
                st.status_name as current_status,
                ccm.bot_count,
                ccm.start_date,
                ccm.end_date,
                ccm.long_call_scripts_active,
                ccm.disposition_set,
                s.id as server_id,
                s.ip as server_ip,
                s.alias as server_alias,
                s.domain as server_domain,
                e.extension_number,
                scb.bot_count as server_bot_count,
                CASE 
                    WHEN ca_data.last_call_time >= $1 THEN true 
                    ELSE false 
                END as is_active
            FROM client_campaign_model ccm
            JOIN clients cl ON ccm.client_id = cl.client_id
            JOIN users u ON cl.client_id = u.id
            JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
            JOIN campaigns ca ON cm.campaign_id = ca.id
            JOIN models m ON cm.model_id = m.id
            LEFT JOIN transfer_settings ts ON ccm.selected_transfer_setting_id = ts.id
            LEFT JOIN status_history sh ON ccm.id = sh.client_campaign_id AND sh.end_date IS NULL
            LEFT JOIN status st ON sh.status_id = st.id
            LEFT JOIN campaign_activity ca_data ON ccm.id = ca_data.client_campaign_model_id
            LEFT JOIN server_campaign_bots scb ON ccm.id = scb.client_campaign_model_id
            LEFT JOIN servers s ON scb.server_id = s.id
            LEFT JOIN extensions e ON scb.extension_id = e.id
            WHERE ccm.id = $2
            ORDER BY s.id, e.extension_number
        """
        
        rows = await conn.fetch(query, one_minute_ago, campaign_id)
        
        if not rows:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Campaign with ID {campaign_id} not found"
            )
        
        # Build campaign data from rows
        first_row = rows[0]
        server_groups = []
        total_bots_on_servers = 0
        active_bots_on_servers = 0
        is_active = first_row['is_active']
        
        for row in rows:
            if row['server_id'] is not None:
                server_bot_count = row['server_bot_count']
                total_bots_on_servers += server_bot_count
                if is_active:
                    active_bots_on_servers += server_bot_count
                    
                server_groups.append(ServerExtensionGroup(
                    server_id=row['server_id'],
                    server_ip=row['server_ip'],
                    server_alias=row['server_alias'],
                    server_domain=row['server_domain'],
                    extension_number=row['extension_number'],
                    bot_count=server_bot_count
                ))
        
        return CampaignStats(
            client_campaign_model_id=first_row['client_campaign_model_id'],
            client_id=first_row['client_id'],
            client_name=first_row['client_name'],
            client_username=first_row['client_username'],
            campaign_name=first_row['campaign_name'],
            model_name=first_row['model_name'],
            transfer_setting=first_row['transfer_setting'],
            current_status=first_row['current_status'],
            is_active=is_active,
            bot_count=first_row['bot_count'],
            total_bots_on_servers=total_bots_on_servers,
            active_bots_on_servers=active_bots_on_servers,
            start_date=first_row['start_date'],
            end_date=first_row['end_date'],
            long_call_scripts_active=first_row['long_call_scripts_active'],
            disposition_set=first_row['disposition_set'],
            server_extension_groups=server_groups
        )


