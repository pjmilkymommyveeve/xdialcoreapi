from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, timedelta
from core.dependencies import require_roles
from database.db import get_db


router = APIRouter(prefix="/servers/stats", tags=["Server Statistics"])


# ============== MODELS ==============

class CampaignOnServer(BaseModel):
    campaign_id: int
    campaign_name: str
    model_name: str
    client_id: int
    client_name: str
    is_active: bool
    current_status: Optional[str]
    total_bots: int
    active_bots: int
    extension_number: int
    selected_transfer_setting: Optional[str]
    bot_count_on_campaign: int
    long_call_scripts_active: bool
    disposition_set: bool


class ServerStats(BaseModel):
    server_id: int
    server_ip: str
    server_alias: Optional[str]
    server_domain: Optional[str]
    total_campaigns: int
    active_campaigns: int
    total_bots: int
    active_bots: int
    campaigns: List[CampaignOnServer]


class AllServersStatsResponse(BaseModel):
    total_servers: int
    total_campaigns_across_servers: int
    total_active_campaigns: int
    total_bots_across_servers: int
    total_active_bots_across_servers: int
    servers: List[ServerStats]


class ServerCampaignDetail(BaseModel):
    server_id: int
    server_ip: str
    server_alias: Optional[str]
    extension_number: int
    total_bots: int
    active_bots: int


class ClientCampaignServerDistribution(BaseModel):
    campaign_id: int
    campaign_name: str
    model_name: str
    client_id: int
    client_name: str
    is_active: bool
    current_status: Optional[str]
    total_bots_across_servers: int
    total_active_bots_across_servers: int
    servers: List[ServerCampaignDetail]


class ClientDistributionResponse(BaseModel):
    total_campaigns: int
    total_active_campaigns: int
    campaigns: List[ClientCampaignServerDistribution]


# ============== ENDPOINTS ==============

@router.get("/all-servers", response_model=AllServersStatsResponse)
async def get_all_servers_stats(
    user_info: Dict = Depends(require_roles(["admin", "onboarding", "qa"])),
    client_id: Optional[int] = Query(None, description="Filter by specific client"),
    server_id: Optional[int] = Query(None, description="Filter by specific server"),
    active_only: bool = Query(False, description="Show only active campaigns")
):
    """
    ADMIN: GET STATISTICS FOR ALL SERVERS
    
    Shows for each server:
    - Total campaigns and active campaigns
    - Total bot count and active bot count
    - List of all campaigns on that server with details
    - Which client each campaign belongs to
    - Extension numbers being used
    - Transfer settings and other campaign details
    
    Filters:
    - client_id: Show only campaigns for a specific client
    - server_id: Show only a specific server
    - active_only: Show only campaigns that are currently active (had calls in last minute)
    
    A campaign is considered "active" if it has had calls in the last 1 minute.
    Active bots are counted only for active campaigns.
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
        
        if server_id:
            param_count += 1
            where_clauses.append(f"s.id = ${param_count}")
            params.append(server_id)
        
        where_clause = " AND " + " AND ".join(where_clauses) if where_clauses else ""
        
        # Single comprehensive query to get all server and campaign data
        query = f"""
            WITH campaign_activity AS (
                SELECT 
                    client_campaign_model_id,
                    MAX(timestamp) as last_call_time
                FROM calls
                GROUP BY client_campaign_model_id
            )
            SELECT 
                s.id as server_id,
                s.ip as server_ip,
                s.alias as server_alias,
                s.domain as server_domain,
                ccm.id as campaign_id,
                ca.name as campaign_name,
                m.name as model_name,
                ccm.client_id,
                cl.name as client_name,
                ccm.bot_count as campaign_bot_count,
                ccm.long_call_scripts_active,
                ccm.disposition_set,
                ts.name as transfer_setting,
                st.status_name as current_status,
                scb.bot_count as server_bot_count,
                e.extension_number,
                CASE 
                    WHEN ca_data.last_call_time >= $1 THEN true 
                    ELSE false 
                END as is_active
            FROM servers s
            JOIN server_campaign_bots scb ON s.id = scb.server_id
            JOIN client_campaign_model ccm ON scb.client_campaign_model_id = ccm.id
            JOIN clients cl ON ccm.client_id = cl.client_id
            JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
            JOIN campaigns ca ON cm.campaign_id = ca.id
            JOIN models m ON cm.model_id = m.id
            JOIN extensions e ON scb.extension_id = e.id
            LEFT JOIN transfer_settings ts ON ccm.selected_transfer_setting_id = ts.id
            LEFT JOIN status_history sh ON ccm.id = sh.client_campaign_id AND sh.end_date IS NULL
            LEFT JOIN status st ON sh.status_id = st.id
            LEFT JOIN campaign_activity ca_data ON ccm.id = ca_data.client_campaign_model_id
            WHERE 1=1 {where_clause}
            ORDER BY s.id, cl.name, ca.name, m.name
        """
        
        rows = await conn.fetch(query, *params)
        
        if not rows:
            return AllServersStatsResponse(
                total_servers=0,
                total_campaigns_across_servers=0,
                total_active_campaigns=0,
                total_bots_across_servers=0,
                total_active_bots_across_servers=0,
                servers=[]
            )
        
        # Group data by server
        servers_data = {}
        total_campaigns_set = set()
        total_active_campaigns_set = set()
        total_bots = 0
        total_active_bots = 0
        
        for row in rows:
            server_id = row['server_id']
            campaign_id = row['campaign_id']
            is_active = row['is_active']
            server_bot_count = row['server_bot_count']
            
            # Skip if active_only filter is enabled and campaign is not active
            if active_only and not is_active:
                continue
            
            # Track unique campaigns
            total_campaigns_set.add(campaign_id)
            if is_active:
                total_active_campaigns_set.add(campaign_id)
            
            # Initialize server if not exists
            if server_id not in servers_data:
                servers_data[server_id] = {
                    'server_id': server_id,
                    'server_ip': row['server_ip'],
                    'server_alias': row['server_alias'],
                    'server_domain': row['server_domain'],
                    'campaigns': {},
                    'total_bots': 0,
                    'active_bots': 0
                }
            
            # Track server-level stats
            servers_data[server_id]['total_bots'] += server_bot_count
            if is_active:
                servers_data[server_id]['active_bots'] += server_bot_count
            
            # Track global stats
            total_bots += server_bot_count
            if is_active:
                total_active_bots += server_bot_count
            
            # Group campaigns on this server (a campaign can appear once per server)
            if campaign_id not in servers_data[server_id]['campaigns']:
                servers_data[server_id]['campaigns'][campaign_id] = {
                    'campaign_id': campaign_id,
                    'campaign_name': row['campaign_name'],
                    'model_name': row['model_name'],
                    'client_id': row['client_id'],
                    'client_name': row['client_name'],
                    'is_active': is_active,
                    'current_status': row['current_status'],
                    'total_bots': server_bot_count,
                    'active_bots': server_bot_count if is_active else 0,
                    'extension_number': row['extension_number'],
                    'selected_transfer_setting': row['transfer_setting'],
                    'bot_count_on_campaign': row['campaign_bot_count'],
                    'long_call_scripts_active': row['long_call_scripts_active'],
                    'disposition_set': row['disposition_set']
                }
        
        # Build response
        servers_list = []
        for server_id, server_data in servers_data.items():
            campaigns_list = [
                CampaignOnServer(**camp_data) 
                for camp_data in server_data['campaigns'].values()
            ]
            
            servers_list.append(ServerStats(
                server_id=server_data['server_id'],
                server_ip=server_data['server_ip'],
                server_alias=server_data['server_alias'],
                server_domain=server_data['server_domain'],
                total_campaigns=len(campaigns_list),
                active_campaigns=sum(1 for c in campaigns_list if c.is_active),
                total_bots=server_data['total_bots'],
                active_bots=server_data['active_bots'],
                campaigns=campaigns_list
            ))
        
        return AllServersStatsResponse(
            total_servers=len(servers_list),
            total_campaigns_across_servers=len(total_campaigns_set),
            total_active_campaigns=len(total_active_campaigns_set),
            total_bots_across_servers=total_bots,
            total_active_bots_across_servers=total_active_bots,
            servers=servers_list
        )


@router.get("/campaign-distribution", response_model=ClientDistributionResponse)
async def get_campaign_server_distribution(
    user_info: Dict = Depends(require_roles(["admin", "onboarding", "qa"])),
    client_id: Optional[int] = Query(None, description="Filter by specific client"),
    active_only: bool = Query(False, description="Show only active campaigns")
):
    """
    ADMIN: GET CAMPAIGN DISTRIBUTION ACROSS SERVERS
    
    Shows for each campaign:
    - Which servers it's running on
    - How many bots on each server
    - Total bots across all servers
    - Active bot counts
    - Extension numbers on each server
    
    This gives a campaign-centric view instead of server-centric.
    
    Filters:
    - client_id: Show only campaigns for a specific client
    - active_only: Show only campaigns that are currently active
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
        
        where_clause = " AND " + " AND ".join(where_clauses) if where_clauses else ""
        
        # Query to get campaign distribution across servers
        query = f"""
            WITH campaign_activity AS (
                SELECT 
                    client_campaign_model_id,
                    MAX(timestamp) as last_call_time
                FROM calls
                GROUP BY client_campaign_model_id
            )
            SELECT 
                ccm.id as campaign_id,
                ca.name as campaign_name,
                m.name as model_name,
                ccm.client_id,
                cl.name as client_name,
                st.status_name as current_status,
                s.id as server_id,
                s.ip as server_ip,
                s.alias as server_alias,
                e.extension_number,
                scb.bot_count as server_bot_count,
                CASE 
                    WHEN ca_data.last_call_time >= $1 THEN true 
                    ELSE false 
                END as is_active
            FROM client_campaign_model ccm
            JOIN clients cl ON ccm.client_id = cl.client_id
            JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
            JOIN campaigns ca ON cm.campaign_id = ca.id
            JOIN models m ON cm.model_id = m.id
            LEFT JOIN status_history sh ON ccm.id = sh.client_campaign_id AND sh.end_date IS NULL
            LEFT JOIN status st ON sh.status_id = st.id
            LEFT JOIN campaign_activity ca_data ON ccm.id = ca_data.client_campaign_model_id
            JOIN server_campaign_bots scb ON ccm.id = scb.client_campaign_model_id
            JOIN servers s ON scb.server_id = s.id
            JOIN extensions e ON scb.extension_id = e.id
            WHERE 1=1 {where_clause}
            ORDER BY ccm.id, s.id
        """
        
        rows = await conn.fetch(query, *params)
        
        if not rows:
            return ClientDistributionResponse(
                total_campaigns=0,
                total_active_campaigns=0,
                campaigns=[]
            )
        
        # Group data by campaign
        campaigns_data = {}
        
        for row in rows:
            campaign_id = row['campaign_id']
            is_active = row['is_active']
            
            # Skip if active_only filter is enabled and campaign is not active
            if active_only and not is_active:
                continue
            
            # Initialize campaign if not exists
            if campaign_id not in campaigns_data:
                campaigns_data[campaign_id] = {
                    'campaign_id': campaign_id,
                    'campaign_name': row['campaign_name'],
                    'model_name': row['model_name'],
                    'client_id': row['client_id'],
                    'client_name': row['client_name'],
                    'is_active': is_active,
                    'current_status': row['current_status'],
                    'total_bots_across_servers': 0,
                    'total_active_bots_across_servers': 0,
                    'servers': []
                }
            
            # Add server details
            server_bot_count = row['server_bot_count']
            campaigns_data[campaign_id]['total_bots_across_servers'] += server_bot_count
            if is_active:
                campaigns_data[campaign_id]['total_active_bots_across_servers'] += server_bot_count
            
            campaigns_data[campaign_id]['servers'].append({
                'server_id': row['server_id'],
                'server_ip': row['server_ip'],
                'server_alias': row['server_alias'],
                'extension_number': row['extension_number'],
                'total_bots': server_bot_count,
                'active_bots': server_bot_count if is_active else 0
            })
        
        # Build response
        campaigns_list = []
        for campaign_data in campaigns_data.values():
            servers_list = [
                ServerCampaignDetail(**server_data) 
                for server_data in campaign_data['servers']
            ]
            
            campaigns_list.append(ClientCampaignServerDistribution(
                campaign_id=campaign_data['campaign_id'],
                campaign_name=campaign_data['campaign_name'],
                model_name=campaign_data['model_name'],
                client_id=campaign_data['client_id'],
                client_name=campaign_data['client_name'],
                is_active=campaign_data['is_active'],
                current_status=campaign_data['current_status'],
                total_bots_across_servers=campaign_data['total_bots_across_servers'],
                total_active_bots_across_servers=campaign_data['total_active_bots_across_servers'],
                servers=servers_list
            ))
        
        total_active = sum(1 for c in campaigns_list if c.is_active)
        
        return ClientDistributionResponse(
            total_campaigns=len(campaigns_list),
            total_active_campaigns=total_active,
            campaigns=campaigns_list
        )

