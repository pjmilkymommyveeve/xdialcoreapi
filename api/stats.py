from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, date, time
from core.dependencies import require_roles
from database.db import get_db


router = APIRouter(prefix="/campaigns/stats", tags=["Campaign Statistics"])


# ============== MODELS ==============

class VoiceTransferStats(BaseModel):
    voice_name: str
    total_calls: int
    transferred_calls: int
    transfer_rate: float
    non_transferred_calls: int


class CampaignTransferStats(BaseModel):
    campaign_id: int
    campaign_name: str
    model_name: str
    client_name: str
    is_active: bool
    current_status: Optional[str]
    total_calls: int
    transferred_calls: int
    transfer_rate: float
    non_transferred_calls: int
    voice_stats: List[VoiceTransferStats]


class AllCampaignsTransferResponse(BaseModel):
    start_date: Optional[str]
    end_date: Optional[str]
    total_campaigns: int
    campaigns: List[CampaignTransferStats]


class VoiceOverallStats(BaseModel):
    voice_name: str
    total_final_calls: int
    transferred_final_calls: int
    transfer_rate: float
    non_transferred_final_calls: int


class OverallVoiceStatsResponse(BaseModel):
    start_date: Optional[str]
    end_date: Optional[str]
    total_calls: int
    total_transferred: int
    overall_transfer_rate: float
    voice_stats: List[VoiceOverallStats]


# ============== HELPER FUNCTIONS ==============

def calculate_transfer_rate(transferred: int, total: int) -> float:
    """Calculate transfer rate as percentage"""
    if total == 0:
        return 0.0
    return round((transferred / total) * 100, 2)


async def build_date_filter(start_date: str, end_date: str, start_time: str, end_time: str):
    """Build date/time filter parameters"""
    where_clauses = []
    params = []
    param_count = 0
    
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            if start_time:
                time_obj = datetime.strptime(start_time, '%H:%M').time()
                start_dt = datetime.combine(start_dt.date(), time_obj)
            param_count += 1
            where_clauses.append(f"c.timestamp >= ${param_count}")
            params.append(start_dt)
        except ValueError:
            pass
    
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            if end_time:
                time_obj = datetime.strptime(end_time, '%H:%M').time()
                end_dt = datetime.combine(end_dt.date(), time_obj)
            else:
                end_dt = datetime.combine(end_dt.date(), time(23, 59, 59))
            param_count += 1
            where_clauses.append(f"c.timestamp <= ${param_count}")
            params.append(end_dt)
        except ValueError:
            pass
    
    return where_clauses, params, param_count


# ============== ADMIN ENDPOINTS ==============

@router.get("/all-campaigns-transfer-stats", response_model=AllCampaignsTransferResponse)
async def get_all_campaigns_transfer_stats(
    user_info: Dict = Depends(require_roles(["admin", "onboarding", "qa"])),
    start_date: str = Query("", description="Start date YYYY-MM-DD"),
    start_time: str = Query("", description="Start time HH:MM"),
    end_date: str = Query("", description="End date YYYY-MM-DD"),
    end_time: str = Query("", description="End time HH:MM"),
    client_id: Optional[int] = Query(None, description="Filter by specific client")
):
    """
    ADMIN: GET TRANSFER STATISTICS FOR ALL CAMPAIGNS
    
    Shows each campaign with:
    - Voice-level breakdown of transfers (based on final/latest stage)
    - Transfer rates per voice
    - Overall campaign transfer stats
    
    All statistics are based on the FINAL STAGE of each call (last stage per number).
    Only includes campaigns that are not Archived.
    """
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Build campaign filter - exclude archived campaigns
        campaign_filter = """
            EXISTS (
                SELECT 1 FROM status_history sh
                JOIN status s ON sh.status_id = s.id
                WHERE sh.client_campaign_id = ccm.id
                AND sh.end_date IS NULL
                AND s.status_name != 'Archived'
            )
        """
        campaign_params = []
        
        if client_id:
            campaign_filter += " AND ccm.client_id = $1"
            campaign_params = [client_id]
        
        # Get all campaigns
        campaigns_query = f"""
            SELECT ccm.id, ccm.client_id, cl.name as client_name,
                   ca.name as campaign_name, m.name as model_name, ccm.is_active,
                   s.status_name as current_status
            FROM client_campaign_model ccm
            JOIN clients cl ON ccm.client_id = cl.client_id
            JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
            JOIN campaigns ca ON cm.campaign_id = ca.id
            JOIN models m ON cm.model_id = m.id
            LEFT JOIN status_history sh ON ccm.id = sh.client_campaign_id 
                AND sh.end_date IS NULL
            LEFT JOIN status s ON sh.status_id = s.id
            WHERE {campaign_filter}
            ORDER BY cl.name, ca.name, m.name
        """
        campaigns = await conn.fetch(campaigns_query, *campaign_params)
        
        if not campaigns:
            return AllCampaignsTransferResponse(
                start_date=start_date or None,
                end_date=end_date or None,
                total_campaigns=0,
                campaigns=[]
            )
        
        # Build date filter
        date_where, date_params, _ = await build_date_filter(start_date, end_date, start_time, end_time)
        
        campaign_stats_list = []
        
        # Process each campaign
        for campaign in campaigns:
            campaign_id = campaign['id']
            
            # Build where clause for latest stages
            base_where = ["c.client_campaign_model_id = $1"]
            params = [campaign_id] + date_params
            
            if date_where:
                base_where.extend(date_where)
            
            base_where_clause = " AND ".join(base_where)
            
            # Get latest stage for each number (final stage of the call)
            latest_stages_query = f"""
                WITH latest_stages AS (
                    SELECT 
                        c.number,
                        MAX(c.stage) as max_stage
                    FROM calls c
                    WHERE {base_where_clause}
                    GROUP BY c.number
                )
                SELECT 
                    c.id,
                    c.number,
                    c.transferred,
                    v.name as voice_name
                FROM calls c
                INNER JOIN latest_stages ls ON c.number = ls.number AND c.stage = ls.max_stage
                LEFT JOIN voices v ON c.voice_id = v.id
                WHERE c.client_campaign_model_id = $1
            """
            final_calls = await conn.fetch(latest_stages_query, *params)
            
            if not final_calls:
                continue
            
            # Aggregate by voice
            voice_data = {}
            total_calls = len(final_calls)
            total_transferred = 0
            
            for call in final_calls:
                voice_name = call['voice_name'] or 'Unknown'
                transferred = call['transferred'] or False
                
                if voice_name not in voice_data:
                    voice_data[voice_name] = {
                        'total': 0,
                        'transferred': 0
                    }
                
                voice_data[voice_name]['total'] += 1
                if transferred:
                    voice_data[voice_name]['transferred'] += 1
                    total_transferred += 1
            
            # Build voice stats list
            voice_stats = []
            for voice_name in sorted(voice_data.keys()):
                data = voice_data[voice_name]
                voice_stats.append(VoiceTransferStats(
                    voice_name=voice_name,
                    total_calls=data['total'],
                    transferred_calls=data['transferred'],
                    transfer_rate=calculate_transfer_rate(data['transferred'], data['total']),
                    non_transferred_calls=data['total'] - data['transferred']
                ))
            
            # Add campaign stats
            campaign_stats_list.append(CampaignTransferStats(
                campaign_id=campaign['id'],
                campaign_name=campaign['campaign_name'],
                model_name=campaign['model_name'],
                client_name=campaign['client_name'],
                is_active=campaign['is_active'],
                current_status=campaign['current_status'],
                total_calls=total_calls,
                transferred_calls=total_transferred,
                transfer_rate=calculate_transfer_rate(total_transferred, total_calls),
                non_transferred_calls=total_calls - total_transferred,
                voice_stats=voice_stats
            ))
        
        return AllCampaignsTransferResponse(
            start_date=start_date or None,
            end_date=end_date or None,
            total_campaigns=len(campaign_stats_list),
            campaigns=campaign_stats_list
        )


@router.get("/overall-voice-stats", response_model=OverallVoiceStatsResponse)
async def get_overall_voice_stats(
    user_info: Dict = Depends(require_roles(["admin", "onboarding", "qa"])),
    start_date: str = Query("", description="Start date YYYY-MM-DD"),
    start_time: str = Query("", description="Start time HH:MM"),
    end_date: str = Query("", description="End date YYYY-MM-DD"),
    end_time: str = Query("", description="End time HH:MM"),
    client_id: Optional[int] = Query(None, description="Filter by specific client")
):
    """
    ADMIN: GET OVERALL VOICE STATISTICS ACROSS ALL CAMPAIGNS
    
    Shows which voice has what final stages across all campaigns:
    - Total final calls per voice
    - Transferred calls per voice
    - Transfer rate per voice
    - Non-transferred calls per voice
    
    All statistics are based on the FINAL STAGE of each call (last stage per number).
    Aggregates data across all campaigns to show overall voice performance.
    Only includes campaigns that are not Archived.
    """
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Build campaign filter - exclude archived campaigns
        campaign_filter = """
            EXISTS (
                SELECT 1 FROM status_history sh
                JOIN status s ON sh.status_id = s.id
                WHERE sh.client_campaign_id = ccm.id
                AND sh.end_date IS NULL
                AND s.status_name != 'Archived'
            )
        """
        campaign_params = []
        
        if client_id:
            campaign_filter += " AND ccm.client_id = $1"
            campaign_params = [client_id]
        
        # Get all campaign IDs matching filter
        campaigns_query = f"""
            SELECT ccm.id
            FROM client_campaign_model ccm
            WHERE {campaign_filter}
        """
        campaign_ids_result = await conn.fetch(campaigns_query, *campaign_params)
        campaign_ids = [row['id'] for row in campaign_ids_result]
        
        if not campaign_ids:
            return OverallVoiceStatsResponse(
                start_date=start_date or None,
                end_date=end_date or None,
                total_calls=0,
                total_transferred=0,
                overall_transfer_rate=0.0,
                voice_stats=[]
            )
        
        # Build date filter
        date_where, date_params, param_count = await build_date_filter(start_date, end_date, start_time, end_time)
        
        # Build where clause
        where_clauses = ["c.client_campaign_model_id = ANY($1)"]
        params = [campaign_ids] + date_params
        
        if date_where:
            where_clauses.extend(date_where)
        
        where_clause = " AND ".join(where_clauses)
        
        # Get latest stage for each number across all campaigns (final stage of the call)
        final_calls_query = f"""
            WITH latest_stages AS (
                SELECT 
                    c.client_campaign_model_id,
                    c.number,
                    MAX(c.stage) as max_stage
                FROM calls c
                WHERE {where_clause}
                GROUP BY c.client_campaign_model_id, c.number
            )
            SELECT 
                c.id,
                c.number,
                c.transferred,
                v.name as voice_name
            FROM calls c
            INNER JOIN latest_stages ls 
                ON c.client_campaign_model_id = ls.client_campaign_model_id 
                AND c.number = ls.number 
                AND c.stage = ls.max_stage
            LEFT JOIN voices v ON c.voice_id = v.id
            WHERE c.client_campaign_model_id = ANY($1)
        """
        final_calls = await conn.fetch(final_calls_query, *params)
        
        if not final_calls:
            return OverallVoiceStatsResponse(
                start_date=start_date or None,
                end_date=end_date or None,
                total_calls=0,
                total_transferred=0,
                overall_transfer_rate=0.0,
                voice_stats=[]
            )
        
        # Aggregate by voice across all campaigns
        voice_data = {}
        total_calls = len(final_calls)
        total_transferred = 0
        
        for call in final_calls:
            voice_name = call['voice_name'] or 'Unknown'
            transferred = call['transferred'] or False
            
            if voice_name not in voice_data:
                voice_data[voice_name] = {
                    'total': 0,
                    'transferred': 0
                }
            
            voice_data[voice_name]['total'] += 1
            if transferred:
                voice_data[voice_name]['transferred'] += 1
                total_transferred += 1
        
        # Build voice stats list
        voice_stats = []
        for voice_name in sorted(voice_data.keys()):
            data = voice_data[voice_name]
            voice_stats.append(VoiceOverallStats(
                voice_name=voice_name,
                total_final_calls=data['total'],
                transferred_final_calls=data['transferred'],
                transfer_rate=calculate_transfer_rate(data['transferred'], data['total']),
                non_transferred_final_calls=data['total'] - data['transferred']
            ))
        
        return OverallVoiceStatsResponse(
            start_date=start_date or None,
            end_date=end_date or None,
            total_calls=total_calls,
            total_transferred=total_transferred,
            overall_transfer_rate=calculate_transfer_rate(total_transferred, total_calls),
            voice_stats=voice_stats
        )