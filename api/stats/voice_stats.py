from fastapi import APIRouter, Depends, HTTPException, status, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, time, timedelta
import csv
import io

from core.dependencies import require_roles
from database.db import get_db
from utils.call import group_calls_by_call_id
from utils.mappings import CLIENT_CATEGORY_MAPPING

router = APIRouter(prefix="/campaigns/stats", tags=["General Statistics"])

# ============== MODELS ==============

class VoiceTransferStats(BaseModel):
    voice_name: str 
    total_calls: int
    transferred_calls: int
    transfer_rate: float
    non_transferred_calls: int
    qualified_transferred_calls: int
    qualified_transfer_rate: float
    non_qualified_transferred_calls: int
    non_qualified_transfer_rate: float

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
    qualified_transferred_calls: int
    qualified_transfer_rate: float
    non_qualified_transferred_calls: int
    non_qualified_transfer_rate: float
    null_voice_calls: int
    null_voice_ratio: float
    voice_stats: List[VoiceTransferStats]

class AllCampaignsTransferResponse(BaseModel):
    start_date: Optional[str]
    end_date: Optional[str]
    total_campaigns: int
    campaigns: List[CampaignTransferStats]

class VoiceOverallStats(BaseModel):
    voice_name: str
    total_calls: int
    transferred_calls: int
    transfer_rate: float
    non_transferred_calls: int
    qualified_transferred_calls: int
    qualified_transfer_rate: float
    non_qualified_transferred_calls: int
    non_qualified_transfer_rate: float

class OverallVoiceStatsResponse(BaseModel):
    start_date: Optional[str]
    end_date: Optional[str]
    total_calls: int
    total_transferred: int
    overall_transfer_rate: float
    qualified_transferred_calls: int
    qualified_transfer_rate: float
    non_qualified_transferred_calls: int
    non_qualified_transfer_rate: float
    null_voice_calls: int
    null_voice_ratio: float
    voice_stats: List[VoiceOverallStats]

# ============== HELPER FUNCTIONS ==============

async def check_campaign_is_active(conn, campaign_id: int) -> bool:
    """Check if campaign had any calls in the last 1 minute"""
    one_minute_ago = datetime.now() - timedelta(minutes=1)
    
    query = """
        SELECT EXISTS(
            SELECT 1 FROM calls 
            WHERE client_campaign_model_id = $1 
            AND timestamp >= $2
        ) as is_active
    """
    
    result = await conn.fetchrow(query, campaign_id, one_minute_ago)
    return result['is_active'] if result else False

def calculate_transfer_rate(transferred: int, total: int) -> float:
    """Calculate transfer rate as percentage"""
    if total == 0:
        return 0.0
    return round((transferred / total) * 100, 2)

def calculate_qualified_rate(qualified: int, transferred: int) -> float:
    """Calculate qualified rate as percentage of transferred calls"""
    if transferred == 0:
        return 0.0
    return round((qualified / transferred) * 100, 2)

def calculate_null_voice_ratio(null_count: int, total: int) -> float:
    """Calculate null voice ratio as percentage"""
    if total == 0:
        return 0.0
    return round((null_count / total) * 100, 2)

async def build_date_filter(start_date: str, end_date: str, start_time: str, end_time: str, param_offset: int = 0):
    """Build date/time filter parameters with parameter offset"""
    where_clauses = []
    params = []
    param_count = param_offset
    
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
    
    return where_clauses, params


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
    - Qualified transfer rates (transferred calls with "Qualified" response category)
    - Non-qualified transfer rates (transferred calls without "Qualified" response category)
    - Overall campaign transfer stats
    - Null voice count and ratio
    
    All statistics are based on the FINAL STAGE of each call_id.
    
    Only includes campaigns that are not Archived.
    
    Voices with NULL values are shown separately and not included in voice statistics.
    
    Uses CLIENT_CATEGORY_MAPPING to determine which categories are "Qualified".
    """
    pool = await get_db()
    async with pool.acquire() as conn:
        # Build list of original category names that map to "Qualified"
        qualified_originals = [orig for orig, combined in CLIENT_CATEGORY_MAPPING.items() 
                              if combined == "Qualified"]
        
        # Build campaign filter parameters
        campaign_where_parts = []
        campaign_params = []
        param_count = 0
        
        if client_id:
            param_count += 1
            campaign_where_parts.append(f"ccm.client_id = ${param_count}")
            campaign_params.append(client_id)
        
        # Build the complete WHERE clause for campaigns
        campaign_filter = " AND ".join(campaign_where_parts) if campaign_where_parts else "1=1"
        
        # Get active campaigns
        campaigns_query = f"""
            SELECT 
                ccm.id as campaign_id,
                cl.name as client_name,
                ca.name as campaign_name,
                m.name as model_name,
                s.status_name as current_status
            FROM client_campaign_model ccm
            JOIN clients cl ON ccm.client_id = cl.client_id
            JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
            JOIN campaigns ca ON cm.campaign_id = ca.id
            JOIN models m ON cm.model_id = m.id
            LEFT JOIN status_history sh ON ccm.id = sh.client_campaign_id AND sh.end_date IS NULL
            LEFT JOIN status s ON sh.status_id = s.id
            WHERE {campaign_filter}
                AND (sh.id IS NULL OR s.status_name != 'Archived')
        """
        campaigns = await conn.fetch(campaigns_query, *campaign_params)
        
        if not campaigns:
            return AllCampaignsTransferResponse(
                start_date=start_date or None,
                end_date=end_date or None,
                total_campaigns=0,
                campaigns=[]
            )
        
        # Build date filter for calls query (no offset needed here)
        date_where, date_params = await build_date_filter(start_date, end_date, start_time, end_time, param_offset=1)
        
        campaigns_dict = {}
        
        # Process each campaign
        for campaign in campaigns:
            campaign_id = campaign['campaign_id']
            
            # Check if campaign is active
            is_active = await check_campaign_is_active(conn, campaign_id)
            
            # Get all calls for this campaign
            calls_query = f"""
                SELECT 
                    c.id,
                    c.call_id,
                    c.number,
                    c.stage,
                    c.timestamp,
                    c.transferred,
                    v.name as voice_name,
                    rc.name as response_category
                FROM calls c
                LEFT JOIN voices v ON c.voice_id = v.id
                LEFT JOIN response_categories rc ON c.response_category_id = rc.id
                WHERE c.client_campaign_model_id = $1
                    {' AND ' + ' AND '.join(date_where) if date_where else ''}
                ORDER BY c.call_id, c.timestamp
            """
            
            calls_params = [campaign_id] + date_params
            all_calls = await conn.fetch(calls_query, *calls_params)
            
            if not all_calls:
                continue
            
            # Convert to list of dicts
            calls_list = [dict(call) for call in all_calls]
            
            # Separate calls with and without call_id
            calls_with_id = [c for c in calls_list if c.get('call_id') is not None]
            calls_without_id = [c for c in calls_list if c.get('call_id') is None]
            
            # Group calls with call_id using the utility function
            grouped = group_calls_by_call_id(calls_with_id)
            
            # Get the call with the highest stage for each call_id
            final_stages = []
            for call_id, calls in grouped.items():
                sorted_calls = sorted(calls, key=lambda x: x.get('stage') or 0)
                final_stages.append(sorted_calls[-1])
            
            # Add calls without call_id as separate sessions
            final_stages.extend(calls_without_id)
            
            # Count overall stats
            total_sessions = len(final_stages)
            null_voice_calls = sum(1 for call in final_stages if call['voice_name'] is None)
            voiced_calls = [call for call in final_stages if call['voice_name'] is not None]
            voiced_count = len(voiced_calls)
            voiced_transferred = sum(1 for call in voiced_calls if call['transferred'])
            qualified_transferred = sum(1 for call in voiced_calls 
                                       if call['transferred'] and call['response_category'] in qualified_originals)
            non_qualified_transferred = voiced_transferred - qualified_transferred
            
            # Count by voice
            voice_stats_dict = {}
            for call in voiced_calls:
                voice = call['voice_name']
                if voice not in voice_stats_dict:
                    voice_stats_dict[voice] = {
                        'total': 0,
                        'transferred': 0,
                        'qualified': 0
                    }
                
                voice_stats_dict[voice]['total'] += 1
                if call['transferred']:
                    voice_stats_dict[voice]['transferred'] += 1
                    if call['response_category'] in qualified_originals:
                        voice_stats_dict[voice]['qualified'] += 1
            
            # Build voice stats list
            voice_stats = []
            for voice_name, stats in voice_stats_dict.items():
                voice_total = stats['total']
                voice_transferred = stats['transferred']
                voice_qualified = stats['qualified']
                voice_non_qualified = voice_transferred - voice_qualified
                
                voice_stats.append(VoiceTransferStats(
                    voice_name=voice_name,
                    total_calls=voice_total,
                    transferred_calls=voice_transferred,
                    transfer_rate=calculate_transfer_rate(voice_transferred, voice_total),
                    non_transferred_calls=voice_total - voice_transferred,
                    qualified_transferred_calls=voice_qualified,
                    qualified_transfer_rate=calculate_qualified_rate(voice_qualified, voice_transferred),
                    non_qualified_transferred_calls=voice_non_qualified,
                    non_qualified_transfer_rate=calculate_qualified_rate(voice_non_qualified, voice_transferred)
                ))
            
            campaigns_dict[campaign_id] = CampaignTransferStats(
                campaign_id=campaign_id,
                campaign_name=campaign['campaign_name'],
                model_name=campaign['model_name'],
                client_name=campaign['client_name'],
                is_active=is_active,
                current_status=campaign['current_status'],
                total_calls=voiced_count,
                transferred_calls=voiced_transferred,
                transfer_rate=calculate_transfer_rate(voiced_transferred, voiced_count),
                non_transferred_calls=voiced_count - voiced_transferred,
                qualified_transferred_calls=qualified_transferred,
                qualified_transfer_rate=calculate_qualified_rate(qualified_transferred, voiced_transferred),
                non_qualified_transferred_calls=non_qualified_transferred,
                non_qualified_transfer_rate=calculate_qualified_rate(non_qualified_transferred, voiced_transferred),
                null_voice_calls=null_voice_calls,
                null_voice_ratio=calculate_null_voice_ratio(null_voice_calls, total_sessions),
                voice_stats=voice_stats
            )
        
        return AllCampaignsTransferResponse(
            start_date=start_date or None,
            end_date=end_date or None,
            total_campaigns=len(campaigns_dict),
            campaigns=list(campaigns_dict.values())
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
    - Qualified transferred calls per voice
    - Qualified transfer rate (of transferred calls)
    - Non-qualified transferred calls per voice
    - Non-qualified transfer rate (of transferred calls)
    - Null voice count and ratio
    
    All statistics are based on the FINAL STAGE of each call_id.
    
    Aggregates data across all campaigns to show overall voice performance.
    
    Only includes campaigns that are not Archived.
    
    Voices with NULL values are shown separately and not included in voice statistics.
    """
    pool = await get_db()
    async with pool.acquire() as conn:
        # Build list of original category names that map to "Qualified"
        qualified_originals = [orig for orig, combined in CLIENT_CATEGORY_MAPPING.items() 
                              if combined == "Qualified"]
        
        # Build campaign filter
        campaign_where_parts = []
        campaign_params = []
        param_count = 0
        
        if client_id:
            param_count += 1
            campaign_where_parts.append(f"ccm.client_id = ${param_count}")
            campaign_params.append(client_id)
        
        campaign_filter = " AND ".join(campaign_where_parts) if campaign_where_parts else "1=1"
        
        # Get active campaigns
        campaigns_query = f"""
            SELECT ccm.id as campaign_id
            FROM client_campaign_model ccm
            LEFT JOIN status_history sh ON ccm.id = sh.client_campaign_id AND sh.end_date IS NULL
            LEFT JOIN status s ON sh.status_id = s.id
            WHERE {campaign_filter}
                AND (sh.id IS NULL OR s.status_name != 'Archived')
        """
        campaigns = await conn.fetch(campaigns_query, *campaign_params)
        
        if not campaigns:
            return OverallVoiceStatsResponse(
                start_date=start_date or None,
                end_date=end_date or None,
                total_calls=0,
                total_transferred=0,
                overall_transfer_rate=0.0,
                qualified_transferred_calls=0,
                qualified_transfer_rate=0.0,
                non_qualified_transferred_calls=0,
                non_qualified_transfer_rate=0.0,
                null_voice_calls=0,
                null_voice_ratio=0.0,
                voice_stats=[]
            )
        
        # Get all calls for all campaigns
        campaign_ids = [c['campaign_id'] for c in campaigns]
        
        # Build date filter for calls query (with offset for campaign_ids parameter)
        date_where, date_params = await build_date_filter(start_date, end_date, start_time, end_time, param_offset=1)
        
        calls_query = f"""
            SELECT 
                c.id,
                c.call_id,
                c.client_campaign_model_id,
                c.number,
                c.stage,
                c.timestamp,
                c.transferred,
                v.name as voice_name,
                rc.name as response_category
            FROM calls c
            LEFT JOIN voices v ON c.voice_id = v.id
            LEFT JOIN response_categories rc ON c.response_category_id = rc.id
            WHERE c.client_campaign_model_id = ANY($1)
                {' AND ' + ' AND '.join(date_where) if date_where else ''}
            ORDER BY c.client_campaign_model_id, c.call_id, c.timestamp
        """
        
        calls_params = [campaign_ids] + date_params
        all_calls = await conn.fetch(calls_query, *calls_params)
        
        if not all_calls:
            return OverallVoiceStatsResponse(
                start_date=start_date or None,
                end_date=end_date or None,
                total_calls=0,
                total_transferred=0,
                overall_transfer_rate=0.0,
                qualified_transferred_calls=0,
                qualified_transfer_rate=0.0,
                non_qualified_transferred_calls=0,
                non_qualified_transfer_rate=0.0,
                null_voice_calls=0,
                null_voice_ratio=0.0,
                voice_stats=[]
            )
        
        # Convert to list of dicts
        calls_list = [dict(call) for call in all_calls]
        
        # Separate calls with and without call_id
        calls_with_id = [c for c in calls_list if c.get('call_id') is not None]
        calls_without_id = [c for c in calls_list if c.get('call_id') is None]
        
        # Group calls with call_id using the utility function
        grouped = group_calls_by_call_id(calls_with_id)
        
        # Get the call with the highest stage for each call_id
        final_stages = []
        for call_id, calls in grouped.items():
            sorted_calls = sorted(calls, key=lambda x: x.get('stage') or 0)
            final_stages.append(sorted_calls[-1])
        
        # Add calls without call_id as separate sessions
        final_stages.extend(calls_without_id)
        
        # Calculate overall totals
        total_sessions = len(final_stages)
        null_voice_calls = sum(1 for call in final_stages if call['voice_name'] is None)
        voiced_calls = [call for call in final_stages if call['voice_name'] is not None]
        voiced_count = len(voiced_calls)
        voiced_transferred = sum(1 for call in voiced_calls if call['transferred'])
        qualified_transferred = sum(1 for call in voiced_calls 
                                   if call['transferred'] and call['response_category'] in qualified_originals)
        non_qualified_transferred = voiced_transferred - qualified_transferred
        
        # Count by voice
        voice_stats_dict = {}
        for call in voiced_calls:
            voice = call['voice_name']
            if voice not in voice_stats_dict:
                voice_stats_dict[voice] = {
                    'total': 0,
                    'transferred': 0,
                    'qualified': 0
                }
            
            voice_stats_dict[voice]['total'] += 1
            if call['transferred']:
                voice_stats_dict[voice]['transferred'] += 1
                if call['response_category'] in qualified_originals:
                    voice_stats_dict[voice]['qualified'] += 1
        
        # Build voice stats list
        voice_stats = []
        for voice_name, stats in sorted(voice_stats_dict.items()):
            voice_total = stats['total']
            voice_transferred = stats['transferred']
            voice_qualified = stats['qualified']
            voice_non_qualified = voice_transferred - voice_qualified
            
            voice_stats.append(VoiceOverallStats(
                voice_name=voice_name,
                total_calls=voice_total,
                transferred_calls=voice_transferred,
                transfer_rate=calculate_transfer_rate(voice_transferred, voice_total),
                non_transferred_calls=voice_total - voice_transferred,
                qualified_transferred_calls=voice_qualified,
                qualified_transfer_rate=calculate_qualified_rate(voice_qualified, voice_transferred),
                non_qualified_transferred_calls=voice_non_qualified,
                non_qualified_transfer_rate=calculate_qualified_rate(voice_non_qualified, voice_transferred)
            ))
        
        return OverallVoiceStatsResponse(
            start_date=start_date or None,
            end_date=end_date or None,
            total_calls=voiced_count,
            total_transferred=voiced_transferred,
            overall_transfer_rate=calculate_transfer_rate(voiced_transferred, voiced_count),
            qualified_transferred_calls=qualified_transferred,
            qualified_transfer_rate=calculate_qualified_rate(qualified_transferred, voiced_transferred),
            non_qualified_transferred_calls=non_qualified_transferred,
            non_qualified_transfer_rate=calculate_qualified_rate(non_qualified_transferred, voiced_transferred),
            null_voice_calls=null_voice_calls,
            null_voice_ratio=calculate_null_voice_ratio(null_voice_calls, total_sessions),
            voice_stats=voice_stats
        )