from fastapi import APIRouter, Depends, HTTPException, status, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, time, timedelta
import csv
import io

from core.dependencies import require_roles
from database.db import get_db
from utils.call import group_calls_by_session

router = APIRouter(prefix="/campaigns/stats", tags=["General Statistics"])

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
    total_final_calls: int
    transferred_calls: int
    transfer_rate: float
    non_transferred_calls: int

class OverallVoiceStatsResponse(BaseModel):
    start_date: Optional[str]
    end_date: Optional[str]
    total_calls: int
    total_transferred: int
    overall_transfer_rate: float
    null_voice_calls: int
    null_voice_ratio: float
    voice_stats: List[VoiceOverallStats]

class CallStageData(BaseModel):
    stage: int
    transcription: Optional[str]
    response_category: Optional[str]
    voice_name: Optional[str]
    transferred: bool
    timestamp: datetime

class CallLookupResult(BaseModel):
    number: str
    campaign_id: int
    campaign_name: str
    model_name: str
    client_name: str
    stages: List[CallStageData]
    final_response_category: Optional[str]
    final_decision_transferred: bool
    total_stages: int

class CallLookupResponse(BaseModel):
    total_numbers_searched: int
    numbers_found: int
    numbers_not_found: int
    results: List[CallLookupResult]
    not_found_numbers: List[str]
    filters_applied: Dict[str, Optional[str]]

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

def calculate_null_voice_ratio(null_count: int, total: int) -> float:
    """Calculate null voice ratio as percentage"""
    if total == 0:
        return 0.0
    return round((null_count / total) * 100, 2)

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

def parse_csv_numbers(content: bytes) -> List[str]:
    """Parse CSV file and extract numbers"""
    try:
        text = content.decode('utf-8')
        reader = csv.reader(io.StringIO(text))
        numbers = []
        
        for row in reader:
            for cell in row:
                cell_numbers = [n.strip() for n in cell.split(',') if n.strip()]
                numbers.extend(cell_numbers)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_numbers = []
        for num in numbers:
            if num not in seen:
                seen.add(num)
                unique_numbers.append(num)
        
        return unique_numbers
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to parse CSV file: {str(e)}"
        )

async def fetch_call_data(
    numbers: List[str],
    conn,
    client_id: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> tuple[List[CallLookupResult], List[str]]:
    """Fetch call data for given numbers with optional filters"""
    if not numbers:
        return [], []
    
    # Build WHERE clause
    where_clauses = ["c.number = ANY($1)"]
    params = [numbers]
    param_count = 1
    
    # Add client filter
    if client_id:
        param_count += 1
        where_clauses.append(f"ccm.client_id = ${param_count}")
        params.append(client_id)
    
    # Add date filters
    if start_date:
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            param_count += 1
            where_clauses.append(f"c.timestamp >= ${param_count}")
            params.append(start_dt)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid start_date format. Use YYYY-MM-DD"
            )
    
    if end_date:
        try:
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            end_dt = datetime.combine(end_dt.date(), time(23, 59, 59))
            param_count += 1
            where_clauses.append(f"c.timestamp <= ${param_count}")
            params.append(end_dt)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid end_date format. Use YYYY-MM-DD"
            )
    
    where_clause = " AND ".join(where_clauses)
    
    # Query to get all call stages for the given numbers
    query = f"""
        SELECT 
            c.number,
            c.stage,
            c.transcription,
            c.transferred,
            c.timestamp,
            c.client_campaign_model_id,
            rc.name as response_category,
            v.name as voice_name,
            cl.name as client_name,
            ca.name as campaign_name,
            m.name as model_name
        FROM calls c
        LEFT JOIN response_categories rc ON c.response_category_id = rc.id
        LEFT JOIN voices v ON c.voice_id = v.id
        JOIN client_campaign_model ccm ON c.client_campaign_model_id = ccm.id
        JOIN clients cl ON ccm.client_id = cl.client_id
        JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
        JOIN campaigns ca ON cm.campaign_id = ca.id
        JOIN models m ON cm.model_id = m.id
        WHERE {where_clause}
        ORDER BY c.number, c.stage
    """
    
    rows = await conn.fetch(query, *params)
    
    # Group by number
    calls_by_number = {}
    for row in rows:
        number = row['number']
        if number not in calls_by_number:
            calls_by_number[number] = {
                'campaign_id': row['client_campaign_model_id'],
                'campaign_name': row['campaign_name'],
                'model_name': row['model_name'],
                'client_name': row['client_name'],
                'stages': []
            }
        
        calls_by_number[number]['stages'].append(CallStageData(
            stage=row['stage'],
            transcription=row['transcription'],
            response_category=row['response_category'],
            voice_name=row['voice_name'],
            transferred=row['transferred'],
            timestamp=row['timestamp']
        ))
    
    # Build results
    results = []
    found_numbers = set()
    
    for number in numbers:
        if number in calls_by_number:
            found_numbers.add(number)
            call_data = calls_by_number[number]
            stages = call_data['stages']
            
            # Get final stage data
            final_stage = stages[-1] if stages else None
            
            results.append(CallLookupResult(
                number=number,
                campaign_id=call_data['campaign_id'],
                campaign_name=call_data['campaign_name'],
                model_name=call_data['model_name'],
                client_name=call_data['client_name'],
                stages=stages,
                final_response_category=final_stage.response_category if final_stage else None,
                final_decision_transferred=final_stage.transferred if final_stage else False,
                total_stages=len(stages)
            ))
    
    # Get numbers not found
    not_found = [num for num in numbers if num not in found_numbers]
    
    return results, not_found

def generate_csv_output(
    results: List[CallLookupResult],
    not_found: List[str],
    filters: Dict[str, Optional[str]]
) -> str:
    """Generate CSV output from call lookup results"""
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Write filter information
    writer.writerow(['Applied Filters:'])
    writer.writerow(['Client ID', filters.get('client_id', 'All')])
    writer.writerow(['Start Date', filters.get('start_date', 'All')])
    writer.writerow(['End Date', filters.get('end_date', 'All')])
    writer.writerow([])  # Empty row
    
    # Write header
    writer.writerow([
        'Number',
        'Client Name',
        'Campaign Name',
        'Model Name',
        'Total Stages',
        'Stage',
        'Transcription',
        'Response Category',
        'Voice',
        'Transferred',
        'Timestamp',
        'Final Response Category',
        'Final Decision (Transferred)'
    ])
    
    # Write data for found numbers
    for result in results:
        for stage in result.stages:
            writer.writerow([
                result.number,
                result.client_name,
                result.campaign_name,
                result.model_name,
                result.total_stages,
                stage.stage,
                stage.transcription or '',
                stage.response_category or '',
                stage.voice_name or '',
                'Yes' if stage.transferred else 'No',
                stage.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                result.final_response_category or '',
                'Yes' if result.final_decision_transferred else 'No'
            ])
    
    # Add section for not found numbers if any
    if not_found:
        writer.writerow([])  # Empty row
        writer.writerow(['Numbers Not Found'])
        for number in not_found:
            writer.writerow([number])
    
    return output.getvalue()

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
    - Null voice count and ratio
    
    All statistics are based on the FINAL STAGE of each call session 
    (last stage per number within 2-minute windows).
    
    Only includes campaigns that are not Archived.
    
    Voices with NULL values are shown separately and not included in voice statistics.
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
            SELECT 
                ccm.id,
                ccm.client_id,
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
            
            # Calculate is_active on the fly
            is_active = await check_campaign_is_active(conn, campaign_id)
            
            # Build where clause
            base_where = ["c.client_campaign_model_id = $1"]
            params = [campaign_id] + date_params
            
            if date_where:
                base_where.extend(date_where)
            
            base_where_clause = " AND ".join(base_where)
            
            # Get all calls for this campaign
            all_calls_query = f"""
                SELECT 
                    c.id,
                    c.number,
                    c.stage,
                    c.timestamp,
                    c.transferred,
                    v.name as voice_name
                FROM calls c
                LEFT JOIN voices v ON c.voice_id = v.id
                WHERE {base_where_clause}
                ORDER BY c.number, c.timestamp, c.stage
            """
            
            all_calls = await conn.fetch(all_calls_query, *params)
            
            if not all_calls:
                continue
            
            # Convert to list of dicts for session grouping
            calls_list = [dict(call) for call in all_calls]
            
            # Group calls into sessions (2-minute window)
            call_sessions = group_calls_by_session(calls_list, duration_minutes=2)
            
            # Get latest stage from each session
            final_calls = []
            for session in call_sessions:
                session_sorted = sorted(session, key=lambda x: x['stage'] or 0)
                final_calls.append(session_sorted[-1])
            
            # Separate null voice calls from voiced calls
            null_voice_calls = [call for call in final_calls if call['voice_name'] is None]
            voiced_calls = [call for call in final_calls if call['voice_name'] is not None]
            
            # Aggregate by voice (only for non-null voices)
            voice_data = {}
            total_voiced_calls = len(voiced_calls)
            total_voiced_transferred = 0
            
            for call in voiced_calls:
                voice_name = call['voice_name']
                transferred = call['transferred'] or False
                
                if voice_name not in voice_data:
                    voice_data[voice_name] = {
                        'total': 0,
                        'transferred': 0
                    }
                
                voice_data[voice_name]['total'] += 1
                if transferred:
                    voice_data[voice_name]['transferred'] += 1
                    total_voiced_transferred += 1
            
            # Build voice stats list (only for non-null voices)
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
            
            # Calculate overall stats (including null voice calls for total count)
            total_all_calls = len(final_calls)
            null_voice_count = len(null_voice_calls)
            
            # Add campaign stats
            campaign_stats_list.append(CampaignTransferStats(
                campaign_id=campaign['id'],
                campaign_name=campaign['campaign_name'],
                model_name=campaign['model_name'],
                client_name=campaign['client_name'],
                is_active=is_active,
                current_status=campaign['current_status'],
                total_calls=total_voiced_calls,
                transferred_calls=total_voiced_transferred,
                transfer_rate=calculate_transfer_rate(total_voiced_transferred, total_voiced_calls),
                non_transferred_calls=total_voiced_calls - total_voiced_transferred,
                null_voice_calls=null_voice_count,
                null_voice_ratio=calculate_null_voice_ratio(null_voice_count, total_all_calls),
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
    - Null voice count and ratio
    
    All statistics are based on the FINAL STAGE of each call session 
    (last stage per number within 2-minute windows).
    
    Aggregates data across all campaigns to show overall voice performance.
    
    Only includes campaigns that are not Archived.
    
    Voices with NULL values are shown separately and not included in voice statistics.
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
                null_voice_calls=0,
                null_voice_ratio=0.0,
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
        
        # Get all calls across all campaigns
        all_calls_query = f"""
            SELECT 
                c.id,
                c.number,
                c.client_campaign_model_id,
                c.stage,
                c.timestamp,
                c.transferred,
                v.name as voice_name
            FROM calls c
            LEFT JOIN voices v ON c.voice_id = v.id
            WHERE {where_clause}
            ORDER BY c.client_campaign_model_id, c.number, c.timestamp, c.stage
        """
        
        all_calls = await conn.fetch(all_calls_query, *params)
        
        if not all_calls:
            return OverallVoiceStatsResponse(
                start_date=start_date or None,
                end_date=end_date or None,
                total_calls=0,
                total_transferred=0,
                overall_transfer_rate=0.0,
                null_voice_calls=0,
                null_voice_ratio=0.0,
                voice_stats=[]
            )
        
        # Convert to list of dicts for session grouping
        calls_list = [dict(call) for call in all_calls]
        
        # Group calls into sessions (2-minute window) - need to group by campaign_id AND number
        # First, separate by campaign
        calls_by_campaign = {}
        for call in calls_list:
            campaign_id = call['client_campaign_model_id']
            if campaign_id not in calls_by_campaign:
                calls_by_campaign[campaign_id] = []
            calls_by_campaign[campaign_id].append(call)
        
        # Group sessions within each campaign, then combine
        all_final_calls = []
        for campaign_calls in calls_by_campaign.values():
            campaign_sessions = group_calls_by_session(campaign_calls, duration_minutes=2)
            for session in campaign_sessions:
                session_sorted = sorted(session, key=lambda x: x['stage'] or 0)
                all_final_calls.append(session_sorted[-1])
        
        # Separate null voice calls from voiced calls
        null_voice_calls = [call for call in all_final_calls if call['voice_name'] is None]
        voiced_calls = [call for call in all_final_calls if call['voice_name'] is not None]
        
        # Aggregate by voice across all campaigns (only for non-null voices)
        voice_data = {}
        total_voiced_calls = len(voiced_calls)
        total_voiced_transferred = 0
        
        for call in voiced_calls:
            voice_name = call['voice_name']
            transferred = call['transferred'] or False
            
            if voice_name not in voice_data:
                voice_data[voice_name] = {
                    'total': 0,
                    'transferred': 0
                }
            
            voice_data[voice_name]['total'] += 1
            if transferred:
                voice_data[voice_name]['transferred'] += 1
                total_voiced_transferred += 1
        
        # Build voice stats list (only for non-null voices)
        voice_stats = []
        for voice_name in sorted(voice_data.keys()):
            data = voice_data[voice_name]
            voice_stats.append(VoiceOverallStats(
                voice_name=voice_name,
                total_final_calls=data['total'],
                transferred_calls=data['transferred'],
                transfer_rate=calculate_transfer_rate(data['transferred'], data['total']),
                non_transferred_calls=data['total'] - data['transferred']
            ))
        
        # Calculate overall stats
        total_all_calls = len(all_final_calls)
        null_voice_count = len(null_voice_calls)
        
        return OverallVoiceStatsResponse(
            start_date=start_date or None,
            end_date=end_date or None,
            total_calls=total_voiced_calls,
            total_transferred=total_voiced_transferred,
            overall_transfer_rate=calculate_transfer_rate(total_voiced_transferred, total_voiced_calls),
            null_voice_calls=null_voice_count,
            null_voice_ratio=calculate_null_voice_ratio(null_voice_count, total_all_calls),
            voice_stats=voice_stats
        )

@router.post("/lookup-calls-json", response_model=CallLookupResponse)
async def lookup_calls_json(
    file: UploadFile = File(..., description="CSV file containing phone numbers"),
    client_id: Optional[int] = Query(None, description="Filter by specific client"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    user_info: Dict = Depends(require_roles(["admin", "onboarding"]))
):
    """
    ADMIN/ONBOARDING: LOOKUP CALL DATA BY PHONE NUMBERS (JSON RESPONSE)
    
    Upload a CSV file containing phone numbers (comma-separated or one per line).
    Returns detailed call data for all stages of each number including:
    - Transcription at each stage
    - Response category at each stage
    - Voice used at each stage
    - Transfer status at each stage
    - Final response category and decision
    
    Optional Filters:
    - client_id: Filter calls by specific client
    - start_date: Filter calls from this date onwards (YYYY-MM-DD)
    - end_date: Filter calls up to this date (YYYY-MM-DD)
    
    Response format: JSON
    """
    # Validate file type
    if not file.filename.endswith('.csv'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be a CSV file"
        )
    
    # Read and parse CSV
    content = await file.read()
    numbers = parse_csv_numbers(content)
    
    if not numbers:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid phone numbers found in CSV file"
        )
    
    # Fetch call data with filters
    pool = await get_db()
    async with pool.acquire() as conn:
        results, not_found = await fetch_call_data(
            numbers,
            conn,
            client_id=client_id,
            start_date=start_date,
            end_date=end_date
        )
    
    filters_applied = {
        "client_id": str(client_id) if client_id else None,
        "start_date": start_date,
        "end_date": end_date
    }
    
    return CallLookupResponse(
        total_numbers_searched=len(numbers),
        numbers_found=len(results),
        numbers_not_found=len(not_found),
        results=results,
        not_found_numbers=not_found,
        filters_applied=filters_applied
    )

@router.post("/lookup-calls-csv")
async def lookup_calls_csv(
    file: UploadFile = File(..., description="CSV file containing phone numbers"),
    client_id: Optional[int] = Query(None, description="Filter by specific client"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    user_info: Dict = Depends(require_roles(["admin", "onboarding"]))
):
    """
    ADMIN/ONBOARDING: LOOKUP CALL DATA BY PHONE NUMBERS (CSV RESPONSE)
    
    Upload a CSV file containing phone numbers (comma-separated or one per line).
    Returns detailed call data for all stages of each number including:
    - Transcription at each stage
    - Response category at each stage
    - Voice used at each stage
    - Transfer status at each stage
    - Final response category and decision
    
    Optional Filters:- client_id: Filter calls by specific client
    - start_date: Filter calls from this date onwards (YYYY-MM-DD)
    - end_date: Filter calls up to this date (YYYY-MM-DD)

    Response format: CSV file download
    """
    # Validate file type
    if not file.filename.endswith('.csv'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File must be a CSV file"
        )

    # Read and parse CSV
    content = await file.read()
    numbers = parse_csv_numbers(content)

    if not numbers:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid phone numbers found in CSV file"
        )

    # Fetch call data with filters
    pool = await get_db()
    async with pool.acquire() as conn:
        results, not_found = await fetch_call_data(
            numbers,
            conn,
            client_id=client_id,
            start_date=start_date,
            end_date=end_date
        )

    # Generate CSV output with filter information
    filters = {
        "client_id": str(client_id) if client_id else None,
        "start_date": start_date or None,
        "end_date": end_date or None
    }
    csv_content = generate_csv_output(results, not_found, filters)

    # Create streaming response
    output = io.BytesIO(csv_content.encode('utf-8'))

    # Build filename with filter information
    filename_parts = ['call_lookup_results']
    if client_id:
        filename_parts.append(f'client_{client_id}')
    if start_date:
        filename_parts.append(f'from_{start_date}')
    if end_date:
        filename_parts.append(f'to_{end_date}')
    filename_parts.append(datetime.now().strftime('%Y%m%d_%H%M%S'))
    filename = '_'.join(filename_parts) + '.csv'

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )