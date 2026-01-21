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
    - Qualified transfer rates (transferred calls with "Qualified" response category)
    - Non-qualified transfer rates (transferred calls without "Qualified" response category)
    - Overall campaign transfer stats
    - Null voice count and ratio
    
    All statistics are based on the FINAL STAGE of each call session 
    (last stage per number within 2-minute windows).
    
    Only includes campaigns that are not Archived.
    
    Voices with NULL values are shown separately and not included in voice statistics.
    """
    pool = await get_db()
    async with pool.acquire() as conn:
        # Build date filter first
        date_where, date_params, param_count = await build_date_filter(start_date, end_date, start_time, end_time)
        
        # Build campaign filter parameters
        campaign_where_parts = []
        campaign_params = []
        current_param = param_count + 1
        
        if client_id:
            campaign_where_parts.append(f"ccm.client_id = ${current_param}")
            campaign_params.append(client_id)
            current_param += 1
        
        # Build the complete WHERE clause for campaigns
        campaign_filter = " AND ".join(campaign_where_parts) if campaign_where_parts else "1=1"
        
        # Optimized query - all aggregations done in database
        query = f"""
            WITH active_campaigns AS (
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
            ),
            campaign_activity AS (
                SELECT 
                    c.client_campaign_model_id as campaign_id,
                    TRUE as is_active
                FROM calls c
                WHERE c.client_campaign_model_id IN (SELECT campaign_id FROM active_campaigns)
                    AND c.timestamp >= NOW() - INTERVAL '1 minute'
                GROUP BY c.client_campaign_model_id
            ),
            all_calls AS (
                SELECT 
                    c.number,
                    c.stage,
                    c.timestamp,
                    c.transferred,
                    c.client_campaign_model_id,
                    v.name as voice_name,
                    rc.name as response_category
                FROM calls c
                LEFT JOIN voices v ON c.voice_id = v.id
                LEFT JOIN response_categories rc ON c.response_category_id = rc.id
                WHERE c.client_campaign_model_id IN (SELECT campaign_id FROM active_campaigns)
                    {' AND ' + ' AND '.join(date_where) if date_where else ''}
            ),
            session_grouped AS (
                SELECT 
                    *,
                    CASE 
                        WHEN LAG(number) OVER (PARTITION BY client_campaign_model_id ORDER BY number, timestamp) = number
                             AND timestamp - LAG(timestamp) OVER (PARTITION BY client_campaign_model_id ORDER BY number, timestamp) <= INTERVAL '2 minutes'
                        THEN 0
                        ELSE 1
                    END as session_start
                FROM all_calls
            ),
            sessions_identified AS (
                SELECT 
                    *,
                    SUM(session_start) OVER (PARTITION BY client_campaign_model_id ORDER BY number, timestamp) as session_id
                FROM session_grouped
            ),
            final_stages AS (
                SELECT DISTINCT ON (client_campaign_model_id, session_id)
                    client_campaign_model_id,
                    session_id,
                    voice_name,
                    transferred,
                    response_category
                FROM sessions_identified
                ORDER BY client_campaign_model_id, session_id, stage DESC NULLS LAST
            ),
            voice_stats AS (
                SELECT 
                    client_campaign_model_id as campaign_id,
                    voice_name,
                    COUNT(*) as total_calls,
                    SUM(CASE WHEN transferred THEN 1 ELSE 0 END) as transferred_calls,
                    SUM(CASE WHEN transferred AND response_category = 'Qualified' THEN 1 ELSE 0 END) as qualified_transferred_calls
                FROM final_stages
                WHERE voice_name IS NOT NULL
                GROUP BY client_campaign_model_id, voice_name
            ),
            campaign_totals AS (
                SELECT 
                    client_campaign_model_id as campaign_id,
                    COUNT(*) as total_sessions,
                    SUM(CASE WHEN voice_name IS NULL THEN 1 ELSE 0 END) as null_voice_calls,
                    SUM(CASE WHEN voice_name IS NOT NULL THEN 1 ELSE 0 END) as voiced_calls,
                    SUM(CASE WHEN voice_name IS NOT NULL AND transferred THEN 1 ELSE 0 END) as voiced_transferred,
                    SUM(CASE WHEN voice_name IS NOT NULL AND transferred AND response_category = 'Qualified' THEN 1 ELSE 0 END) as qualified_transferred
                FROM final_stages
                GROUP BY client_campaign_model_id
            )
            SELECT 
                ac.campaign_id,
                ac.client_name,
                ac.campaign_name,
                ac.model_name,
                ac.current_status,
                COALESCE(ca.is_active, FALSE) as is_active,
                COALESCE(ct.total_sessions, 0) as total_sessions,
                COALESCE(ct.null_voice_calls, 0) as null_voice_calls,
                COALESCE(ct.voiced_calls, 0) as voiced_calls,
                COALESCE(ct.voiced_transferred, 0) as voiced_transferred,
                COALESCE(ct.qualified_transferred, 0) as qualified_transferred,
                vs.voice_name,
                vs.total_calls as voice_total_calls,
                vs.transferred_calls as voice_transferred_calls,
                vs.qualified_transferred_calls as voice_qualified_transferred
            FROM active_campaigns ac
            LEFT JOIN campaign_activity ca ON ac.campaign_id = ca.campaign_id
            LEFT JOIN campaign_totals ct ON ac.campaign_id = ct.campaign_id
            LEFT JOIN voice_stats vs ON ac.campaign_id = vs.campaign_id
            ORDER BY ac.campaign_id, vs.voice_name
        """
        
        all_params = campaign_params + date_params
        rows = await conn.fetch(query, *all_params)
        
        if not rows:
            return AllCampaignsTransferResponse(
                start_date=start_date or None,
                end_date=end_date or None,
                total_campaigns=0,
                campaigns=[]
            )
        
        # Process aggregated results
        campaigns_dict = {}
        
        for row in rows:
            campaign_id = row['campaign_id']
            
            # Initialize campaign if not exists
            if campaign_id not in campaigns_dict:
                total_voiced_calls = row['voiced_calls']
                total_voiced_transferred = row['voiced_transferred']
                total_qualified_transferred = row['qualified_transferred']
                total_non_qualified = total_voiced_transferred - total_qualified_transferred
                
                campaigns_dict[campaign_id] = CampaignTransferStats(
                    campaign_id=campaign_id,
                    campaign_name=row['campaign_name'],
                    model_name=row['model_name'],
                    client_name=row['client_name'],
                    is_active=row['is_active'],
                    current_status=row['current_status'],
                    total_calls=total_voiced_calls,
                    transferred_calls=total_voiced_transferred,
                    transfer_rate=calculate_transfer_rate(total_voiced_transferred, total_voiced_calls),
                    non_transferred_calls=total_voiced_calls - total_voiced_transferred,
                    qualified_transferred_calls=total_qualified_transferred,
                    qualified_transfer_rate=calculate_qualified_rate(total_qualified_transferred, total_voiced_transferred),
                    non_qualified_transferred_calls=total_non_qualified,
                    non_qualified_transfer_rate=calculate_qualified_rate(total_non_qualified, total_voiced_transferred),
                    null_voice_calls=row['null_voice_calls'],
                    null_voice_ratio=calculate_null_voice_ratio(row['null_voice_calls'], row['total_sessions']),
                    voice_stats=[]
                )
            
            # Add voice stats if present
            if row['voice_name'] is not None:
                voice_total = row['voice_total_calls']
                voice_transferred = row['voice_transferred_calls']
                voice_qualified = row['voice_qualified_transferred']
                voice_non_qualified = voice_transferred - voice_qualified
                
                campaigns_dict[campaign_id].voice_stats.append(VoiceTransferStats(
                    voice_name=row['voice_name'],
                    total_calls=voice_total,
                    transferred_calls=voice_transferred,
                    transfer_rate=calculate_transfer_rate(voice_transferred, voice_total),
                    non_transferred_calls=voice_total - voice_transferred,
                    qualified_transferred_calls=voice_qualified,
                    qualified_transfer_rate=calculate_qualified_rate(voice_qualified, voice_transferred),
                    non_qualified_transferred_calls=voice_non_qualified,
                    non_qualified_transfer_rate=calculate_qualified_rate(voice_non_qualified, voice_transferred)
                ))
        
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
    
    All statistics are based on the FINAL STAGE of each call session 
    (last stage per number within 2-minute windows).
    
    Aggregates data across all campaigns to show overall voice performance.
    
    Only includes campaigns that are not Archived.
    
    Voices with NULL values are shown separately and not included in voice statistics.
    """
    pool = await get_db()
    async with pool.acquire() as conn:
        # Build date filter
        date_where, date_params, param_count = await build_date_filter(start_date, end_date, start_time, end_time)
        
        # Build campaign filter
        campaign_where_parts = []
        campaign_params = []
        current_param = param_count + 1
        
        if client_id:
            campaign_where_parts.append(f"ccm.client_id = ${current_param}")
            campaign_params.append(client_id)
        
        campaign_filter = " AND ".join(campaign_where_parts) if campaign_where_parts else "1=1"
        
        # Optimized query - all aggregations done in database
        query = f"""
            WITH active_campaigns AS (
                SELECT ccm.id as campaign_id
                FROM client_campaign_model ccm
                LEFT JOIN status_history sh ON ccm.id = sh.client_campaign_id AND sh.end_date IS NULL
                LEFT JOIN status s ON sh.status_id = s.id
                WHERE {campaign_filter}
                    AND (sh.id IS NULL OR s.status_name != 'Archived')
            ),
            all_calls AS (
                SELECT 
                    c.number,
                    c.client_campaign_model_id,
                    c.stage,
                    c.timestamp,
                    c.transferred,
                    v.name as voice_name,
                    rc.name as response_category
                FROM calls c
                LEFT JOIN voices v ON c.voice_id = v.id
                LEFT JOIN response_categories rc ON c.response_category_id = rc.id
                WHERE c.client_campaign_model_id IN (SELECT campaign_id FROM active_campaigns)
                    {' AND ' + ' AND '.join(date_where) if date_where else ''}
            ),
            session_grouped AS (
                SELECT 
                    *,
                    CASE 
                        WHEN LAG(number) OVER (PARTITION BY client_campaign_model_id ORDER BY number, timestamp) = number
                             AND timestamp - LAG(timestamp) OVER (PARTITION BY client_campaign_model_id ORDER BY number, timestamp) <= INTERVAL '2 minutes'
                        THEN 0
                        ELSE 1
                    END as session_start
                FROM all_calls
            ),
            sessions_identified AS (
                SELECT 
                    *,
                    SUM(session_start) OVER (PARTITION BY client_campaign_model_id ORDER BY number, timestamp) as session_id
                FROM session_grouped
            ),
            final_stages AS (
                SELECT DISTINCT ON (client_campaign_model_id, session_id)
                    client_campaign_model_id,
                    session_id,
                    voice_name,
                    transferred,
                    response_category
                FROM sessions_identified
                ORDER BY client_campaign_model_id, session_id, stage DESC NULLS LAST
            ),
            voice_aggregates AS (
                SELECT 
                    voice_name,
                    COUNT(*) as total_calls,
                    SUM(CASE WHEN transferred THEN 1 ELSE 0 END) as transferred_calls,
                    SUM(CASE WHEN transferred AND response_category = 'Qualified' THEN 1 ELSE 0 END) as qualified_transferred_calls
                FROM final_stages
                WHERE voice_name IS NOT NULL
                GROUP BY voice_name
            ),
            overall_totals AS (
                SELECT 
                    COUNT(*) as total_sessions,
                    SUM(CASE WHEN voice_name IS NULL THEN 1 ELSE 0 END) as null_voice_calls,
                    SUM(CASE WHEN voice_name IS NOT NULL THEN 1 ELSE 0 END) as voiced_calls,
                    SUM(CASE WHEN voice_name IS NOT NULL AND transferred THEN 1 ELSE 0 END) as voiced_transferred,
                    SUM(CASE WHEN voice_name IS NOT NULL AND transferred AND response_category = 'Qualified' THEN 1 ELSE 0 END) as qualified_transferred
                FROM final_stages
            )
            SELECT 
                ot.total_sessions,
                ot.null_voice_calls,
                ot.voiced_calls,
                ot.voiced_transferred,
                ot.qualified_transferred,
                va.voice_name,
                va.total_calls as voice_total_calls,
                va.transferred_calls as voice_transferred_calls,
                va.qualified_transferred_calls as voice_qualified_transferred
            FROM overall_totals ot
            LEFT JOIN voice_aggregates va ON TRUE
            ORDER BY va.voice_name
        """
        
        all_params = campaign_params + date_params
        rows = await conn.fetch(query, *all_params)
        
        if not rows or rows[0]['total_sessions'] is None:
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
        
        # Get totals from first row (they're the same across all rows)
        first_row = rows[0]
        total_sessions = first_row['total_sessions']
        null_voice_calls = first_row['null_voice_calls']
        voiced_calls = first_row['voiced_calls']
        voiced_transferred = first_row['voiced_transferred']
        qualified_transferred = first_row['qualified_transferred']
        non_qualified_transferred = voiced_transferred - qualified_transferred
        
        # Build voice stats list
        voice_stats = []
        for row in rows:
            if row['voice_name'] is not None:
                voice_total = row['voice_total_calls']
                voice_transferred = row['voice_transferred_calls']
                voice_qualified = row['voice_qualified_transferred']
                voice_non_qualified = voice_transferred - voice_qualified
                
                voice_stats.append(VoiceOverallStats(
                    voice_name=row['voice_name'],
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
            total_calls=voiced_calls,
            total_transferred=voiced_transferred,
            overall_transfer_rate=calculate_transfer_rate(voiced_transferred, voiced_calls),
            qualified_transferred_calls=qualified_transferred,
            qualified_transfer_rate=calculate_qualified_rate(qualified_transferred, voiced_transferred),
            non_qualified_transferred_calls=non_qualified_transferred,
            non_qualified_transfer_rate=calculate_qualified_rate(non_qualified_transferred, voiced_transferred),
            null_voice_calls=null_voice_calls,
            null_voice_ratio=calculate_null_voice_ratio(null_voice_calls, total_sessions),
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
    
    Optional Filters:
    - client_id: Filter calls by specific client
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