from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, date, time, timedelta
from core.dependencies import require_roles
from database.db import get_db
from zoneinfo import ZoneInfo

from utils.mappings import CLIENT_CATEGORY_MAPPING, ADMIN_CATEGORY_MAPPING
from utils.call import group_calls_by_call_id
router = APIRouter(prefix="/campaigns", tags=["Campaigns"])


# ============== CLIENT MODELS ==============

class CallRecord(BaseModel):
    id: int
    call_id: int
    number: str
    list_id: str
    category: str
    category_color: str
    timestamp: str
    stage: int
    has_transcription: bool
    transcription: str
    transferred: bool

class CategoryCount(BaseModel):
    name: str
    color: str
    count: int
    transferred_count: int
    original_name: str

class CampaignInfo(BaseModel):
    id: int
    name: str
    model: str
    is_active: bool


class DashboardFilters(BaseModel):
    search: str
    list_id: str
    start_date: str
    start_time: str
    end_date: str
    end_time: str
    sort_order: str
    selected_categories: List[str]


class PaginationInfo(BaseModel):
    page: int
    page_size: int
    total_records: int
    total_pages: int
    has_next: bool
    has_prev: bool


class CampaignDashboardResponse(BaseModel):
    client_name: str
    campaign: CampaignInfo
    calls: List[CallRecord]
    total_calls: int
    all_categories: List[CategoryCount]
    filters: DashboardFilters
    pagination: PaginationInfo


# ============== ADMIN MODELS ==============

class StageCategoryCount(BaseModel):
    """Category count for a specific stage"""
    stage: int
    count: int
    transferred_count: int


class CategoryCountWithStages(BaseModel):
    """Extended CategoryCount with stage-specific breakdowns"""
    name: str
    color: str
    count: int
    transferred_count: int
    original_name: str
    stage_counts: List[StageCategoryCount] = []

class StageFilter(BaseModel):
    """Model for stage-specific category filters"""
    stage: int
    categories: List[str]

class CallStageDetail(BaseModel):
    stage: int
    category: str
    category_color: str
    voice: str
    timestamp: str
    transcription: str


class DetailedCallRecord(BaseModel):
    id: int
    call_id: int
    number: str
    list_id: str
    latest_category: str
    latest_category_color: str
    latest_stage: int
    first_timestamp: str
    last_timestamp: str
    total_stages: int
    has_transcription: bool
    transferred: bool
    stages: List[CallStageDetail]


class AdminCampaignDashboardResponse(BaseModel):
    client_name: str
    campaign: CampaignInfo
    calls: List[DetailedCallRecord]
    total_calls: int
    all_categories: List[CategoryCountWithStages]
    filters: DashboardFilters
    pagination: PaginationInfo

# ============== TRANSFER METRICS MODELS ==============

class TransferMetrics(BaseModel):
    a_grade_transfers: int
    b_grade_transfers: int
    drop_offs: int
    total_calls: int


class CategoryInterval(BaseModel):
    interval_start: str
    interval_end: str
    categories: List[CategoryCount]


class CategoryTimeSeriesResponse(BaseModel):
    intervals: List[CategoryInterval]
    start_date: str
    end_date: str
    interval_minutes: int

# ============== HELPER FUNCTIONS ==============

def resolve_client_category(original_category: str, call_data: dict) -> str:
    """
    Dynamically resolve category based on call properties.
    All conditional category mapping logic lives here.
    """
    # Rule 1: "already" becomes "Neutral" when transferred
    if original_category == "already" and call_data.get('transferred'):
        return "Neutral"
    
    # Rule 2: "busy" becomes "Neutral" when transferred
    if original_category == "busy" and call_data.get('transferred'):
        return "Neutral"
    
    # Default: use the static mapping
    return CLIENT_CATEGORY_MAPPING.get(original_category, original_category)



async def get_user_client_id(conn, user_id: int, roles: List[str]) -> Optional[int]:
    """
    Get the client_id that the user has access to.
    For 'client' role: returns user_id as client_id
    For 'client_member' role: returns employer's client_ids
    For other roles: returns None (they can access any)
    """
    if 'client' in roles:
        return user_id
    elif 'client_member' in roles:
        employer_query = "SELECT client_id FROM client_employees WHERE user_id = $1"
        employer = await conn.fetchrow(employer_query, user_id)
        if employer:
            return employer['client_id']
    return None


async def verify_campaign_access(conn, campaign_id: int, user_id: int, roles: List[str], allowed_statuses: List[str]) -> dict:
    """Verify user has access to campaign and return campaign data."""
    access_query = """
        SELECT ccm.id, ccm.client_id, c.name as client_name,
               ca.name as campaign_name, m.name as model_name,
               s.status_name as current_status
        FROM client_campaign_model ccm
        JOIN clients c ON ccm.client_id = c.client_id
        JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
        JOIN campaigns ca ON cm.campaign_id = ca.id
        JOIN models m ON cm.model_id = m.id
        LEFT JOIN status_history sh ON ccm.id = sh.client_campaign_id 
            AND sh.end_date IS NULL
        LEFT JOIN status s ON sh.status_id = s.id
        WHERE ccm.id = $1
    """
    campaign = await conn.fetchrow(access_query, campaign_id)
    
    if not campaign:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Campaign not found"
        )
    
    if campaign['current_status'] not in allowed_statuses:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Campaign is not accessible"
        )
    
    # Get allowed client_id for non-privileged users
    allowed_client_id = await get_user_client_id(conn, user_id, roles)
    
    # If allowed_client_id is None, user is privileged (can access any)
    if allowed_client_id is not None and campaign['client_id'] != allowed_client_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied"
        )
    
    return dict(campaign)

# ============== CLIENT ENDPOINT ==============

@router.get("/{campaign_id}/dashboard", response_model=CampaignDashboardResponse)
async def get_client_campaign(
    campaign_id: int,
    user_info: Dict = Depends(require_roles(['admin', 'onboarding', 'client', 'client_member'])),
    search: str = Query("", description="Search by number or category"),
    list_id: str = Query("", description="Filter by list ID"),
    start_date: str = Query("", description="Start date YYYY-MM-DD"),
    start_time: str = Query("", description="Start time HH:MM"),
    end_date: str = Query("", description="End date YYYY-MM-DD"),
    end_time: str = Query("", description="End time HH:MM"),
    categories: List[str] = Query([], description="Selected categories"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=500, description="Records per page"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order for timestamp: asc or desc")
):
    """Get client campaign dashboard with call records (latest stage of each call session)."""
    PRIVILEGED_ROLES = ['admin', 'onboarding', 'qa']
    
    user_id = user_info['user_id']
    roles = user_info['roles']
    
    is_privileged = any(role in PRIVILEGED_ROLES for role in roles)
    allowed_statuses = ['Enabled', 'Testing']
    
    pool = await get_db()
    
    async with pool.acquire() as conn:
        campaign = await verify_campaign_access(conn, campaign_id, user_id, roles, allowed_statuses)
        
        has_any_filter = any([search, list_id, start_date, end_date, categories])
        if not has_any_filter:
            today = date.today()
            start_date = today.strftime('%Y-%m-%d')
            end_date = today.strftime('%Y-%m-%d')
        
        # Build base query WITHOUT category filter for counting all categories
        base_where_clauses = ["c.client_campaign_model_id = $1", "c.call_id IS NOT NULL"]
        base_params = [campaign_id]
        base_param_count = 1
        
        if search:
            base_param_count += 1
            base_where_clauses.append(f"(c.number ILIKE ${base_param_count} OR rc.name ILIKE ${base_param_count})")
            base_params.append(f"%{search}%")
        
        if list_id:
            base_param_count += 1
            base_where_clauses.append(f"c.list_id ILIKE ${base_param_count}")
            base_params.append(f"%{list_id}%")
        
        if start_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                if start_time:
                    time_obj = datetime.strptime(start_time, '%H:%M').time()
                    start_dt = datetime.combine(start_dt.date(), time_obj)
                base_param_count += 1
                base_where_clauses.append(f"c.timestamp >= ${base_param_count}")
                base_params.append(start_dt)
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
                base_param_count += 1
                base_where_clauses.append(f"c.timestamp <= ${base_param_count}")
                base_params.append(end_dt)
            except ValueError:
                pass
        
        # Query for category counts (without category filter)
        base_calls_query = f"""
            SELECT c.id, c.call_id, c.number, c.list_id, c.timestamp, c.stage, c.transferred,
                   c.transcription, rc.name as category_name, rc.color as category_color,
                   v.name as voice_name
            FROM calls c
            LEFT JOIN response_categories rc ON c.response_category_id = rc.id
            LEFT JOIN voices v ON c.voice_id = v.id
            WHERE {' AND '.join(base_where_clauses)}
            ORDER BY c.call_id, c.stage
        """
        base_calls = await conn.fetch(base_calls_query, *base_params)
        base_calls_list = [dict(call) for call in base_calls]
        
        # Group by call_id and get latest stage
        base_call_sessions = group_calls_by_call_id(base_calls_list)
        base_latest_calls = []
        for call_id, session in base_call_sessions.items():
            session_sorted = sorted(session, key=lambda x: x['stage'] or 0)
            base_latest_calls.append(session_sorted[-1])
        
        # Build filtered query WITH category filter for display
        where_clauses = base_where_clauses.copy()
        params = base_params.copy()
        param_count = base_param_count
        
        if categories:
            reverse_mapping = {}
            for orig, combined in CLIENT_CATEGORY_MAPPING.items():
                # Skip empty string mappings
                if combined == "":
                    continue
                if combined not in reverse_mapping:
                    reverse_mapping[combined] = []
                reverse_mapping[combined].append(orig)
            
            original_names = []
            for cat in categories:
                if cat in reverse_mapping:
                    original_names.extend(reverse_mapping[cat])
                else:
                    original_names.append(cat)
            
            if original_names:
                param_count += 1
                where_clauses.append(f"rc.name = ANY(${param_count})")
                params.append(original_names)
        
        # Query for filtered calls (with category filter if applied)
        calls_query = f"""
            SELECT c.id, c.call_id, c.number, c.list_id, c.timestamp, c.stage, c.transferred,
                   c.transcription, rc.name as category_name, rc.color as category_color,
                   v.name as voice_name
            FROM calls c
            LEFT JOIN response_categories rc ON c.response_category_id = rc.id
            LEFT JOIN voices v ON c.voice_id = v.id
            WHERE {' AND '.join(where_clauses)}
            ORDER BY c.call_id, c.stage
        """
        all_calls = await conn.fetch(calls_query, *params)
        calls_list = [dict(call) for call in all_calls]
        
        # Group by call_id and get latest stage
        call_sessions = group_calls_by_call_id(calls_list)
        latest_calls = []
        for call_id, session in call_sessions.items():
            session_sorted = sorted(session, key=lambda x: x['stage'] or 0)
            latest_calls.append(session_sorted[-1])
        
        latest_calls.sort(key=lambda x: x['timestamp'], reverse=(sort_order.lower() == 'desc'))
        
        # Pagination on filtered calls
        total_calls = len(latest_calls)
        total_pages = (total_calls + page_size - 1) // page_size if total_calls > 0 else 1
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_calls = latest_calls[start_idx:end_idx]
        
        # Get categories with counts from BASE (unfiltered by category) calls
        all_categories_query = "SELECT id, name, color FROM response_categories ORDER BY name"
        db_categories = await conn.fetch(all_categories_query)
        
        # Build combined category counts structure
        combined_counts = {}
        combined_transferred_counts = {}
        category_colors = {}
        
        # Process all base calls to count categories
        for call in base_latest_calls:
            if call['category_name']:
                cat_name = call['category_name']
                # Use the resolver function
                resolved_category = resolve_client_category(cat_name, call)
                
                # Skip empty mappings
                if resolved_category == "":
                    continue
                
                # Initialize if this is the first time we see this combined category
                if resolved_category not in combined_counts:
                    combined_counts[resolved_category] = 0
                    combined_transferred_counts[resolved_category] = 0
                    category_colors[resolved_category] = call['category_color'] or '#6B7280'
                
                combined_counts[resolved_category] += 1
                if call.get('transferred'):
                    combined_transferred_counts[resolved_category] += 1
        
        # Build final category list
        all_categories = []
        for combined_name in sorted(combined_counts.keys()):
            all_categories.append(CategoryCount(
                name=combined_name,
                color=category_colors.get(combined_name, '#6B7280'),
                count=combined_counts[combined_name],
                transferred_count=combined_transferred_counts[combined_name],
                original_name=combined_name
            ))
        
        # Format paginated calls - filter out empty string categories
        calls_data = []
        for call in paginated_calls:
            original_category = call['category_name'] or 'Unknown'
            # Use the resolver function with full call data
            combined_category = resolve_client_category(original_category, call)
            
            # Skip calls with empty string category mapping
            if combined_category == "":
                continue
            
            calls_data.append(CallRecord(
                id=call['id'],
                call_id=call['call_id'],
                number=call['number'],
                list_id=call['list_id'] or 'N/A',
                category=combined_category,
                category_color=call['category_color'] or '#6B7280',
                timestamp=call['timestamp'].strftime('%m/%d/%Y, %H:%M:%S'),
                stage=call['stage'] or 0,
                has_transcription=bool(call['transcription']),
                transcription=call['transcription'] or 'No transcript available',
                transferred=call.get('transferred', False)
            ))
        
        return CampaignDashboardResponse(
            client_name=campaign['client_name'],
            campaign=CampaignInfo(
                id=campaign['id'],
                name=campaign['campaign_name'],
                model=campaign['model_name'],
                is_active=False
            ),
            calls=calls_data,
            total_calls=total_calls,
            all_categories=all_categories,
            filters=DashboardFilters(
                search=search,
                list_id=list_id,
                start_date=start_date,
                start_time=start_time,
                end_date=end_date,
                end_time=end_time,
                sort_order=sort_order,  
                selected_categories=categories
            ),
            pagination=PaginationInfo(
                page=page,
                page_size=page_size,
                total_records=total_calls,
                total_pages=total_pages,
                has_next=page < total_pages,
                has_prev=page > 1
            )
        )

# ============== ADMIN ENDPOINT ==============

@router.get("/admin/{campaign_id}/dashboard", response_model=AdminCampaignDashboardResponse)
async def get_admin_campaign_dashboard(
    campaign_id: int,
    user_info: Dict = Depends(require_roles(['admin', 'onboarding', 'qa'])),
    search: str = Query("", description="Search by number or category"),
    list_id: str = Query("", description="Filter by list ID"),
    start_date: str = Query("", description="Start date YYYY-MM-DD"),
    start_time: str = Query("", description="Start time HH:MM"),
    end_date: str = Query("", description="End date YYYY-MM-DD"),
    end_time: str = Query("", description="End time HH:MM"),
    categories: List[str] = Query([], description="Selected categories (applies to latest stage only)"),
    stage_filters: str = Query("", description="JSON string of stage-specific filters: [{\"stage\": 1, \"categories\": [\"cat1\", \"cat2\"]}, ...]"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=500, description="Records per page"),
    sort_order: str = Query("desc", regex="^(asc|desc)$", description="Sort order for timestamp: asc or desc")
):
    """
    Get admin campaign dashboard with detailed call records including all stages.
    
    Supports two types of category filtering:
    1. Legacy `categories` param: filters by latest stage category only
    2. New `stage_filters` param: filters by specific stage categories (JSON array)
    
    Example stage_filters: '[{"stage": 1, "categories": ["unknown"]}, {"stage": 5, "categories": ["already"]}]'
    This would show only sessions where stage 1 has "unknown" AND stage 5 has "already".
    """
    import json
    
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Verify campaign access (admin can see all)
        access_query = """
            SELECT ccm.id, ccm.client_id, c.name as client_name,
                   ca.name as campaign_name, m.name as model_name,
                   s.status_name as current_status
            FROM client_campaign_model ccm
            JOIN clients c ON ccm.client_id = c.client_id
            JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
            JOIN campaigns ca ON cm.campaign_id = ca.id
            JOIN models m ON cm.model_id = m.id
            LEFT JOIN status_history sh ON ccm.id = sh.client_campaign_id 
                AND sh.end_date IS NULL
            LEFT JOIN status s ON sh.status_id = s.id
            WHERE ccm.id = $1
        """
        campaign = await conn.fetchrow(access_query, campaign_id)
        
        if not campaign:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Campaign not found"
            )
        
        # Parse stage filters
        parsed_stage_filters = []
        if stage_filters:
            try:
                stage_filter_data = json.loads(stage_filters)
                if isinstance(stage_filter_data, list):
                    parsed_stage_filters = [
                        StageFilter(**sf) for sf in stage_filter_data
                    ]
            except (json.JSONDecodeError, ValueError) as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid stage_filters format: {str(e)}"
                )
        
        # Default to today if no filters
        has_any_filter = any([search, list_id, start_date, end_date, categories, parsed_stage_filters])
        if not has_any_filter:
            today = date.today()
            start_date = today.strftime('%Y-%m-%d')
            end_date = today.strftime('%Y-%m-%d')
        
        # Build base query (without category filters - we'll filter in Python for efficiency)
        where_clauses = ["c.client_campaign_model_id = $1", "c.call_id IS NOT NULL"]
        params = [campaign_id]
        param_count = 1
        
        if search:
            param_count += 1
            where_clauses.append(f"(c.number ILIKE ${param_count} OR rc.name ILIKE ${param_count})")
            params.append(f"%{search}%")
        
        if list_id:
            param_count += 1
            where_clauses.append(f"c.list_id ILIKE ${param_count}")
            params.append(f"%{list_id}%")
        
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
        
        # Fetch all calls (no category filter in SQL for better performance with stage filters)
        calls_query = f"""
            SELECT c.id, c.call_id, c.number, c.list_id, c.timestamp, c.stage, c.transferred,
                   c.transcription, rc.name as category_name, rc.color as category_color,
                   v.name as voice_name
            FROM calls c
            LEFT JOIN response_categories rc ON c.response_category_id = rc.id
            LEFT JOIN voices v ON c.voice_id = v.id
            WHERE {' AND '.join(where_clauses)}
            ORDER BY c.call_id, c.stage
        """
        all_calls = await conn.fetch(calls_query, *params)
        
        # Convert to list of dicts for grouping
        calls_list = [dict(call) for call in all_calls]
        
        # Group calls by call_id (each call_id is a unique session)
        call_sessions_dict = group_calls_by_call_id(calls_list)
        call_sessions = list(call_sessions_dict.values())
        
        # Store unfiltered sessions for category counting
        unfiltered_sessions = call_sessions.copy()
        
        # Apply stage-specific filters if provided
        if parsed_stage_filters:
            filtered_sessions = []
            
            # Build reverse mapping for ADMIN categories
            reverse_mapping = {}
            for orig, combined in ADMIN_CATEGORY_MAPPING.items():
                if combined not in reverse_mapping:
                    reverse_mapping[combined] = []
                reverse_mapping[combined].append(orig)
            
            for session in call_sessions:
                # Create a stage-to-calls mapping for this session
                stage_map = {}
                for call in session:
                    stage_num = call['stage'] or 0
                    if stage_num not in stage_map:
                        stage_map[stage_num] = []
                    stage_map[stage_num].append(call)
                
                # Check if session matches ALL stage filters
                matches_all_filters = True
                
                for stage_filter in parsed_stage_filters:
                    stage_num = stage_filter.stage
                    required_categories = stage_filter.categories
                    
                    # Convert combined category names to original names
                    original_names = []
                    for cat in required_categories:
                        if cat in reverse_mapping:
                            original_names.extend(reverse_mapping[cat])
                        else:
                            original_names.append(cat)
                    
                    # Check if this stage exists and has any of the required categories
                    if stage_num not in stage_map:
                        matches_all_filters = False
                        break
                    
                    stage_calls = stage_map[stage_num]
                    stage_has_category = any(
                        call['category_name'] in original_names 
                        for call in stage_calls
                    )
                    
                    if not stage_has_category:
                        matches_all_filters = False
                        break
                
                if matches_all_filters:
                    filtered_sessions.append(session)
            
            call_sessions = filtered_sessions
        
        # Apply legacy category filter (applies to latest stage only)
        elif categories:
            filtered_sessions = []
            
            # Build reverse mapping for ADMIN categories
            reverse_mapping = {}
            for orig, combined in ADMIN_CATEGORY_MAPPING.items():
                if combined not in reverse_mapping:
                    reverse_mapping[combined] = []
                reverse_mapping[combined].append(orig)
            
            # Convert combined category names to original names
            original_names = []
            for cat in categories:
                if cat in reverse_mapping:
                    original_names.extend(reverse_mapping[cat])
                else:
                    original_names.append(cat)
            
            for session in call_sessions:
                session_sorted = sorted(session, key=lambda x: x['stage'] or 0)
                latest_call = session_sorted[-1]
                
                if latest_call['category_name'] in original_names:
                    filtered_sessions.append(session)
            
            call_sessions = filtered_sessions
        
        # Sort sessions by latest timestamp
        call_sessions.sort(
            key=lambda session: max(call['timestamp'] for call in session),
            reverse=(sort_order.lower() == 'desc')
        )
        
        # Pagination on filtered sessions
        total_calls = len(call_sessions)
        total_pages = (total_calls + page_size - 1) // page_size if total_calls > 0 else 1
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_sessions = call_sessions[start_idx:end_idx]
        
        # Get all categories for the sidebar
        all_categories_query = "SELECT id, name, color FROM response_categories ORDER BY name"
        db_categories = await conn.fetch(all_categories_query)
        
        # Count categories from ALL calls in unfiltered sessions (for stage-specific counts)
        # Structure: {combined_category: {stage: {count, transferred_count}}}
        stage_category_counts = {}
        
        for session in unfiltered_sessions:
            for call in session:
                if call['category_name']:
                    original_name = call['category_name']
                    combined_name = ADMIN_CATEGORY_MAPPING.get(original_name, original_name)
                    stage_num = call['stage'] or 0
                    
                    if combined_name not in stage_category_counts:
                        stage_category_counts[combined_name] = {}
                    
                    if stage_num not in stage_category_counts[combined_name]:
                        stage_category_counts[combined_name][stage_num] = {
                            'count': 0,
                            'transferred_count': 0
                        }
                    
                    stage_category_counts[combined_name][stage_num]['count'] += 1
                    if call.get('transferred'):
                        stage_category_counts[combined_name][stage_num]['transferred_count'] += 1
        
        # Count categories from latest stage of each unfiltered session (for overall counts)
        category_counts_raw = {}
        for session in unfiltered_sessions:
            latest_call = sorted(session, key=lambda x: x['stage'] or 0)[-1]
            if latest_call['category_name']:
                cat_name = latest_call['category_name']
                if cat_name not in category_counts_raw:
                    category_counts_raw[cat_name] = {
                        'name': cat_name,
                        'color': latest_call['category_color'] or '#6B7280',
                        'count': 0,
                        'transferred_count': 0
                    }
                category_counts_raw[cat_name]['count'] += 1
                if latest_call.get('transferred'):
                    category_counts_raw[cat_name]['transferred_count'] += 1
        
        # Combine categories using ADMIN_CATEGORY_MAPPING
        combined_counts = {}
        combined_transferred_counts = {}
        category_colors = {}
        
        for db_cat in db_categories:
            original_name = db_cat['name'] or 'UNKNOWN'
            combined_name = ADMIN_CATEGORY_MAPPING.get(original_name, original_name)
            
            if combined_name not in combined_counts:
                combined_counts[combined_name] = 0
                combined_transferred_counts[combined_name] = 0
                category_colors[combined_name] = db_cat['color'] or '#6B7280'
        
        for cat_data in category_counts_raw.values():
            original_name = cat_data['name']
            combined_name = ADMIN_CATEGORY_MAPPING.get(original_name, original_name)
            combined_counts[combined_name] += cat_data['count']
            combined_transferred_counts[combined_name] += cat_data['transferred_count']
            if not category_colors.get(combined_name):
                category_colors[combined_name] = cat_data['color']
        
        all_categories = []
        for combined_name in sorted(combined_counts.keys()):
            # Build stage counts for this category
            stage_counts_list = []
            if combined_name in stage_category_counts:
                for stage_num in sorted(stage_category_counts[combined_name].keys()):
                    stage_data = stage_category_counts[combined_name][stage_num]
                    stage_counts_list.append(StageCategoryCount(
                        stage=stage_num,
                        count=stage_data['count'],
                        transferred_count=stage_data['transferred_count']
                    ))
            
            all_categories.append(CategoryCountWithStages(
                name=combined_name,
                color=category_colors.get(combined_name, '#6B7280'),
                count=combined_counts[combined_name],
                transferred_count=combined_transferred_counts[combined_name],
                original_name=combined_name,
                stage_counts=stage_counts_list
            ))
        
        # Format detailed calls (only paginated sessions)
        calls_data = []
        for session in paginated_sessions:
            # Sort stages by stage number
            stages_sorted = sorted(session, key=lambda x: x['stage'] or 0)
            latest_call = stages_sorted[-1]
            first_call = stages_sorted[0]
            
            original_category = latest_call['category_name'] or 'Unknown'
            combined_category = ADMIN_CATEGORY_MAPPING.get(original_category, original_category)
            
            stage_details = []
            # Exclude the last stage from stage_details
            for stage_call in stages_sorted[:-1]:
                stage_category = stage_call['category_name'] or 'Unknown'
                stage_combined = ADMIN_CATEGORY_MAPPING.get(stage_category, stage_category)
                
                stage_details.append(CallStageDetail(
                    stage=stage_call['stage'] or 0,
                    category=stage_combined,
                    category_color=stage_call['category_color'] or '#6B7280',
                    voice=stage_call['voice_name'] or 'Unknown',
                    timestamp=stage_call['timestamp'].strftime('%m/%d/%Y, %H:%M:%S'),
                    transcription=stage_call['transcription'] or 'No transcript available'
                ))
            
            calls_data.append(DetailedCallRecord(
                id=latest_call['id'],
                call_id=latest_call['call_id'],
                number=latest_call['number'],
                list_id=latest_call['list_id'] or 'N/A',
                latest_category=combined_category,
                latest_category_color=latest_call['category_color'] or '#6B7280',
                latest_stage=latest_call['stage'] or 0,
                first_timestamp=first_call['timestamp'].strftime('%m/%d/%Y, %H:%M:%S'),
                last_timestamp=latest_call['timestamp'].strftime('%m/%d/%Y, %H:%M:%S'),
                total_stages=len(stages_sorted),
                has_transcription=any(s['transcription'] for s in stages_sorted),
                transferred=latest_call['transferred'] or False,
                stages=stage_details
            ))
        
        return AdminCampaignDashboardResponse(
            client_name=campaign['client_name'],
            campaign=CampaignInfo(
                id=campaign['id'],
                name=campaign['campaign_name'],
                model=campaign['model_name'],
                is_active=False
            ),
            calls=calls_data,
            total_calls=total_calls,
            all_categories=all_categories,
            filters=DashboardFilters(
                search=search,
                list_id=list_id,
                start_date=start_date,
                start_time=start_time,
                end_date=end_date,
                end_time=end_time,
                sort_order=sort_order,
                selected_categories=categories
            ),
            pagination=PaginationInfo(
                page=page,
                page_size=page_size,
                total_records=total_calls,
                total_pages=total_pages,
                has_next=page < total_pages,
                has_prev=page > 1
            )
        )
# ============== TRANSFER METRICS ENDPOINT ==============

@router.get("/{campaign_id}/transfer-metrics", response_model=TransferMetrics)
async def get_transfer_metrics(
    campaign_id: int,
    user_info: Dict = Depends(require_roles(['admin', 'onboarding', 'client', 'client_member'])),
    start_date: str = Query("", description="Start date YYYY-MM-DD"),
    start_time: str = Query("", description="Start time HH:MM"),
    end_date: str = Query("", description="End date YYYY-MM-DD"),
    end_time: str = Query("", description="End time HH:MM")
):
    """Get transfer metrics: A-grade, B-grade transfers and drop-offs."""
    PRIVILEGED_ROLES = ['admin', 'onboarding', 'qa']
    
    user_id = user_info['user_id']
    roles = user_info['roles']
    
    is_privileged = any(role in PRIVILEGED_ROLES for role in roles)
    allowed_statuses = ['Enabled', 'Testing']
    
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Verify access
        campaign = await verify_campaign_access(conn, campaign_id, user_id, roles, allowed_statuses)
        
        # Default to today if no filters
        if not start_date and not end_date:
            today = date.today()
            start_date = today.strftime('%Y-%m-%d')
            end_date = today.strftime('%Y-%m-%d')
        
        # Build query with filters
        where_clauses = ["c.client_campaign_model_id = $1", "c.call_id IS NOT NULL"]
        params = [campaign_id]
        param_count = 1
        
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
        
        calls_query = f"""
            SELECT c.id, c.call_id, c.number, c.timestamp, c.stage, c.transferred,
                   rc.name as category_name
            FROM calls c
            LEFT JOIN response_categories rc ON c.response_category_id = rc.id
            WHERE {' AND '.join(where_clauses)}
            ORDER BY c.call_id, c.stage
        """
        all_calls = await conn.fetch(calls_query, *params)
        
        # Convert to list of dicts for grouping
        calls_list = [dict(call) for call in all_calls]
        
        # Group calls by call_id
        call_sessions = group_calls_by_call_id(calls_list)
        
        # Get latest stage from each session
        latest_calls = []
        for call_id, session in call_sessions.items():
            session_sorted = sorted(session, key=lambda x: x['stage'] or 0)
            latest_calls.append(session_sorted[-1])
        
        # Calculate metrics using CLIENT_CATEGORY_MAPPING
        a_grade_transfers = 0
        b_grade_transfers = 0
        drop_offs = 0
        
        for call in latest_calls:
            original_category = call['category_name'] or ''
            combined_category = CLIENT_CATEGORY_MAPPING.get(original_category, original_category)
            is_transferred = call.get('transferred', False)
            
            if combined_category == "Qualified" and is_transferred:
                a_grade_transfers += 1
            elif combined_category != "Qualified" and is_transferred:
                b_grade_transfers += 1
            else:
                drop_offs += 1
        
        return TransferMetrics(
            a_grade_transfers=a_grade_transfers,
            b_grade_transfers=b_grade_transfers,
            drop_offs=drop_offs,
            total_calls=len(latest_calls)
        )

@router.get("/{campaign_id}/category-timeseries", response_model=CategoryTimeSeriesResponse)
async def get_category_timeseries(
    campaign_id: int,
    user_info: Dict = Depends(require_roles(['admin', 'onboarding', 'client', 'client_member'])),
    start_date: str = Query("", description="Start date YYYY-MM-DD"),
    start_time: str = Query("", description="Start time HH:MM"),
    end_date: str = Query("", description="End date YYYY-MM-DD"),
    end_time: str = Query("", description="End time HH:MM"),
    interval_minutes: int = Query(60, ge=1, le=1440, description="Interval in minutes (1-1440)")
):
    """
    Get category counts grouped by time intervals for a client's campaign.
    Returns time-series data showing how category counts change over time.
    Uses CLIENT_CATEGORY_MAPPING for category grouping.
    """
    PRIVILEGED_ROLES = ['admin', 'onboarding', 'qa']
    
    user_id = user_info['user_id']
    roles = user_info['roles']
    
    # Determine allowed statuses based on roles
    is_privileged = any(role in PRIVILEGED_ROLES for role in roles)
    allowed_statuses = ['Enabled', 'Testing', 'Disabled'] if is_privileged else ['Enabled', 'Testing']
    
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Verify access to campaign
        campaign = await verify_campaign_access(conn, campaign_id, user_id, roles, allowed_statuses)
        
        # Default to today if no filters provided
        if not start_date and not end_date:
            today = date.today()
            start_date = today.strftime('%Y-%m-%d')
            end_date = today.strftime('%Y-%m-%d')
        
        # Parse start datetime with UTC timezone
        if start_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                if start_time:
                    time_obj = datetime.strptime(start_time, '%H:%M').time()
                    start_dt = datetime.combine(start_dt.date(), time_obj, tzinfo=ZoneInfo('UTC'))
                else:
                    # Default to start of day
                    start_dt = datetime.combine(start_dt.date(), time(0, 0, 0), tzinfo=ZoneInfo('UTC'))
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid start date/time format. Use YYYY-MM-DD for date and HH:MM for time"
                )
        else:
            today = date.today()
            start_dt = datetime.combine(today, time(0, 0, 0), tzinfo=ZoneInfo('UTC'))
        
        # Parse end datetime with UTC timezone
        if end_date:
            try:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                if end_time:
                    time_obj = datetime.strptime(end_time, '%H:%M').time()
                    end_dt = datetime.combine(end_dt.date(), time_obj, tzinfo=ZoneInfo('UTC'))
                else:
                    # Default to end of day
                    end_dt = datetime.combine(end_dt.date(), time(23, 59, 59), tzinfo=ZoneInfo('UTC'))
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid end date/time format. Use YYYY-MM-DD for date and HH:MM for time"
                )
        else:
            # If only start date given, end at end of same day
            end_dt = datetime.combine(start_dt.date(), time(23, 59, 59), tzinfo=ZoneInfo('UTC'))
        
        # Validate time range
        if start_dt >= end_dt:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Start date/time must be before end date/time"
            )
        
        # Efficient single query to fetch all calls in time range
        calls_query = """
            SELECT 
                c.id, 
                c.call_id,
                c.number, 
                c.timestamp, 
                c.stage, 
                c.transferred,
                rc.name as category_name, 
                rc.color as category_color
            FROM calls c
            LEFT JOIN response_categories rc ON c.response_category_id = rc.id
            WHERE c.client_campaign_model_id = $1
                AND c.call_id IS NOT NULL
                AND c.timestamp >= $2
                AND c.timestamp <= $3
            ORDER BY c.call_id, c.stage
        """
        all_calls = await conn.fetch(calls_query, campaign_id, start_dt, end_dt)
        
        # Convert to list of dicts for session grouping
        calls_list = [dict(call) for call in all_calls]
        
        # Group calls by call_id
        call_sessions = group_calls_by_call_id(calls_list)
        
        # Extract latest stage from each session (this represents the final outcome)
        latest_calls = []
        for call_id, session in call_sessions.items():
            session_sorted = sorted(session, key=lambda x: x['stage'] or 0)
            latest_call = session_sorted[-1]
            latest_calls.append(latest_call)
        
        # Get all categories from database to build mapping
        all_categories_query = "SELECT name, color FROM response_categories ORDER BY name"
        db_categories = await conn.fetch(all_categories_query)
        
        # Build category info using CLIENT_CATEGORY_MAPPING
        # Only include categories that are mapped (exclude empty string mappings)
        category_info = {}
        for db_cat in db_categories:
            original_name = db_cat['name'] or 'UNKNOWN'
            
            # Skip if not in mapping or mapped to empty string
            if original_name not in CLIENT_CATEGORY_MAPPING:
                continue
            if CLIENT_CATEGORY_MAPPING[original_name] == "":
                continue
            
            combined_name = CLIENT_CATEGORY_MAPPING[original_name]
            
            if combined_name not in category_info:
                category_info[combined_name] = {
                    'color': db_cat['color'] or '#6B7280',
                    'original_names': []
                }
            category_info[combined_name]['original_names'].append(original_name)
        
        # Generate time intervals
        intervals = []
        current_start = start_dt
        interval_delta = timedelta(minutes=interval_minutes)
        
        while current_start < end_dt:
            current_end = min(current_start + interval_delta, end_dt)
            intervals.append({
                'start': current_start,
                'end': current_end,
                'categories': {
                    cat_name: {'count': 0, 'transferred_count': 0} 
                    for cat_name in category_info.keys()
                }
            })
            current_start = current_end
        
        # Distribute calls into their respective time intervals
        for call in latest_calls:
            call_timestamp = call['timestamp']
            original_category = call['category_name'] or 'Unknown'
            
            # Skip if not in CLIENT_CATEGORY_MAPPING or mapped to empty string
            if original_category not in CLIENT_CATEGORY_MAPPING:
                continue
            if CLIENT_CATEGORY_MAPPING[original_category] == "":
                continue
            
            combined_category = CLIENT_CATEGORY_MAPPING[original_category]
            is_transferred = call.get('transferred', False)
            
            # Find which interval this call belongs to
            for interval in intervals:
                if interval['start'] <= call_timestamp < interval['end']:
                    if combined_category in interval['categories']:
                        interval['categories'][combined_category]['count'] += 1
                        if is_transferred:
                            interval['categories'][combined_category]['transferred_count'] += 1
                    break
        
        # Format response
        response_intervals = []
        for interval in intervals:
            category_counts = []
            
            # Sort categories alphabetically for consistent ordering
            for cat_name in sorted(interval['categories'].keys()):
                cat_data = interval['categories'][cat_name]
                category_counts.append(CategoryCount(
                    name=cat_name,
                    color=category_info[cat_name]['color'],
                    count=cat_data['count'],
                    transferred_count=cat_data['transferred_count'],
                    original_name=cat_name
                ))
            
            response_intervals.append(CategoryInterval(
                interval_start=interval['start'].strftime('%Y-%m-%d %H:%M:%S'),
                interval_end=interval['end'].strftime('%Y-%m-%d %H:%M:%S'),
                categories=category_counts
            ))
        
        return CategoryTimeSeriesResponse(
            intervals=response_intervals,
            start_date=start_dt.strftime('%Y-%m-%d %H:%M:%S'),
            end_date=end_dt.strftime('%Y-%m-%d %H:%M:%S'),
            interval_minutes=interval_minutes
        )