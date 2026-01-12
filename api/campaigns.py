from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, date, time, timedelta
from core.dependencies import require_roles
from database.db import get_db


router = APIRouter(prefix="/campaigns", tags=["Campaigns"])


# ============== CATEGORY MAPPINGS ==============

CLIENT_CATEGORY_MAPPING = {
    "spanishanswermachine": "Answering Machine",
    "answermachine": "Answering Machine",

    "dnc": "DNC",
    "dnq": "Do Not Qualify",
    "honeypot": "Honeypot",

    "unknown": "Unclear Response",

    "busy": "Busy",
    "already": "Already",
    "notinterested": "Not Interested",
    "rebuttal": "Not Interested",
    "donttransfer": "Not Interested",

    "qualified": "Qualified",
    "interested": "Qualified",

    "neutral": "Neutral",

    "inaudible": "DAIR",
    "notresponding" : "DAIR",
    "usersilent": "User Silent",
    "userhangup": "User Hangup",
}


ADMIN_CATEGORY_MAPPING = {
    "greetingresponse": "Greeting Response",
    "notfeelinggood": "Not Feeling Good",
    "dnc": "Do Not Call",
    "honeypot_hardcoded": "Honeypot",
    "honeypot": "Honeypot",
    "spanishanswermachine": "Spanish Answering Machine",
    "answermachine": "Answering Machine",
    "already": "Already Customer",
    "rebuttal": "Rebuttal",
    "notinterested": "Not Interested",
    "busy": "Busy",
    "dnq": "Do Not Qualify",
    "qualified": "Qualified",
    "neutral": "Neutral",
    "repeatpitch": "Repeat Pitch",
    "interested": "Interested",
    "unkown": "Unclear Response",
}


# ============== CLIENT MODELS ==============

class CallRecord(BaseModel):
    id: int
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

class CallStageDetail(BaseModel):
    stage: int
    category: str
    category_color: str
    voice: str
    timestamp: str
    transcription: str


class DetailedCallRecord(BaseModel):
    id: int
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
    all_categories: List[CategoryCount]
    filters: DashboardFilters
    pagination: PaginationInfo


# ============== HELPER FUNCTIONS ==============

def group_calls_by_session(calls: List[dict], duration_minutes: int = 2) -> List[List[dict]]:
    """
    Group calls by number and timestamp proximity.
    Calls with the same number within duration_minutes are considered part of the same session.
    """
    if not calls:
        return []
    
    # Sort by number and timestamp
    sorted_calls = sorted(calls, key=lambda x: (x['number'], x['timestamp']))
    
    sessions = []
    current_session = []
    
    for call in sorted_calls:
        if not current_session:
            current_session.append(call)
        else:
            last_call = current_session[-1]
            
            # Check if same number and within duration window
            same_number = call['number'] == last_call['number']
            time_diff = call['timestamp'] - last_call['timestamp']
            within_window = time_diff <= timedelta(minutes=duration_minutes)
            
            if same_number and within_window:
                current_session.append(call)
            else:
                # Start new session
                sessions.append(current_session)
                current_session = [call]
    
    # Add last session
    if current_session:
        sessions.append(current_session)
    
    return sessions


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
               ca.name as campaign_name, m.name as model_name, ccm.is_active,
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
    # Privileged roles that can see any campaign with any status
    PRIVILEGED_ROLES = ['admin', 'onboarding', 'qa']
    
    user_id = user_info['user_id']
    roles = user_info['roles']
    
    # Determine allowed statuses based on roles
    is_privileged = any(role in PRIVILEGED_ROLES for role in roles)
    allowed_statuses = ['Enabled', 'Testing','Disabled'] if is_privileged else ['Enabled', 'Testing']
    
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Verify access
        campaign = await verify_campaign_access(conn, campaign_id, user_id, roles, allowed_statuses)
        
        # Default to today if no filters
        has_any_filter = any([search, list_id, start_date, end_date, categories])
        if not has_any_filter:
            today = date.today()
            start_date = today.strftime('%Y-%m-%d')
            end_date = today.strftime('%Y-%m-%d')
        
        # Build query with filters
        where_clauses = ["c.client_campaign_model_id = $1"]
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
        
        if categories:
            reverse_mapping = {}
            for orig, combined in CLIENT_CATEGORY_MAPPING.items():
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
        
        calls_query = f"""
            SELECT c.id, c.number, c.list_id, c.timestamp, c.stage, c.transferred,
                   c.transcription, rc.name as category_name, rc.color as category_color,
                   v.name as voice_name
            FROM calls c
            LEFT JOIN response_categories rc ON c.response_category_id = rc.id
            LEFT JOIN voices v ON c.voice_id = v.id
            WHERE {' AND '.join(where_clauses)}
            ORDER BY c.number, c.timestamp, c.stage
        """
        all_calls = await conn.fetch(calls_query, *params)
        
        # Convert to list of dicts for grouping
        calls_list = [dict(call) for call in all_calls]
        
        # Group calls into sessions (2-minute window)
        call_sessions = group_calls_by_session(calls_list, duration_minutes=2)
        
        # Get latest stage from each session
        latest_calls = []
        for session in call_sessions:
            # Sort by stage to get the latest
            session_sorted = sorted(session, key=lambda x: x['stage'] or 0)
            latest_calls.append(session_sorted[-1])
        
        # Sort by timestamp based on sort_order parameter
        latest_calls.sort(key=lambda x: x['timestamp'], reverse=(sort_order.lower() == 'desc'))
        
        # Pagination
        total_calls = len(latest_calls)
        total_pages = (total_calls + page_size - 1) // page_size if total_calls > 0 else 1
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_calls = latest_calls[start_idx:end_idx]
        
        # Get categories (from latest call in each session)
        all_categories_query = "SELECT id, name, color FROM response_categories ORDER BY name"
        db_categories = await conn.fetch(all_categories_query)
        
        category_counts_raw = {}
        for session in call_sessions:
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
            all_categories.append(CategoryCount(
                name=combined_name,
                color=category_colors.get(combined_name, '#6B7280'),
                count=combined_counts[combined_name],
                transferred_count=combined_transferred_counts[combined_name],
                original_name=combined_name
            ))
        
        calls_data = []
        for call in paginated_calls:
            original_category = call['category_name'] or 'Unknown'
            combined_category = CLIENT_CATEGORY_MAPPING.get(original_category, original_category)
            
            calls_data.append(CallRecord(
                id=call['id'],
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
                is_active=campaign['is_active']
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
    categories: List[str] = Query([], description="Selected categories"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=500, description="Records per page")
):
    """Get admin campaign dashboard with detailed call records including all stages (grouped by 2-minute sessions)."""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Admin can see all statuses, no ownership check needed
        access_query = """
            SELECT ccm.id, ccm.client_id, c.name as client_name,
                   ca.name as campaign_name, m.name as model_name, ccm.is_active,
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
        
        # Default to today if no filters
        has_any_filter = any([search, list_id, start_date, end_date, categories])
        if not has_any_filter:
            today = date.today()
            start_date = today.strftime('%Y-%m-%d')
            end_date = today.strftime('%Y-%m-%d')
        
        # Build query
        where_clauses = ["c.client_campaign_model_id = $1"]
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
        
        if categories:
            reverse_mapping = {}
            for orig, combined in ADMIN_CATEGORY_MAPPING.items():
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
        
        calls_query = f"""
            SELECT c.id, c.number, c.list_id, c.timestamp, c.stage, c.transferred,
                   c.transcription, rc.name as category_name, rc.color as category_color,
                   v.name as voice_name
            FROM calls c
            LEFT JOIN response_categories rc ON c.response_category_id = rc.id
            LEFT JOIN voices v ON c.voice_id = v.id
            WHERE {' AND '.join(where_clauses)}
            ORDER BY c.number, c.timestamp, c.stage
        """
        all_calls = await conn.fetch(calls_query, *params)
        
        # Convert to list of dicts for grouping
        calls_list = [dict(call) for call in all_calls]
        
        # Group calls into sessions (2-minute window)
        call_sessions = group_calls_by_session(calls_list, duration_minutes=2)
        
        # Pagination on sessions
        total_calls = len(call_sessions)
        total_pages = (total_calls + page_size - 1) // page_size if total_calls > 0 else 1
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_sessions = call_sessions[start_idx:end_idx]
        paginated_sessions.sort(key=lambda session: max(call['timestamp'] for call in session), reverse=True)

        
        # Get categories (from latest call in each session)
        all_categories_query = "SELECT id, name, color FROM response_categories ORDER BY name"
        db_categories = await conn.fetch(all_categories_query)
        
        category_counts_raw = {}
        for session in call_sessions:
            latest_call = sorted(session, key=lambda x: x['stage'] or 0)[-1]
            if latest_call['category_name']:
                cat_name = latest_call['category_name']
                if cat_name not in category_counts_raw:
                    category_counts_raw[cat_name] = {
                        'name': cat_name,
                        'color': latest_call['category_color'] or '#6B7280',
                        'count': 0
                    }
                category_counts_raw[cat_name]['count'] += 1
        
        combined_counts = {}
        category_colors = {}
        
        for db_cat in db_categories:
            original_name = db_cat['name'] or 'UNKNOWN'
            combined_name = ADMIN_CATEGORY_MAPPING.get(original_name, original_name)
            
            if combined_name not in combined_counts:
                combined_counts[combined_name] = 0
                category_colors[combined_name] = db_cat['color'] or '#6B7280'
        
        for cat_data in category_counts_raw.values():
            original_name = cat_data['name']
            combined_name = ADMIN_CATEGORY_MAPPING.get(original_name, original_name)
            combined_counts[combined_name] += cat_data['count']
            if not category_colors.get(combined_name):
                category_colors[combined_name] = cat_data['color']
        
        all_categories = []
        for combined_name in sorted(combined_counts.keys()):
            all_categories.append(CategoryCount(
                name=combined_name,
                color=category_colors.get(combined_name, '#6B7280'),
                count=combined_counts[combined_name],
                original_name=combined_name
            ))
        
        # Format detailed calls
        calls_data = []
        for session in paginated_sessions:
            # Sort stages by stage number
            stages_sorted = sorted(session, key=lambda x: x['stage'] or 0)
            latest_call = stages_sorted[-1]
            first_call = stages_sorted[0]
            
            original_category = latest_call['category_name'] or 'Unknown'
            combined_category = ADMIN_CATEGORY_MAPPING.get(original_category, original_category)
            
            stage_details = []
            for stage_call in stages_sorted:
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
                is_active=campaign['is_active']
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