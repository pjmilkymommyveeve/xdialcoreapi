from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, date, time
from core.dependencies import get_current_user_id, require_roles
from database.db import get_db


router = APIRouter(prefix="/campaigns", tags=["Campaigns"])


# Client category mapping 
CLIENT_CATEGORY_MAPPING = {
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
    "repeatpitch": "Repeat Pitch"
}


# Admin category mapping (same for now, but separate)
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
    "interested": "Interested"
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


class CategoryCount(BaseModel):
    name: str
    color: str
    count: int
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


# ============== CLIENT ENDPOINT ==============

@router.get("/{campaign_id}/dashboard", response_model=CampaignDashboardResponse)
async def get_client_campaign(
    campaign_id: int,
    user_id: int = Depends(get_current_user_id),
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
    """GET CLIENT CAMPAIGN DASHBOARD - Show call records with filters (latest stage only)"""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # verify user has access to this campaign
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
        
        # Check if campaign is in valid status (Enabled)
        if campaign['current_status'] not in ['Enabled']:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Campaign is not enabled"
            )
        
        # check user owns this campaign (unless admin/onboarding)
        user_role_query = "SELECT r.name FROM users u JOIN roles r ON u.role_id = r.id WHERE u.id = $1"
        user_role_row = await conn.fetchrow(user_role_query, user_id)
        user_role = user_role_row['name']
        
        if user_role not in ['admin', 'onboarding'] and campaign['client_id'] != user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # default to today if no filters
        has_any_filter = any([search, list_id, start_date, end_date, categories])
        if not has_any_filter:
            today = date.today()
            start_date = today.strftime('%Y-%m-%d')
            end_date = today.strftime('%Y-%m-%d')
        
        # get latest stage for each number
        latest_stages_query = """
            SELECT number, MAX(stage) as max_stage
            FROM calls
            WHERE client_campaign_model_id = $1
            GROUP BY number
        """
        latest_stages_rows = await conn.fetch(latest_stages_query, campaign_id)
        latest_stages = {row['number']: row['max_stage'] for row in latest_stages_rows}
        
        # build base query for calls
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
        
        # handle category filter with mapping
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
        
        # fetch all calls matching filters
        calls_query = f"""
            SELECT c.id, c.number, c.list_id, c.timestamp, c.stage,
                   c.transcription, rc.name as category_name, rc.color as category_color,
                   v.name as voice_name
            FROM calls c
            LEFT JOIN response_categories rc ON c.response_category_id = rc.id
            LEFT JOIN voices v ON c.voice_id = v.id
            WHERE {' AND '.join(where_clauses)}
            ORDER BY c.timestamp DESC
        """
        all_calls = await conn.fetch(calls_query, *params)
        
        # filter to latest stage only
        filtered_calls = []
        for call in all_calls:
            if call['number'] in latest_stages and call['stage'] == latest_stages[call['number']]:
                filtered_calls.append(call)
        
        # calculate pagination
        total_calls = len(filtered_calls)
        total_pages = (total_calls + page_size - 1) // page_size if total_calls > 0 else 1
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_calls = filtered_calls[start_idx:end_idx]
        
        # get all categories with counts
        all_categories_query = """
            SELECT id, name, color FROM response_categories ORDER BY name
        """
        db_categories = await conn.fetch(all_categories_query)
        
        # count categories from filtered calls
        category_counts_raw = {}
        for call in filtered_calls:
            if call['category_name']:
                cat_name = call['category_name']
                if cat_name not in category_counts_raw:
                    category_counts_raw[cat_name] = {
                        'name': cat_name,
                        'color': call['category_color'] or '#6B7280',
                        'count': 0
                    }
                category_counts_raw[cat_name]['count'] += 1
        
        # combine categories according to mapping
        combined_counts = {}
        category_colors = {}
        
        for db_cat in db_categories:
            original_name = db_cat['name'] or 'UNKNOWN'
            combined_name = CLIENT_CATEGORY_MAPPING.get(original_name, original_name)
            
            if combined_name not in combined_counts:
                combined_counts[combined_name] = 0
                category_colors[combined_name] = db_cat['color'] or '#6B7280'
        
        for cat_data in category_counts_raw.values():
            original_name = cat_data['name']
            combined_name = CLIENT_CATEGORY_MAPPING.get(original_name, original_name)
            combined_counts[combined_name] += cat_data['count']
            if not category_colors.get(combined_name):
                category_colors[combined_name] = cat_data['color']
        
        # build category list
        all_categories = []
        for combined_name in sorted(combined_counts.keys()):
            all_categories.append(CategoryCount(
                name=combined_name.capitalize(),
                color=category_colors.get(combined_name, '#6B7280'),
                count=combined_counts[combined_name],
                original_name=combined_name
            ))
        
        # format calls for response
        calls_data = []
        for call in paginated_calls:
            original_category = call['category_name'] or 'Unknown'
            combined_category = CLIENT_CATEGORY_MAPPING.get(original_category, original_category)
            
            calls_data.append(CallRecord(
                id=call['id'],
                number=call['number'],
                list_id=call['list_id'] or 'N/A',
                category=combined_category.capitalize(),
                category_color=call['category_color'] or '#6B7280',
                timestamp=call['timestamp'].strftime('%m/%d/%Y, %H:%M:%S'),
                stage=call['stage'] or 0,
                has_transcription=bool(call['transcription']),
                transcription=call['transcription'] or 'No transcript available'
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
    user_info: Dict = Depends(require_roles(["admin", "onboarding", "qa"])),
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
    """GET ADMIN CAMPAIGN DASHBOARD - Show detailed call records with all stages and transcriptions"""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # verify campaign exists (admin can see all statuses)
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
        
        # default to today if no filters
        has_any_filter = any([search, list_id, start_date, end_date, categories])
        if not has_any_filter:
            today = date.today()
            start_date = today.strftime('%Y-%m-%d')
            end_date = today.strftime('%Y-%m-%d')
        
        # build base query for calls
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
        
        # handle category filter with mapping
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
        
        # fetch all calls matching filters
        calls_query = f"""
            SELECT c.id, c.number, c.list_id, c.timestamp, c.stage, c.transferred,
                   c.transcription, rc.name as category_name, rc.color as category_color,
                   v.name as voice_name
            FROM calls c
            LEFT JOIN response_categories rc ON c.response_category_id = rc.id
            LEFT JOIN voices v ON c.voice_id = v.id
            WHERE {' AND '.join(where_clauses)}
            ORDER BY c.number, c.stage
        """
        all_calls = await conn.fetch(calls_query, *params)
        
        # group calls by number
        calls_by_number = {}
        for call in all_calls:
            number = call['number']
            if number not in calls_by_number:
                calls_by_number[number] = []
            calls_by_number[number].append(call)
        
        # calculate pagination
        total_calls = len(calls_by_number)
        total_pages = (total_calls + page_size - 1) // page_size if total_calls > 0 else 1
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_numbers = list(calls_by_number.items())[start_idx:end_idx]
        
        # get all categories with counts (based on latest stage only)
        all_categories_query = """
            SELECT id, name, color FROM response_categories ORDER BY name
        """
        db_categories = await conn.fetch(all_categories_query)
        
        # count categories from latest stage of each number
        category_counts_raw = {}
        for number, stages in calls_by_number.items():
            latest_call = stages[-1]
            if latest_call['category_name']:
                cat_name = latest_call['category_name']
                if cat_name not in category_counts_raw:
                    category_counts_raw[cat_name] = {
                        'name': cat_name,
                        'color': latest_call['category_color'] or '#6B7280',
                        'count': 0
                    }
                category_counts_raw[cat_name]['count'] += 1
        
        # combine categories according to mapping
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
        
        # build category list
        all_categories = []
        for combined_name in sorted(combined_counts.keys()):
            all_categories.append(CategoryCount(
                name=combined_name.capitalize(),
                color=category_colors.get(combined_name, '#6B7280'),
                count=combined_counts[combined_name],
                original_name=combined_name
            ))
        
        # format detailed calls for response
        calls_data = []
        for number, stages in paginated_numbers:
            latest_call = stages[-1]
            first_call = stages[0]
            
            original_category = latest_call['category_name'] or 'Unknown'
            combined_category = ADMIN_CATEGORY_MAPPING.get(original_category, original_category)
            
            # build stage details
            stage_details = []
            for stage_call in stages:
                stage_category = stage_call['category_name'] or 'Unknown'
                stage_combined = ADMIN_CATEGORY_MAPPING.get(stage_category, stage_category)
                
                stage_details.append(CallStageDetail(
                    stage=stage_call['stage'] or 0,
                    category=stage_combined.capitalize(),
                    category_color=stage_call['category_color'] or '#6B7280',
                    voice=stage_call['voice_name'] or 'Unknown',
                    timestamp=stage_call['timestamp'].strftime('%m/%d/%Y, %H:%M:%S'),
                    transcription=stage_call['transcription'] or 'No transcript available'
                ))
            
            calls_data.append(DetailedCallRecord(
                id=latest_call['id'],
                number=number,
                list_id=latest_call['list_id'] or 'N/A',
                latest_category=combined_category.capitalize(),
                latest_category_color=latest_call['category_color'] or '#6B7280',
                latest_stage=latest_call['stage'] or 0,
                first_timestamp=first_call['timestamp'].strftime('%m/%d/%Y, %H:%M:%S'),
                last_timestamp=latest_call['timestamp'].strftime('%m/%d/%Y, %H:%M:%S'),
                total_stages=len(stages),
                has_transcription=any(s['transcription'] for s in stages),
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