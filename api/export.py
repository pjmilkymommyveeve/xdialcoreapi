from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, time, timedelta
from io import StringIO
import csv
from core.dependencies import require_roles
from database.db import get_db


router = APIRouter(prefix="/export", tags=["Data Export"])


# ============== CATEGORY MAPPING ==============

CATEGORY_MAPPING = {
    "spanishanswermachine": "Answering Machine",
    "answermachine": "Answering Machine",

    "dnc": "DNC",
    "dnq": "Do Not Qualify",
    "honeypot": "Honeypot",

    "unknown": "Unclear Response",

    "busy": "Busy",
    "already": "Not Interested",
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

# ============== MODELS ==============

class CategoryInfo(BaseModel):
    name: str
    color: str
    count: int
    original_name: str


class ExportOptionsResponse(BaseModel):
    client_name: str
    campaign_id: int
    campaign_name: str
    model_name: str
    list_ids: List[str]
    all_categories: List[CategoryInfo]
    total_records: int


class ExportRequest(BaseModel):
    list_ids: List[str] = []
    categories: List[str] = []
    start_date: Optional[str] = None
    start_time: Optional[str] = None
    end_date: Optional[str] = None
    end_time: Optional[str] = None


# ============== HELPER FUNCTIONS ==============
def group_calls_by_session(calls: List[dict]) -> List[dict]:
    """
    Group calls by number and 2-minute sessions, returning latest stage per session.
    Calls are considered part of the same session if they're within 2 minutes of each other.
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
            within_window = time_diff <= timedelta(minutes=2)
            
            if same_number and within_window:
                current_session.append(call)
            else:
                # Start new session
                sessions.append(current_session)
                current_session = [call]
    
    # Add last session
    if current_session:
        sessions.append(current_session)
    
    # Get latest stage from each session (by sorting by stage number)
    latest_calls = []
    for session in sessions:
        # Sort by stage to get the latest
        session_sorted = sorted(session, key=lambda x: x['stage'] or 0)
        latest_calls.append(session_sorted[-1])  # Get the last one (highest stage)
    
    return latest_calls



async def get_user_client_id(conn, user_id: int, roles: List[str]) -> Optional[int]:
    """
    Get the client_id that the user has access to.
    For 'client' role: returns user_id as client_id
    For privileged roles: returns None (can access any)
    Note: client_member role is NOT allowed to export
    """
    PRIVILEGED_ROLES = ['admin', 'onboarding', 'qa']
    
    if any(role in PRIVILEGED_ROLES for role in roles):
        return None
    elif 'client' in roles:
        return user_id
    return None


async def verify_export_access(conn, campaign_id: int, user_id: int, roles: List[str]) -> dict:
    """Verify user has access to export campaign data."""
    campaign_query = """
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
    campaign = await conn.fetchrow(campaign_query, campaign_id)
    
    if not campaign:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Campaign not found"
        )
    
    # Only Enabled and Disabled campaigns can be exported
    if campaign['current_status'] not in ['Enabled', 'Disabled', 'Testing']:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Campaign is not accessible for export"
        )
    
    # Check ownership for non-privileged users
    allowed_client_id = await get_user_client_id(conn, user_id, roles)
    
    # If allowed_client_id is None, user is privileged (can access any)
    if allowed_client_id is not None and campaign['client_id'] != allowed_client_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied"
        )
    
    return dict(campaign)


# ============== ENDPOINTS ==============

@router.get("/{campaign_id}/options", response_model=ExportOptionsResponse)
async def get_export_options(
    campaign_id: int,
    list_ids: Optional[str] = None,  # Comma-separated list IDs
    start_date: Optional[str] = None,
    start_time: Optional[str] = None,
    end_date: Optional[str] = None,
    end_time: Optional[str] = None,
    user_info: Dict = Depends(require_roles(['admin', 'onboarding', 'client']))
):
    """
    Get available filters and counts for export with optional pre-filtering.
    Query parameters:
    - list_ids: Comma-separated list of list IDs (e.g., "101,102,103")
    - start_date: Start date in YYYY-MM-DD format
    - start_time: Start time in HH:MM format
    - end_date: End date in YYYY-MM-DD format
    - end_time: End time in HH:MM format
    
    Privileged roles: Can export any campaign
    Client role: Can only export their own campaigns
    Client member role: NOT allowed (not in require_roles)
    """
    user_id = user_info['user_id']
    roles = user_info['roles']
    
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Verify access
        campaign = await verify_export_access(conn, campaign_id, user_id, roles)
        
        # Get unique list IDs (always show all available lists)
        list_ids_query = """
            SELECT DISTINCT list_id
            FROM calls
            WHERE client_campaign_model_id = $1
              AND list_id IS NOT NULL
              AND list_id != ''
            ORDER BY list_id
        """
        list_ids_rows = await conn.fetch(list_ids_query, campaign_id)
        all_list_ids = [row['list_id'] for row in list_ids_rows]
        
        # Build query with filters for call counts
        where_clauses = ["c.client_campaign_model_id = $1"]
        params = [campaign_id]
        param_count = 1
        
        # Parse and apply list_ids filter
        if list_ids:
            selected_lists = [lid.strip() for lid in list_ids.split(',') if lid.strip()]
            if selected_lists:
                param_count += 1
                where_clauses.append(f"c.list_id = ANY(${param_count})")
                params.append(selected_lists)
        
        # Apply date/time filters
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
        
        # Get filtered calls
        filtered_calls_query = f"""
            SELECT c.number, c.stage, c.timestamp, rc.name as category_name, rc.color as category_color
            FROM calls c
            LEFT JOIN response_categories rc ON c.response_category_id = rc.id
            WHERE {' AND '.join(where_clauses)}
            ORDER BY c.number, c.timestamp
        """
        filtered_calls = await conn.fetch(filtered_calls_query, *params)
        
        # Convert to list of dicts for processing
        calls_list = [dict(call) for call in filtered_calls]
        
        # Group by 2-minute sessions and get latest stage per session
        latest_stage_calls = group_calls_by_session(calls_list)
        
        # Get all categories from database that are in the mapping
        categories_query = "SELECT id, name, color FROM response_categories ORDER BY name"
        db_categories = await conn.fetch(categories_query)
        
        # Filter to only mapped categories
        mapped_categories = []
        for db_cat in db_categories:
            original_name = db_cat['name'] or 'UNKNOWN'
            if original_name in CATEGORY_MAPPING:
                mapped_categories.append(db_cat)
        
        # Count categories from filtered calls
        category_counts_raw = {}
        for call in latest_stage_calls:
            if call['category_name']:
                cat_name = call['category_name']
                # Only count if in mapping
                if cat_name in CATEGORY_MAPPING:
                    if cat_name not in category_counts_raw:
                        category_counts_raw[cat_name] = {
                            'name': cat_name,
                            'color': call['category_color'] or '#6B7280',
                            'count': 0
                        }
                    category_counts_raw[cat_name]['count'] += 1
        
        # Initialize combined counts and colors
        combined_counts = {}
        category_colors = {}
        
        # Initialize all mapped categories with 0 counts
        for db_cat in mapped_categories:
            original_name = db_cat['name'] or 'UNKNOWN'
            combined_name = CATEGORY_MAPPING[original_name]
            
            if combined_name not in combined_counts:
                combined_counts[combined_name] = 0
                category_colors[combined_name] = db_cat['color'] or '#6B7280'
        
        # Add actual counts from filtered calls
        for cat_data in category_counts_raw.values():
            original_name = cat_data['name']
            combined_name = CATEGORY_MAPPING[original_name]
            combined_counts[combined_name] += cat_data['count']
            if not category_colors.get(combined_name):
                category_colors[combined_name] = cat_data['color']
        
        # Build categories list
        all_categories = []
        for combined_name in sorted(combined_counts.keys()):
            all_categories.append(CategoryInfo(
                name=combined_name,
                color=category_colors.get(combined_name, '#6B7280'),
                count=combined_counts[combined_name],
                original_name=combined_name
            ))
        
        return ExportOptionsResponse(
            client_name=campaign['client_name'],
            campaign_id=campaign['id'],
            campaign_name=campaign['campaign_name'],
            model_name=campaign['model_name'],
            list_ids=all_list_ids,  # Always return all available list IDs
            all_categories=all_categories,  # Counts based on current filters
            total_records=len(latest_stage_calls)  # Total based on current filters
        )


@router.post("/{campaign_id}/download")
async def download_export(
    campaign_id: int,
    export_request: ExportRequest,
    user_info: Dict = Depends(require_roles(['admin', 'onboarding', 'client']))
):
    """
    Generate and download CSV file.
    Privileged roles: Can export any campaign
    Client role: Can only export their own campaigns
    Client member role: NOT allowed (not in require_roles)
    """
    user_id = user_info['user_id']
    roles = user_info['roles']
    
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Verify access
        campaign = await verify_export_access(conn, campaign_id, user_id, roles)
        
        # Build query with filters
        where_clauses = ["c.client_campaign_model_id = $1"]
        params = [campaign_id]
        param_count = 1
        
        if export_request.list_ids:
            param_count += 1
            where_clauses.append(f"c.list_id = ANY(${param_count})")
            params.append(export_request.list_ids)
        
        if export_request.categories:
            # Create reverse mapping to get original category names
            reverse_mapping = {}
            for orig, combined in CATEGORY_MAPPING.items():
                if combined not in reverse_mapping:
                    reverse_mapping[combined] = []
                reverse_mapping[combined].append(orig)
            
            # Convert selected combined categories back to original names
            original_names = []
            for cat in export_request.categories:
                if cat in reverse_mapping:
                    original_names.extend(reverse_mapping[cat])
                else:
                    # Only add if it's in the mapping
                    if cat in CATEGORY_MAPPING.values():
                        original_names.append(cat)
            
            if original_names:
                param_count += 1
                where_clauses.append(f"rc.name = ANY(${param_count})")
                params.append(original_names)
        
        if export_request.start_date:
            try:
                start_dt = datetime.strptime(export_request.start_date, '%Y-%m-%d')
                if export_request.start_time:
                    time_obj = datetime.strptime(export_request.start_time, '%H:%M').time()
                    start_dt = datetime.combine(start_dt.date(), time_obj)
                param_count += 1
                where_clauses.append(f"c.timestamp >= ${param_count}")
                params.append(start_dt)
            except ValueError:
                pass
        
        if export_request.end_date:
            try:
                end_dt = datetime.strptime(export_request.end_date, '%Y-%m-%d')
                if export_request.end_time:
                    time_obj = datetime.strptime(export_request.end_time, '%H:%M').time()
                    end_dt = datetime.combine(end_dt.date(), time_obj)
                else:
                    end_dt = datetime.combine(end_dt.date(), time(23, 59, 59))
                param_count += 1
                where_clauses.append(f"c.timestamp <= ${param_count}")
                params.append(end_dt)
            except ValueError:
                pass
        
        # Fetch calls
        calls_query = f"""
            SELECT c.id, c.number, c.list_id, c.timestamp, c.stage,
                   c.transferred, c.transcription,
                   rc.name as category_name, v.name as voice_name
            FROM calls c
            LEFT JOIN response_categories rc ON c.response_category_id = rc.id
            LEFT JOIN voices v ON c.voice_id = v.id
            WHERE {' AND '.join(where_clauses)}
            ORDER BY c.number, c.timestamp
        """
        all_calls = await conn.fetch(calls_query, *params)
        
        # Convert to list of dicts
        calls_list = [dict(call) for call in all_calls]
        
        # Group by 2-minute sessions and get latest stage per session
        filtered_calls = group_calls_by_session(calls_list)
        
        # Filter out categories not in mapping
        filtered_calls = [
            call for call in filtered_calls 
            if not call['category_name'] or call['category_name'] in CATEGORY_MAPPING
        ]
        
        # Sort by timestamp descending for export
        filtered_calls.sort(key=lambda x: x['timestamp'], reverse=True)
        
        # Create CSV in memory
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow([
            'Call ID', 'Phone Number', 'List ID', 'Category', 'Timestamp',
            'Transferred', 'Stage'
        ])
        
        for call in filtered_calls:
            original_category = call['category_name'] or 'Unknown'
            combined_category = CATEGORY_MAPPING.get(original_category, original_category)
            
            writer.writerow([
                call['id'],
                call['number'],
                call['list_id'] or '',
                combined_category,
                call['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'Yes' if call['transferred'] else 'No',
                call['stage'] or 0
            ])
        
        # Prepare response
        output.seek(0)
        filename = f"call_data_{campaign['campaign_name']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )