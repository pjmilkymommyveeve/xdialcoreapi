from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, time
from io import StringIO
import csv
from core.dependencies import require_roles
from database.db import get_db


router = APIRouter(prefix="/export", tags=["Data Export"])


# ============== CATEGORY MAPPING ==============

CATEGORY_MAPPING = {
    "dnc": "DNC",
    "honeypot_hardcoded": "Honeypot",
    "honeypot": "Honeypot",
    "spanishanswermachine": "Spanish Answering Machine",
    "answermachine": "Answering Machine",
    "already": "Not Interested",
    "notinterested": "Not Interested",
    "busy": "Busy",
    "dnq": "DNQ",
    "qualified": "Qualified",
    "neutral": "Neutral",
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
    user_info: Dict = Depends(require_roles(['admin', 'onboarding', 'client']))
):
    """
    Get available filters and counts for export.
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
        
        # Get unique list IDs
        list_ids_query = """
            SELECT DISTINCT list_id
            FROM calls
            WHERE client_campaign_model_id = $1
              AND list_id IS NOT NULL
              AND list_id != ''
            ORDER BY list_id
        """
        list_ids_rows = await conn.fetch(list_ids_query, campaign_id)
        list_ids = [row['list_id'] for row in list_ids_rows]
        
        # Get latest stage for each number
        latest_stages_query = """
            SELECT number, MAX(stage) as max_stage
            FROM calls
            WHERE client_campaign_model_id = $1
            GROUP BY number
        """
        latest_stages_rows = await conn.fetch(latest_stages_query, campaign_id)
        latest_stages = {row['number']: row['max_stage'] for row in latest_stages_rows}
        
        # Get all calls at latest stage
        all_calls_query = """
            SELECT c.number, c.stage, rc.name as category_name, rc.color as category_color
            FROM calls c
            LEFT JOIN response_categories rc ON c.response_category_id = rc.id
            WHERE c.client_campaign_model_id = $1
        """
        all_calls = await conn.fetch(all_calls_query, campaign_id)
        
        # Filter to latest stage only
        latest_stage_calls = []
        for call in all_calls:
            if call['number'] in latest_stages and call['stage'] == latest_stages[call['number']]:
                latest_stage_calls.append(call)
        
        # Get all categories from database
        categories_query = "SELECT id, name, color FROM response_categories ORDER BY name"
        db_categories = await conn.fetch(categories_query)
        
        # Count categories from filtered calls
        category_counts_raw = {}
        for call in latest_stage_calls:
            if call['category_name']:
                cat_name = call['category_name']
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
        
        # First, initialize all categories from database with 0 counts
        for db_cat in db_categories:
            original_name = db_cat['name'] or 'UNKNOWN'
            combined_name = CATEGORY_MAPPING.get(original_name, original_name)
            
            if combined_name not in combined_counts:
                combined_counts[combined_name] = 0
                category_colors[combined_name] = db_cat['color'] or '#6B7280'
        
        # Then, add actual counts from calls
        for cat_data in category_counts_raw.values():
            original_name = cat_data['name']
            combined_name = CATEGORY_MAPPING.get(original_name, original_name)
            combined_counts[combined_name] += cat_data['count']
            if not category_colors.get(combined_name):
                category_colors[combined_name] = cat_data['color']
        
        # Build categories list
        all_categories = []
        for combined_name in sorted(combined_counts.keys()):
            all_categories.append(CategoryInfo(
                name=combined_name.capitalize(),
                color=category_colors.get(combined_name, '#6B7280'),
                count=combined_counts[combined_name],
                original_name=combined_name
            ))
        
        return ExportOptionsResponse(
            client_name=campaign['client_name'],
            campaign_id=campaign['id'],
            campaign_name=campaign['campaign_name'],
            model_name=campaign['model_name'],
            list_ids=list_ids,
            all_categories=all_categories,
            total_records=len(latest_stage_calls)
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
        
        # Get latest stages
        latest_stages_query = """
            SELECT number, MAX(stage) as max_stage
            FROM calls
            WHERE client_campaign_model_id = $1
            GROUP BY number
        """
        latest_stages_rows = await conn.fetch(latest_stages_query, campaign_id)
        latest_stages = {row['number']: row['max_stage'] for row in latest_stages_rows}
        
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
            ORDER BY c.timestamp DESC
        """
        all_calls = await conn.fetch(calls_query, *params)
        
        # Filter to latest stage
        filtered_calls = []
        for call in all_calls:
            if call['number'] in latest_stages and call['stage'] == latest_stages[call['number']]:
                filtered_calls.append(call)
        
        # Create CSV in memory
        output = StringIO()
        writer = csv.writer(output)
        writer.writerow([
            'Call ID', 'Phone Number', 'List ID', 'Category', 'Timestamp',
            'Transferred', 'Stage', 'Voice', 'Transcription'
        ])
        
        for call in filtered_calls:
            original_category = call['category_name'] or 'Unknown'
            combined_category = CATEGORY_MAPPING.get(original_category, original_category)
            
            writer.writerow([
                call['id'],
                call['number'],
                call['list_id'] or '',
                combined_category.capitalize(),
                call['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),
                'Yes' if call['transferred'] else 'No',
                call['stage'] or 0,
                call['voice_name'] or 'Unknown',
                call['transcription'] or ''
            ])
        
        # Prepare response
        output.seek(0)
        filename = f"call_data_{campaign['campaign_name']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )