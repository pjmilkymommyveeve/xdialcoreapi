from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, time
from io import StringIO
import csv
from core.dependencies import get_current_user_id
from database.db import get_db


router = APIRouter(prefix="/export", tags=["Data Export"])


# category mapping
CATEGORY_MAPPING = {
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


class CategoryInfo(BaseModel):
    name: str
    combined_name: str
    count: int


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


@router.get("/{campaign_id}/options", response_model=ExportOptionsResponse)
async def get_export_options(
    campaign_id: int,
    user_id: int = Depends(get_current_user_id)
):
    """GET EXPORT OPTIONS - Get available filters and counts for export"""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # verify access to campaign
        campaign_query = """
            SELECT ccm.id, ccm.client_id, c.name as client_name,
                   ca.name as campaign_name, m.name as model_name
            FROM client_campaign_model ccm
            JOIN clients c ON ccm.client_id = c.client_id
            JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
            JOIN campaigns ca ON cm.campaign_id = ca.id
            JOIN models m ON cm.model_id = m.id
            WHERE ccm.id = $1 AND ccm.is_enabled = true
        """
        campaign = await conn.fetchrow(campaign_query, campaign_id)
        
        if not campaign:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Campaign not found"
            )
        
        # check user owns campaign (unless admin/onboarding)
        user_role_query = "SELECT r.name FROM users u JOIN roles r ON u.role_id = r.id WHERE u.id = $1"
        user_role_row = await conn.fetchrow(user_role_query, user_id)
        user_role = user_role_row['name']
        
        if user_role not in ['admin', 'onboarding'] and campaign['client_id'] != user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # get unique list IDs
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
        
        # get latest stage for each number
        latest_stages_query = """
            SELECT number, MAX(stage) as max_stage
            FROM calls
            WHERE client_campaign_model_id = $1
            GROUP BY number
        """
        latest_stages_rows = await conn.fetch(latest_stages_query, campaign_id)
        latest_stages = {row['number']: row['max_stage'] for row in latest_stages_rows}
        
        # get all calls at latest stage
        all_calls_query = """
            SELECT c.number, c.stage, rc.name as category_name
            FROM calls c
            LEFT JOIN response_categories rc ON c.response_category_id = rc.id
            WHERE c.client_campaign_model_id = $1
        """
        all_calls = await conn.fetch(all_calls_query, campaign_id)
        
        # filter to latest stage only
        latest_stage_calls = []
        for call in all_calls:
            if call['number'] in latest_stages and call['stage'] == latest_stages[call['number']]:
                latest_stage_calls.append(call)
        
        # get all categories from database
        categories_query = "SELECT name FROM response_categories ORDER BY name"
        db_categories = await conn.fetch(categories_query)
        
        # count categories
        category_count_dict = {}
        for call in latest_stage_calls:
            if call['category_name']:
                cat_name = call['category_name']
                category_count_dict[cat_name] = category_count_dict.get(cat_name, 0) + 1
        
        # combine categories
        combined_counts = {}
        for db_cat in db_categories:
            original_name = db_cat['name'] or 'UNKNOWN'
            combined_name = CATEGORY_MAPPING.get(original_name, original_name)
            count = category_count_dict.get(original_name, 0)
            
            if combined_name in combined_counts:
                combined_counts[combined_name] += count
            else:
                combined_counts[combined_name] = count
        
        # build categories list
        all_categories = []
        for combined_name in sorted(combined_counts.keys()):
            all_categories.append(CategoryInfo(
                name=combined_name.capitalize(),
                combined_name=combined_name,
                count=combined_counts[combined_name]
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
    user_id: int = Depends(get_current_user_id)
):
    """POST DOWNLOAD EXPORT - Generate and download CSV file"""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # verify access
        campaign_query = """
            SELECT ccm.id, ccm.client_id, ca.name as campaign_name
            FROM client_campaign_model ccm
            JOIN campaign_model cm ON ccm.campaign_model_id = cm.id
            JOIN campaigns ca ON cm.campaign_id = ca.id
            WHERE ccm.id = $1 AND ccm.is_enabled = true
        """
        campaign = await conn.fetchrow(campaign_query, campaign_id)
        
        if not campaign:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Campaign not found"
            )
        
        # check access
        user_role_query = "SELECT r.name FROM users u JOIN roles r ON u.role_id = r.id WHERE u.id = $1"
        user_role_row = await conn.fetchrow(user_role_query, user_id)
        user_role = user_role_row['name']
        
        if user_role not in ['admin', 'onboarding'] and campaign['client_id'] != user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        # get latest stages
        latest_stages_query = """
            SELECT number, MAX(stage) as max_stage
            FROM calls
            WHERE client_campaign_model_id = $1
            GROUP BY number
        """
        latest_stages_rows = await conn.fetch(latest_stages_query, campaign_id)
        latest_stages = {row['number']: row['max_stage'] for row in latest_stages_rows}
        
        # build query with filters
        where_clauses = ["c.client_campaign_model_id = $1"]
        params = [campaign_id]
        param_count = 1
        
        if export_request.list_ids:
            param_count += 1
            where_clauses.append(f"c.list_id = ANY(${param_count})")
            params.append(export_request.list_ids)
        
        if export_request.categories:
            # reverse category mapping
            reverse_mapping = {}
            for orig, combined in CATEGORY_MAPPING.items():
                if combined not in reverse_mapping:
                    reverse_mapping[combined] = []
                reverse_mapping[combined].append(orig)
            
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
        
        # fetch calls
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
        
        # filter to latest stage
        filtered_calls = []
        for call in all_calls:
            if call['number'] in latest_stages and call['stage'] == latest_stages[call['number']]:
                filtered_calls.append(call)
        
        # create csv in memory
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
        
        # prepare response
        output.seek(0)
        filename = f"call_data_{campaign['campaign_name']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )