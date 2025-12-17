from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime
import httpx
import asyncio
import re
from core.dependencies import require_roles, get_current_user_id
from database.db import get_db

router = APIRouter(prefix="/recordings", tags=["Recordings"])


class Recording(BaseModel):
    time: str
    phone_number: str
    duration: str
    size: str
    file_url: str
    server_id: int
    server_name: str
    extension: int


class PaginationInfo(BaseModel):
    page: int
    page_size: int
    total_records: int
    total_pages: int
    has_next: bool
    has_prev: bool


class RecordingsResponse(BaseModel):
    recordings: List[Recording]
    pagination: PaginationInfo
    total_servers_queried: int
    servers_with_data: int


def parse_duration_to_seconds(duration_str: str) -> int:
    """Convert duration string (MM:SS or HH:MM:SS) to seconds for sorting."""
    if not duration_str or duration_str == 'N/A':
        return 0
    
    parts = duration_str.split(':')
    try:
        if len(parts) == 2:  # MM:SS
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 3:  # HH:MM:SS
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except (ValueError, IndexError):
        return 0
    
    return 0


def parse_size_to_bytes(size_str: str) -> int:
    """Convert size string (e.g., '1.5 MB') to bytes for sorting."""
    if not size_str or size_str == 'N/A':
        return 0
    
    units = {
        'B': 1,
        'KB': 1024,
        'MB': 1024 * 1024,
        'GB': 1024 * 1024 * 1024
    }
    
    match = re.match(r'^([\d.]+)\s*([A-Z]+)$', size_str, re.IGNORECASE)
    if match:
        try:
            value = float(match.group(1))
            unit = match.group(2).upper()
            return int(value * units.get(unit, 1))
        except (ValueError, KeyError):
            return 0
    
    return 0


def normalize_date_format(date_str: str) -> str:
    """Convert date to YYYYMMDD format if needed."""
    if not date_str:
        return ""
    
    # Remove any separators
    date_str = date_str.replace('-', '').replace('/', '').strip()
    
    # If it's already 8 digits (YYYYMMDD), return as is
    if len(date_str) == 8 and date_str.isdigit():
        return date_str
    
    return date_str


async def fetch_recordings_from_server(
    client: httpx.AsyncClient,
    server_id: int,
    server_name: str,
    domain: str,
    extension: str,
    date: str,
    number: Optional[str] = None
) -> List[Dict]:
    """Fetch recordings from a single server/extension combination."""
    try:
        # Ensure domain ends with /
        if not domain.endswith('/'):
            domain += '/'
        
        # Construct API URL
        api_url = f"{domain}server_api/fetch_recording.php"
        
        # Build request parameters
        params = {
            'date': date,
            'extension': extension
        }
        if number:
            params['number'] = number
        
        # Make request
        response = await client.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Convert to list if needed
        if isinstance(data, dict):
            recordings = list(data.values())
        elif isinstance(data, list):
            recordings = data
        else:
            recordings = []
        
        # Normalize and add server info
        normalized_recordings = []
        for rec in recordings:
            normalized_recordings.append({
                'time': rec.get('time', 'N/A'),
                'phone_number': rec.get('phone_number') or rec.get('number', 'N/A'),
                'duration': rec.get('duration', 'N/A'),
                'size': rec.get('size', 'N/A'),
                'file_url': rec.get('file_url') or rec.get('url', ''),
                'server_id': server_id,
                'server_name': server_name,
                'extension': extension
            })
        
        return normalized_recordings
    
    except Exception as e:
        # Log error but don't fail entire request
        print(f"Error fetching from {server_name} (ext: {extension}): {str(e)}")
        return []


@router.get("/campaign/{campaign_id}", response_model=RecordingsResponse)
async def fetch_campaign_recordings(
    campaign_id: int,
    user_id: int = Depends(get_current_user_id),
    date: str = Query(..., description="Date in YYYYMMDD or YYYY-MM-DD format"),
    number: Optional[str] = Query(None, description="Phone number to filter recordings"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=500, description="Records per page"),
    sort_by: str = Query("time", description="Column to sort by: time, phone, duration, size"),
    sort_dir: str = Query("desc", description="Sort direction: asc, desc")
):
    """
    Fetch recordings for a campaign from ALL associated servers/extensions.
    Automatically queries all servers and extensions linked to the campaign via ServerCampaignBots.
    Aggregates results from all servers and returns paginated results.
    
    Allowed roles: admin, onboarding, qa, client (must own campaign)
    """
    pool = await get_db()
    
    # Normalize date format to YYYYMMDD
    date = normalize_date_format(date)
    
    # Validate date format
    if not (len(date) == 8 and date.isdigit()):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Expected YYYYMMDD or YYYY-MM-DD"
        )
    
    # Validate sort parameters
    valid_sort_columns = ['time', 'phone', 'duration', 'size']
    if sort_by not in valid_sort_columns:
        sort_by = 'time'
    
    if sort_dir not in ['asc', 'desc']:
        sort_dir = 'desc'
    
    async with pool.acquire() as conn:
        # Verify campaign exists and user has access
        campaign_query = """
            SELECT ccm.id, ccm.client_id, c.name as client_name
            FROM client_campaign_model ccm
            JOIN clients c ON ccm.client_id = c.client_id
            WHERE ccm.id = $1 AND ccm.is_enabled = true
        """
        campaign = await conn.fetchrow(campaign_query, campaign_id)
        
        if not campaign:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Campaign not found"
            )
        
        # Check user has access to this campaign
        user_role_query = "SELECT r.name FROM users u JOIN roles r ON u.role_id = r.id WHERE u.id = $1"
        user_role_row = await conn.fetchrow(user_role_query, user_id)
        user_role = user_role_row['name']
        
        if user_role not in ['admin', 'onboarding', 'qa'] and campaign['client_id'] != user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied to this campaign"
            )
        
        # Get all servers and extensions associated with this campaign
        servers_query = """
            SELECT 
                scb.server_id,
                COALESCE(srv.alias, srv.ip) as server_name,
                srv.domain,
                ext.extension_number as extension
            FROM server_campaign_bots scb
            JOIN servers srv ON scb.server_id = srv.id
            JOIN extensions ext ON scb.extension_id = ext.id
            WHERE scb.client_campaign_model_id = $1 AND scb.bot_count > 0
        """
        server_extensions = await conn.fetch(servers_query, campaign_id)
        
        if not server_extensions:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No active servers/extensions found for this campaign"
            )
        
        # Fetch recordings from all servers in parallel
        all_recordings = []
        total_servers = len(server_extensions)
        servers_with_data = 0
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Create tasks for all server queries
            tasks = []
            for server_ext in server_extensions:
                if not server_ext['domain']:
                    print(f"Warning: Server {server_ext['server_name']} has no domain configured")
                    continue
                
                task = fetch_recordings_from_server(
                    client=client,
                    server_id=server_ext['server_id'],
                    server_name=server_ext['server_name'],
                    domain=server_ext['domain'],
                    extension=server_ext['extension'],
                    date=date,
                    number=number
                )
                tasks.append(task)
            
            # Execute all queries in parallel
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Collect all recordings
            for result in results:
                if isinstance(result, list):
                    if result:  # If server returned data
                        servers_with_data += 1
                    all_recordings.extend(result)
        
        # Get total count before sorting/pagination
        total_count = len(all_recordings)
        
        # Sort recordings
        if sort_by == 'time':
            all_recordings.sort(
                key=lambda x: x['time'],
                reverse=(sort_dir == 'desc')
            )
        elif sort_by == 'phone':
            all_recordings.sort(
                key=lambda x: x['phone_number'],
                reverse=(sort_dir == 'desc')
            )
        elif sort_by == 'duration':
            all_recordings.sort(
                key=lambda x: parse_duration_to_seconds(x['duration']),
                reverse=(sort_dir == 'desc')
            )
        elif sort_by == 'size':
            all_recordings.sort(
                key=lambda x: parse_size_to_bytes(x['size']),
                reverse=(sort_dir == 'desc')
            )
        
        # Calculate pagination
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        
        # Get page slice
        paginated_recordings = all_recordings[start_idx:end_idx]
        
        # Convert to Pydantic models
        recording_models = [Recording(**rec) for rec in paginated_recordings]
        
        return RecordingsResponse(
            recordings=recording_models,
            pagination=PaginationInfo(
                page=page,
                page_size=page_size,
                total_records=total_count,
                total_pages=total_pages,
                has_next=page < total_pages,
                has_prev=page > 1
            ),
            total_servers_queried=total_servers,
            servers_with_data=servers_with_data
        )