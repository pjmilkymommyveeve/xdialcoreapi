from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime
import httpx
import asyncio
import re
from core.dependencies import require_roles
from database.db import get_db


router = APIRouter(prefix="/recordings", tags=["Recordings"])


# ============== MODELS ==============

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


# ============== HELPER FUNCTIONS ==============

def parse_duration_to_seconds(duration_str: str) -> int:
    """Convert duration string (MM:SS or HH:MM:SS) to seconds for sorting."""
    if not duration_str or duration_str == 'N/A':
        return 0
    parts = duration_str.split(':')
    try:
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 3:
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
    date_str = date_str.replace('-', '').replace('/', '').strip()
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
        if not domain.endswith('/'):
            domain += '/'
        
        api_url = f"{domain}server_api/fetch_recording.php"
        params = {'date': date, 'extension': extension}
        if number:
            params['number'] = number
        
        response = await client.get(api_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, dict):
            recordings = list(data.values())
        elif isinstance(data, list):
            recordings = data
        else:
            recordings = []
        
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
        print(f"Error fetching from {server_name} (ext: {extension}): {str(e)}")
        return []


async def get_user_client_id(conn, user_id: int, roles: List[str]) -> Optional[int]:
    """
    Get the client_id that the user has access to.
    For 'client' role: returns user_id as client_id
    For 'client_member' role: returns employer's client_id
    For privileged roles: returns None (can access any)
    """
    PRIVILEGED_ROLES = ['admin', 'onboarding', 'qa']
    
    if any(role in PRIVILEGED_ROLES for role in roles):
        return None
    elif 'client' in roles:
        return user_id
    elif 'client_member' in roles:
        employer_query = "SELECT client_id FROM client_employees WHERE user_id = $1"
        employer = await conn.fetchrow(employer_query, user_id)
        if employer:
            return employer['client_id']
    return None


async def verify_campaign_access(conn, campaign_id: int, user_id: int, roles: List[str]) -> dict:
    """Verify user has access to campaign and return campaign data."""
    PRIVILEGED_ROLES = ['admin', 'onboarding', 'qa']
    
    campaign_query = """
        SELECT ccm.id, ccm.client_id, c.name as client_name,
               s.status_name as current_status
        FROM client_campaign_model ccm
        JOIN clients c ON ccm.client_id = c.client_id
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
    
    # Check status based on roles
    is_privileged = any(role in PRIVILEGED_ROLES for role in roles)
    allowed_statuses = ['Enabled', 'Disabled'] if is_privileged else ['Enabled', 'Disabled']
    
    if campaign['current_status'] not in allowed_statuses:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Campaign is not accessible"
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


# ============== ENDPOINT ==============

@router.get("/campaign/{campaign_id}", response_model=RecordingsResponse)
async def fetch_campaign_recordings(
    campaign_id: int,
    user_info: Dict = Depends(require_roles(['admin', 'onboarding', 'qa', 'client', 'client_member'])),
    date: str = Query(..., description="Date in YYYYMMDD or YYYY-MM-DD format"),
    number: Optional[str] = Query(None, description="Phone number to filter recordings"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(100, ge=1, le=500, description="Records per page"),
    sort_by: str = Query("time", description="Column to sort by: time, phone, duration, size"),
    sort_dir: str = Query("desc", description="Sort direction: asc, desc")
):
    """
    Fetch recordings for a campaign from all associated servers/extensions.
    Privileged roles: Can access any campaign's recordings
    Client role: Can only access their own campaigns' recordings
    Client member role: Can only access their employer's campaigns' recordings
    """
    user_id = user_info['user_id']
    roles = user_info['roles']
    
    pool = await get_db()
    
    # Normalize and validate date
    date = normalize_date_format(date)
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
        # Verify campaign access
        campaign = await verify_campaign_access(conn, campaign_id, user_id, roles)
        
        # Get all servers and extensions for this campaign
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
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, list):
                    if result:
                        servers_with_data += 1
                    all_recordings.extend(result)
        
        total_count = len(all_recordings)
        
        # Sort recordings
        if sort_by == 'time':
            all_recordings.sort(key=lambda x: x['time'], reverse=(sort_dir == 'desc'))
        elif sort_by == 'phone':
            all_recordings.sort(key=lambda x: x['phone_number'], reverse=(sort_dir == 'desc'))
        elif sort_by == 'duration':
            all_recordings.sort(key=lambda x: parse_duration_to_seconds(x['duration']), reverse=(sort_dir == 'desc'))
        elif sort_by == 'size':
            all_recordings.sort(key=lambda x: parse_size_to_bytes(x['size']), reverse=(sort_dir == 'desc'))
        
        # Pagination
        total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_recordings = all_recordings[start_idx:end_idx]
        
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