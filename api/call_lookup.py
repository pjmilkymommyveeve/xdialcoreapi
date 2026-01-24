from fastapi import APIRouter, Depends, HTTPException, status, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Optional, Dict
from datetime import datetime, time
import csv
from typing import Optional
import io

from core.dependencies import require_roles
from database.db import get_db
from utils.call import group_calls_by_session

router = APIRouter(prefix="/campaigns/call-lookup", tags=["Call Lookup"])

# ============== MODELS ==============

class CallStageData(BaseModel):
    stage: Optional[int]
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
    first_timestamp: datetime
    last_timestamp: datetime

class CallLookupResponse(BaseModel):
    total_numbers_searched: int
    numbers_found: int
    numbers_not_found: int
    results: List[CallLookupResult]
    not_found_numbers: List[str]
    filters_applied: Dict[str, Optional[str]]

# ============== HELPER FUNCTIONS ==============

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
        ORDER BY c.number, c.timestamp, c.stage
    """
    
    rows = await conn.fetch(query, *params)
    
    # Convert to list of dicts
    calls_list = [dict(row) for row in rows]
    
    # Group calls into sessions (2-minute window)
    call_sessions = group_calls_by_session(calls_list, duration_minutes=2)
    
    # Build results from sessions
    results = []
    found_numbers = set()
    
    for session in call_sessions:
        if not session:
            continue
            
        # All calls in a session have the same number
        number = session[0]['number']
        found_numbers.add(number)
        
        # Sort stages within the session
        session_sorted = sorted(session, key=lambda x: x['stage'] or 0)
        
        # Get session metadata
        first_call = min(session, key=lambda x: x['timestamp'])
        last_call = max(session, key=lambda x: x['timestamp'])
        
        # Build stage data
        stages = []
        for call in session_sorted:
            stages.append(CallStageData(
                stage=call['stage'],
                transcription=call['transcription'],
                response_category=call['response_category'],
                voice_name=call['voice_name'],
                transferred=call['transferred'],
                timestamp=call['timestamp']
            ))
        
        # Get final stage data
        final_stage = session_sorted[-1]
        
        results.append(CallLookupResult(
            number=number,
            campaign_id=session[0]['client_campaign_model_id'],
            campaign_name=session[0]['campaign_name'],
            model_name=session[0]['model_name'],
            client_name=session[0]['client_name'],
            stages=stages,
            final_response_category=final_stage['response_category'],
            final_decision_transferred=final_stage['transferred'],
            total_stages=len(stages),
            first_timestamp=first_call['timestamp'],
            last_timestamp=last_call['timestamp']
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
        'First Timestamp',
        'Last Timestamp',
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
                result.first_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                result.last_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
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

# ============== ENDPOINTS ==============

@router.post("/json", response_model=CallLookupResponse)
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
    
    Calls are automatically grouped into sessions using a 2-minute window.
    Multiple calls to the same number are grouped if they occur within 2 minutes of each other.
    
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

@router.post("/csv")
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
    
    Calls are automatically grouped into sessions using a 2-minute window.
    Multiple calls to the same number are grouped if they occur within 2 minutes of each other.
    
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