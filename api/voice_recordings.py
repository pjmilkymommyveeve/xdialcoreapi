from fastapi import APIRouter, HTTPException, status, UploadFile, File, Form
from pydantic import BaseModel, Field
from typing import List
from database.db import get_db
import os
import subprocess
from datetime import datetime

router = APIRouter(prefix="/voice-recordings", tags=["Voice Recordings"])


# ============== PYDANTIC SCHEMAS ==============

class VoiceRecordingResponse(BaseModel):
    id: int
    name: str
    campaign_model_voice_id: int
    campaign_name: str
    model_name: str
    voice_name: str


class UploadRecordingsResponse(BaseModel):
    success: bool
    uploaded_count: int
    failed_count: int
    recordings: List[VoiceRecordingResponse]
    deployment_log: str
    errors: List[str] = []


class DeleteRecordingsResponse(BaseModel):
    success: bool
    deleted_count: int
    deployment_log: str
    errors: List[str] = []


# ============== CONFIGURATION ==============

RECORDINGS_DIR = "/home/doc/work/xdialcoreapi/sounds"
DEPLOY_SCRIPT_PATH = "/home/doc/work/xdialcoreapi/scripts/deploy_recordings.sh"
DEPLOY_LOG_FILE = "/home/doc/work/xdialcoreapi/logs/deployment.log"
REMOTE_PATH = "/usr/share/asterisk/sounds"

# Ensure directories exist
os.makedirs(RECORDINGS_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DEPLOY_LOG_FILE), exist_ok=True)
os.makedirs(os.path.dirname(DEPLOY_SCRIPT_PATH), exist_ok=True)


# ============== HELPER FUNCTIONS ==============

async def get_server_ips() -> List[str]:
    """
    Fetch all server IPs from the database.
    
    Returns:
        List of server IP addresses
    """
    pool = await get_db()
    
    async with pool.acquire() as conn:
        query = "SELECT ip FROM servers WHERE ip IS NOT NULL AND ip != '' ORDER BY ip"
        servers = await conn.fetch(query)
        
        server_ips = [str(server['ip']).strip() for server in servers if server['ip']]
        
        # Filter out any empty or invalid IPs
        server_ips = [ip for ip in server_ips if ip and len(ip) > 0]
        
        return server_ips


def append_to_log(content: str):
    """
    Append content to the consolidated deployment log file.
    
    Args:
        content: Log content to append
    """
    try:
        with open(DEPLOY_LOG_FILE, 'a') as f:
            f.write(content)
            f.write("\n" + "="*80 + "\n\n")
    except Exception as e:
        print(f"Error writing to log file: {str(e)}")


def run_deployment_script(action: str, files: List[str], server_ips: List[str]) -> tuple[bool, str]:
    """
    Run the bash deployment script to sync recordings to all servers.
    
    Args:
        action: "upload" or "delete"
        files: List of filenames to deploy/delete
        server_ips: List of server IPs to deploy to
    
    Returns:
        tuple: (success: bool, log_content: str)
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        # Build command
        cmd = [
            "bash",
            DEPLOY_SCRIPT_PATH,
            action,
            ",".join(files),
            ",".join(server_ips),
            RECORDINGS_DIR  # Add source directory
        ]
        
        # Run deployment script
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout (multiple servers can take time)
        )
        
        # Build log content
        log_content = f"=== Deployment Log ({action}) ===\n"
        log_content += f"Timestamp: {timestamp}\n"
        log_content += f"Action: {action}\n"
        log_content += f"Files: {', '.join(files)}\n"
        log_content += f"Servers: {len(server_ips)} server(s)\n"
        log_content += f"\n=== STDOUT ===\n{result.stdout}\n"
        log_content += f"\n=== STDERR ===\n{result.stderr}\n"
        log_content += f"\n=== Return Code: {result.returncode} ===\n"
        
        # Append to consolidated log file
        append_to_log(log_content)
        
        success = result.returncode == 0
        return success, log_content
        
    except subprocess.TimeoutExpired:
        error_msg = f"=== Deployment Error ===\nTimestamp: {timestamp}\nError: Deployment script timed out after 10 minutes"
        append_to_log(error_msg)
        return False, error_msg
        
    except Exception as e:
        error_msg = f"=== Deployment Error ===\nTimestamp: {timestamp}\nError: {str(e)}"
        append_to_log(error_msg)
        return False, error_msg


# ============== ENDPOINTS ==============

@router.get("/campaign-model-voices/{cmv_id}", response_model=List[VoiceRecordingResponse])
async def get_voice_recordings(cmv_id: int):
    """Get all recordings for a campaign model voice assignment."""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Check if campaign model voice exists
        cmv_check = """
            SELECT cmv.id, c.name as campaign_name, m.name as model_name, v.name as voice_name
            FROM campaign_model_voice cmv
            JOIN campaign_model cm ON cmv.campaign_model_id = cm.id
            JOIN campaigns c ON cm.campaign_id = c.id
            JOIN models m ON cm.model_id = m.id
            JOIN voices v ON cmv.voice_id = v.id
            WHERE cmv.id = $1
        """
        cmv = await conn.fetchrow(cmv_check, cmv_id)
        
        if not cmv:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Campaign model voice assignment with ID {cmv_id} not found"
            )
        
        # Fetch recordings
        query = """
            SELECT id, name, campaign_model_voice_id
            FROM voice_recordings
            WHERE campaign_model_voice_id = $1
            ORDER BY name
        """
        recordings = await conn.fetch(query, cmv_id)
        
        return [
            {
                'id': r['id'],
                'name': r['name'],
                'campaign_model_voice_id': r['campaign_model_voice_id'],
                'campaign_name': cmv['campaign_name'],
                'model_name': cmv['model_name'],
                'voice_name': cmv['voice_name']
            }
            for r in recordings
        ]


@router.post("/upload", response_model=UploadRecordingsResponse)
async def upload_voice_recordings(
    campaign_model_voice_id: int = Form(...),
    files: List[UploadFile] = File(...)
):
    """
    Upload multiple voice recording files and deploy them to all servers.
    
    This endpoint:
    1. Validates the campaign_model_voice_id
    2. Saves files to /home/doc/work/xdialcoreapi/sounds
    3. Creates database records
    4. Fetches server IPs from database
    5. Runs bash script to deploy to all servers
    6. Appends deployment logs to /home/doc/work/xdialcoreapi/logs/deployment.log
    """
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Validate campaign_model_voice exists
        cmv_check = """
            SELECT cmv.id, c.name as campaign_name, m.name as model_name, v.name as voice_name
            FROM campaign_model_voice cmv
            JOIN campaign_model cm ON cmv.campaign_model_id = cm.id
            JOIN campaigns c ON cm.campaign_id = c.id
            JOIN models m ON cm.model_id = m.id
            JOIN voices v ON cmv.voice_id = v.id
            WHERE cmv.id = $1
        """
        cmv = await conn.fetchrow(cmv_check, campaign_model_voice_id)
        
        if not cmv:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Campaign model voice assignment with ID {campaign_model_voice_id} not found"
            )
        
        uploaded_recordings = []
        failed_files = []
        saved_files = []
        errors = []
        
        # Process each file
        for file in files:
            try:
                # Validate file extension (audio files only)
                allowed_extensions = {'.wav', '.sln', '.gsm', '.ulaw', '.alaw', '.mp3', '.g722'}
                file_ext = os.path.splitext(file.filename)[1].lower()
                
                if file_ext not in allowed_extensions:
                    errors.append(f"{file.filename}: Invalid file type. Allowed: {', '.join(allowed_extensions)}")
                    failed_files.append(file.filename)
                    continue
                
                # Check if recording already exists
                existing_query = """
                    SELECT id FROM voice_recordings
                    WHERE campaign_model_voice_id = $1 AND name = $2
                """
                existing = await conn.fetchrow(existing_query, campaign_model_voice_id, file.filename)
                
                if existing:
                    errors.append(f"{file.filename}: Recording already exists")
                    failed_files.append(file.filename)
                    continue
                
                # Save file to local storage
                file_path = os.path.join(RECORDINGS_DIR, file.filename)
                
                with open(file_path, 'wb') as f:
                    content = await file.read()
                    f.write(content)
                
                # Set file permissions (readable by all)
                os.chmod(file_path, 0o644)
                
                saved_files.append(file.filename)
                
                # Create database record
                insert_query = """
                    INSERT INTO voice_recordings (name, campaign_model_voice_id)
                    VALUES ($1, $2)
                    RETURNING id, name, campaign_model_voice_id
                """
                result = await conn.fetchrow(insert_query, file.filename, campaign_model_voice_id)
                
                uploaded_recordings.append({
                    'id': result['id'],
                    'name': result['name'],
                    'campaign_model_voice_id': result['campaign_model_voice_id'],
                    'campaign_name': cmv['campaign_name'],
                    'model_name': cmv['model_name'],
                    'voice_name': cmv['voice_name']
                })
                
            except Exception as e:
                errors.append(f"{file.filename}: {str(e)}")
                failed_files.append(file.filename)
                # Clean up file if it was saved
                file_path = os.path.join(RECORDINGS_DIR, file.filename)
                if os.path.exists(file_path):
                    os.remove(file_path)
        
        # Deploy to all servers if any files were uploaded successfully
        deployment_log = ""
        deployment_success = True
        
        if saved_files:
            # Get server IPs from database
            server_ips = await get_server_ips()
            
            if not server_ips:
                errors.append("No servers found in database")
                deployment_success = False
            else:
                # Log server IPs for debugging
                print(f"Deploying to {len(server_ips)} servers: {', '.join(server_ips)}")
                deployment_success, deployment_log = run_deployment_script("upload", saved_files, server_ips)
                
                if not deployment_success:
                    errors.append("Deployment to servers failed. Check logs for details.")
        
        return UploadRecordingsResponse(
            success=deployment_success and len(failed_files) == 0,
            uploaded_count=len(uploaded_recordings),
            failed_count=len(failed_files),
            recordings=uploaded_recordings,
            deployment_log=deployment_log,
            errors=errors
        )


@router.post("/delete", response_model=DeleteRecordingsResponse)
async def delete_voice_recordings(recording_ids: List[int] = Form(...)):
    """
    Delete multiple voice recordings and remove them from all servers.
    
    This endpoint:
    1. Validates recording IDs
    2. Deletes category assignments
    3. Deletes database records
    4. Fetches server IPs from database
    5. Runs bash script to delete from all servers
    6. Appends deployment logs to /home/doc/work/xdialcoreapi/logs/deployment.log
    """
    pool = await get_db()
    
    async with pool.acquire() as conn:
        deleted_files = []
        errors = []
        
        # Fetch and validate all recordings
        for recording_id in recording_ids:
            check_query = """
                SELECT id, name FROM voice_recordings WHERE id = $1
            """
            recording = await conn.fetchrow(check_query, recording_id)
            
            if not recording:
                errors.append(f"Recording ID {recording_id}: Not found")
                continue
            
            deleted_files.append(recording['name'])
            
            # Delete category assignments first
            delete_categories_query = "DELETE FROM voice_recording_categories WHERE voice_recording_id = $1"
            await conn.execute(delete_categories_query, recording_id)
            
            # Delete from database
            delete_query = "DELETE FROM voice_recordings WHERE id = $1"
            await conn.execute(delete_query, recording_id)
            
            # Delete from local storage
            file_path = os.path.join(RECORDINGS_DIR, recording['name'])
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    errors.append(f"{recording['name']}: Failed to delete local file - {str(e)}")
        
        # Deploy deletions to all servers
        deployment_log = ""
        deployment_success = True
        
        if deleted_files:
            # Get server IPs from database
            server_ips = await get_server_ips()
            
            if not server_ips:
                errors.append("No servers found in database")
                deployment_success = False
            else:
                print(f"Deploying to {len(server_ips)} servers: {', '.join(server_ips)}")
                deployment_success, deployment_log = run_deployment_script("delete", deleted_files, server_ips)
                
                if not deployment_success:
                    errors.append("Deployment to servers failed. Check logs for details.")
        
        return DeleteRecordingsResponse(
            success=deployment_success and len(errors) == 0,
            deleted_count=len(deleted_files),
            deployment_log=deployment_log,
            errors=errors
        )