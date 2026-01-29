from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
from typing import List
from database.db import get_db

router = APIRouter(prefix="/voice-categories", tags=["Voice Categories"])


# ============== PYDANTIC SCHEMAS ==============

class VoiceCategoryCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)


class VoiceCategoryUpdate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)


class VoiceCategoryResponse(BaseModel):
    id: int
    name: str


class VoiceRecordingCategoryCreate(BaseModel):
    voice_recording_id: int = Field(..., gt=0)
    voice_category_id: int = Field(..., gt=0)


class VoiceRecordingCategoryResponse(BaseModel):
    id: int
    voice_recording_id: int
    voice_recording_name: str
    voice_category_id: int
    voice_category_name: str


class BulkVoiceCategoryCreate(BaseModel):
    categories: List[VoiceCategoryCreate]


class BulkVoiceCategoryCreateResponse(BaseModel):
    success: bool
    created_count: int
    failed_count: int
    categories: List[VoiceCategoryResponse]
    errors: List[str] = []


class BulkVoiceCategoryDeleteResponse(BaseModel):
    success: bool
    deleted_count: int
    failed_count: int
    errors: List[str] = []


class BulkVoiceRecordingCategoryCreate(BaseModel):
    assignments: List[VoiceRecordingCategoryCreate]


class BulkVoiceRecordingCategoryCreateResponse(BaseModel):
    success: bool
    created_count: int
    failed_count: int
    assignments: List[VoiceRecordingCategoryResponse]
    errors: List[str] = []


class BulkVoiceRecordingCategoryDeleteResponse(BaseModel):
    success: bool
    deleted_count: int
    failed_count: int
    errors: List[str] = []


# ============== VOICE CATEGORY CRUD ENDPOINTS ==============

@router.get("/", response_model=List[VoiceCategoryResponse])
async def get_all_voice_categories():
    """Get all voice categories."""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        query = "SELECT id, name FROM voice_categories ORDER BY name"
        categories = await conn.fetch(query)
        
        return [{'id': c['id'], 'name': c['name']} for c in categories]


@router.post("/", response_model=VoiceCategoryResponse, status_code=status.HTTP_201_CREATED)
async def create_voice_category(category_data: VoiceCategoryCreate):
    """Create a new voice category."""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Check if category name already exists
        check_query = "SELECT id FROM voice_categories WHERE name = $1"
        existing = await conn.fetchrow(check_query, category_data.name)
        
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Voice category with name '{category_data.name}' already exists"
            )
        
        # Insert new category
        insert_query = "INSERT INTO voice_categories (name) VALUES ($1) RETURNING id, name"
        category = await conn.fetchrow(insert_query, category_data.name)
        
        return {'id': category['id'], 'name': category['name']}


@router.post("/bulk", response_model=BulkVoiceCategoryCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_voice_categories_bulk(bulk_data: BulkVoiceCategoryCreate):
    """Create multiple voice categories at once."""
    pool = await get_db()
    
    created_categories = []
    errors = []
    
    async with pool.acquire() as conn:
        for category_data in bulk_data.categories:
            try:
                # Check if category name already exists
                check_query = "SELECT id FROM voice_categories WHERE name = $1"
                existing = await conn.fetchrow(check_query, category_data.name)
                
                if existing:
                    errors.append(f"Category '{category_data.name}' already exists")
                    continue
                
                # Insert new category
                insert_query = "INSERT INTO voice_categories (name) VALUES ($1) RETURNING id, name"
                category = await conn.fetchrow(insert_query, category_data.name)
                
                created_categories.append({'id': category['id'], 'name': category['name']})
                
            except Exception as e:
                errors.append(f"Category '{category_data.name}': {str(e)}")
    
    return BulkVoiceCategoryCreateResponse(
        success=len(errors) == 0,
        created_count=len(created_categories),
        failed_count=len(errors),
        categories=created_categories,
        errors=errors
    )


@router.put("/{category_id}", response_model=VoiceCategoryResponse)
async def update_voice_category(category_id: int, category_data: VoiceCategoryUpdate):
    """Update a voice category's name."""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Check if category exists
        check_query = "SELECT id FROM voice_categories WHERE id = $1"
        existing = await conn.fetchrow(check_query, category_id)
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Voice category with ID {category_id} not found"
            )
        
        # Check if new name conflicts
        conflict_query = "SELECT id FROM voice_categories WHERE name = $1 AND id != $2"
        conflict = await conn.fetchrow(conflict_query, category_data.name, category_id)
        
        if conflict:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Voice category with name '{category_data.name}' already exists"
            )
        
        # Update category
        update_query = "UPDATE voice_categories SET name = $1 WHERE id = $2 RETURNING id, name"
        category = await conn.fetchrow(update_query, category_data.name, category_id)
        
        return {'id': category['id'], 'name': category['name']}


@router.delete("/{category_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_voice_category(category_id: int):
    """Delete a voice category. Fails if assigned to any recordings."""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Check if category exists
        check_query = "SELECT id FROM voice_categories WHERE id = $1"
        existing = await conn.fetchrow(check_query, category_id)
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Voice category with ID {category_id} not found"
            )
        
        # Check if category is assigned
        usage_query = "SELECT COUNT(*) as count FROM voice_recording_categories WHERE voice_category_id = $1"
        usage = await conn.fetchrow(usage_query, category_id)
        
        if usage['count'] > 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Cannot delete category: assigned to {usage['count']} recording(s)"
            )
        
        # Delete category
        delete_query = "DELETE FROM voice_categories WHERE id = $1"
        await conn.execute(delete_query, category_id)


@router.post("/bulk-delete", response_model=BulkVoiceCategoryDeleteResponse)
async def delete_voice_categories_bulk(category_ids: List[int]):
    """Delete multiple voice categories at once. Skips categories assigned to recordings."""
    pool = await get_db()
    
    deleted_count = 0
    errors = []
    
    async with pool.acquire() as conn:
        for category_id in category_ids:
            try:
                # Check if category exists
                check_query = "SELECT id, name FROM voice_categories WHERE id = $1"
                existing = await conn.fetchrow(check_query, category_id)
                
                if not existing:
                    errors.append(f"Category ID {category_id}: Not found")
                    continue
                
                # Check if category is assigned
                usage_query = "SELECT COUNT(*) as count FROM voice_recording_categories WHERE voice_category_id = $1"
                usage = await conn.fetchrow(usage_query, category_id)
                
                if usage['count'] > 0:
                    errors.append(f"Category '{existing['name']}': assigned to {usage['count']} recording(s)")
                    continue
                
                # Delete category
                delete_query = "DELETE FROM voice_categories WHERE id = $1"
                await conn.execute(delete_query, category_id)
                deleted_count += 1
                
            except Exception as e:
                errors.append(f"Category ID {category_id}: {str(e)}")
    
    return BulkVoiceCategoryDeleteResponse(
        success=len(errors) == 0,
        deleted_count=deleted_count,
        failed_count=len(errors),
        errors=errors
    )


# ============== VOICE RECORDING CATEGORY ASSIGNMENT ENDPOINTS ==============

@router.get("/recordings/{recording_id}/categories", response_model=List[VoiceRecordingCategoryResponse])
async def get_recording_categories(recording_id: int):
    """Get all categories assigned to a recording."""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Check if recording exists
        rec_check = "SELECT id, name FROM voice_recordings WHERE id = $1"
        rec = await conn.fetchrow(rec_check, recording_id)
        
        if not rec:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Voice recording with ID {recording_id} not found"
            )
        
        # Fetch categories
        query = """
            SELECT vrc.id, vrc.voice_recording_id, vrc.voice_category_id, vc.name as category_name
            FROM voice_recording_categories vrc
            JOIN voice_categories vc ON vrc.voice_category_id = vc.id
            WHERE vrc.voice_recording_id = $1
            ORDER BY vc.name
        """
        categories = await conn.fetch(query, recording_id)
        
        return [
            {
                'id': c['id'],
                'voice_recording_id': c['voice_recording_id'],
                'voice_recording_name': rec['name'],
                'voice_category_id': c['voice_category_id'],
                'voice_category_name': c['category_name']
            }
            for c in categories
        ]


@router.post("/recordings/categories", response_model=VoiceRecordingCategoryResponse, status_code=status.HTTP_201_CREATED)
async def assign_category_to_recording(assignment: VoiceRecordingCategoryCreate):
    """Assign a category to a voice recording."""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Check if recording exists
        rec_check = "SELECT id, name FROM voice_recordings WHERE id = $1"
        rec = await conn.fetchrow(rec_check, assignment.voice_recording_id)
        
        if not rec:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Voice recording with ID {assignment.voice_recording_id} not found"
            )
        
        # Check if category exists
        cat_check = "SELECT id, name FROM voice_categories WHERE id = $1"
        cat = await conn.fetchrow(cat_check, assignment.voice_category_id)
        
        if not cat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Voice category with ID {assignment.voice_category_id} not found"
            )
        
        # Check if assignment already exists
        existing_query = """
            SELECT id FROM voice_recording_categories 
            WHERE voice_recording_id = $1 AND voice_category_id = $2
        """
        existing = await conn.fetchrow(existing_query, assignment.voice_recording_id, assignment.voice_category_id)
        
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Category '{cat['name']}' already assigned to this recording"
            )
        
        # Create assignment
        insert_query = """
            INSERT INTO voice_recording_categories (voice_recording_id, voice_category_id)
            VALUES ($1, $2)
            RETURNING id, voice_recording_id, voice_category_id
        """
        result = await conn.fetchrow(insert_query, assignment.voice_recording_id, assignment.voice_category_id)
        
        return {
            'id': result['id'],
            'voice_recording_id': result['voice_recording_id'],
            'voice_recording_name': rec['name'],
            'voice_category_id': result['voice_category_id'],
            'voice_category_name': cat['name']
        }


@router.post("/recordings/categories/bulk", response_model=BulkVoiceRecordingCategoryCreateResponse, status_code=status.HTTP_201_CREATED)
async def assign_categories_to_recordings_bulk(bulk_data: BulkVoiceRecordingCategoryCreate):
    """Assign multiple categories to recordings at once."""
    pool = await get_db()
    
    created_assignments = []
    errors = []
    
    async with pool.acquire() as conn:
        for assignment in bulk_data.assignments:
            try:
                # Check if recording exists
                rec_check = "SELECT id, name FROM voice_recordings WHERE id = $1"
                rec = await conn.fetchrow(rec_check, assignment.voice_recording_id)
                
                if not rec:
                    errors.append(f"Recording ID {assignment.voice_recording_id}: Not found")
                    continue
                
                # Check if category exists
                cat_check = "SELECT id, name FROM voice_categories WHERE id = $1"
                cat = await conn.fetchrow(cat_check, assignment.voice_category_id)
                
                if not cat:
                    errors.append(f"Category ID {assignment.voice_category_id}: Not found")
                    continue
                
                # Check if assignment already exists
                existing_query = """
                    SELECT id FROM voice_recording_categories 
                    WHERE voice_recording_id = $1 AND voice_category_id = $2
                """
                existing = await conn.fetchrow(existing_query, assignment.voice_recording_id, assignment.voice_category_id)
                
                if existing:
                    errors.append(f"Category '{cat['name']}' already assigned to recording '{rec['name']}'")
                    continue
                
                # Create assignment
                insert_query = """
                    INSERT INTO voice_recording_categories (voice_recording_id, voice_category_id)
                    VALUES ($1, $2)
                    RETURNING id, voice_recording_id, voice_category_id
                """
                result = await conn.fetchrow(insert_query, assignment.voice_recording_id, assignment.voice_category_id)
                
                created_assignments.append({
                    'id': result['id'],
                    'voice_recording_id': result['voice_recording_id'],
                    'voice_recording_name': rec['name'],
                    'voice_category_id': result['voice_category_id'],
                    'voice_category_name': cat['name']
                })
                
            except Exception as e:
                errors.append(f"Recording {assignment.voice_recording_id} + Category {assignment.voice_category_id}: {str(e)}")
    
    return BulkVoiceRecordingCategoryCreateResponse(
        success=len(errors) == 0,
        created_count=len(created_assignments),
        failed_count=len(errors),
        assignments=created_assignments,
        errors=errors
    )


@router.delete("/recordings/categories/{vrc_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_category_from_recording(vrc_id: int):
    """Remove a category from a voice recording."""
    pool = await get_db()
    
    async with pool.acquire() as conn:
        # Check if assignment exists
        check_query = "SELECT id FROM voice_recording_categories WHERE id = $1"
        existing = await conn.fetchrow(check_query, vrc_id)
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Voice recording category assignment with ID {vrc_id} not found"
            )
        
        # Delete assignment
        delete_query = "DELETE FROM voice_recording_categories WHERE id = $1"
        await conn.execute(delete_query, vrc_id)


@router.post("/recordings/categories/bulk-delete", response_model=BulkVoiceRecordingCategoryDeleteResponse)
async def remove_categories_from_recordings_bulk(vrc_ids: List[int]):
    """Remove multiple category assignments from recordings at once."""
    pool = await get_db()
    
    deleted_count = 0
    errors = []
    
    async with pool.acquire() as conn:
        for vrc_id in vrc_ids:
            try:
                # Check if assignment exists
                check_query = "SELECT id FROM voice_recording_categories WHERE id = $1"
                existing = await conn.fetchrow(check_query, vrc_id)
                
                if not existing:
                    errors.append(f"Assignment ID {vrc_id}: Not found")
                    continue
                
                # Delete assignment
                delete_query = "DELETE FROM voice_recording_categories WHERE id = $1"
                await conn.execute(delete_query, vrc_id)
                deleted_count += 1
                
            except Exception as e:
                errors.append(f"Assignment ID {vrc_id}: {str(e)}")
    
    return BulkVoiceRecordingCategoryDeleteResponse(
        success=len(errors) == 0,
        deleted_count=deleted_count,
        failed_count=len(errors),
        errors=errors
    )