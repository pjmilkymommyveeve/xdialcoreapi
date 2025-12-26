from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict
from datetime import datetime
from core.dependencies import require_roles
from core.auth import hash_password
from database.db import get_db


router = APIRouter(prefix="/client/employees", tags=["Client Employees"])


# ============== CONFIGURATION ==============

# Role to assign to new employees
EMPLOYEE_ROLE = 'client_member'


# ============== MODELS ==============

class EmployeeBase(BaseModel):
    username: str = Field(..., min_length=3, max_length=100)
    
    @validator('username')
    def username_validator(cls, v):
        if not v.strip():
            raise ValueError('Username cannot be empty or whitespace')
        return v.strip()


class EmployeeCreate(EmployeeBase):
    password: str = Field(..., min_length=8)
    
    @validator('password')
    def password_validator(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        return v


class EmployeeUpdatePassword(BaseModel):
    password: str = Field(..., min_length=8)
    
    @validator('password')
    def password_validator(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters long')
        return v


class EmployeeResponse(BaseModel):
    id: int
    user_id: int
    username: str
    is_active: bool
    created_at: Optional[datetime] = None


class EmployeesListResponse(BaseModel):
    client_id: int
    client_name: str
    employees: List[EmployeeResponse]
    total_employees: int


# ============== HELPER FUNCTIONS ==============

async def get_user_client_id(conn, user_id: int, roles: List[str]) -> Optional[int]:
    """
    Get the client_id that the user has access to.
    For 'client' role: returns user_id as client_id
    For privileged roles: returns None (can access any)
    """
    PRIVILEGED_ROLES = ['admin', 'onboarding', 'qa']
    
    if any(role in PRIVILEGED_ROLES for role in roles):
        return None
    elif 'client' in roles:
        return user_id
    return None


async def verify_client_access(conn, client_id: int, user_id: int, roles: List[str]) -> None:
    """
    Verify user has access to manage the specified client's employees.
    """
    allowed_client_id = await get_user_client_id(conn, user_id, roles)
    
    # If allowed_client_id is None, user is privileged (can access any)
    if allowed_client_id is not None and client_id != allowed_client_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied"
        )


async def verify_client_exists(conn, client_id: int) -> dict:
    """Verify client exists and return client data."""
    client_query = """
        SELECT c.client_id, c.name
        FROM clients c
        WHERE c.client_id = $1
    """
    client_data = await conn.fetchrow(client_query, client_id)
    
    if not client_data:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Client not found"
        )
    
    return dict(client_data)


# ============== ENDPOINTS ==============

@router.get("/{client_id}", response_model=EmployeesListResponse)
async def get_client_employees(
    client_id: int,
    user_info: Dict = Depends(require_roles(['admin', 'onboarding', 'client']))
):
    """
    Get all employees for the specified client.
    Privileged roles: Can access any client's employees
    Client role: Can only access their own employees
    """
    user_id = user_info['user_id']
    roles = user_info['roles']
    
    pool = await get_db()
    
    async with pool.acquire() as conn:
        await verify_client_access(conn, client_id, user_id, roles)
        client_data = await verify_client_exists(conn, client_id)
        
        employees_query = """
            SELECT 
                ce.id,
                ce.user_id,
                u.username,
                u.is_active
            FROM client_employees ce
            JOIN users u ON ce.user_id = u.id
            WHERE ce.client_id = $1
            ORDER BY u.username ASC
        """
        employees_data = await conn.fetch(employees_query, client_id)
        
        employees_list = [
            EmployeeResponse(
                id=emp['id'],
                user_id=emp['user_id'],
                username=emp['username'],
                is_active=emp['is_active']
            )
            for emp in employees_data
        ]
        
        return EmployeesListResponse(
            client_id=client_id,
            client_name=client_data['name'],
            employees=employees_list,
            total_employees=len(employees_list)
        )


@router.post("/{client_id}", response_model=EmployeeResponse, status_code=status.HTTP_201_CREATED)
async def create_client_employee(
    client_id: int,
    employee: EmployeeCreate,
    user_info: Dict = Depends(require_roles(['admin', 'onboarding', 'client']))
):
    """
    Create a new employee for the specified client.
    Privileged roles: Can create employee for any client
    Client role: Can only create employee for themselves
    """
    user_id = user_info['user_id']
    roles = user_info['roles']
    
    pool = await get_db()
    
    async with pool.acquire() as conn:
        await verify_client_access(conn, client_id, user_id, roles)
        await verify_client_exists(conn, client_id)
        
        # Check username uniqueness
        username_check_query = """
            SELECT id FROM users WHERE username = $1
        """
        existing_user = await conn.fetchrow(username_check_query, employee.username)
        
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username already exists"
            )
        
        # Get employee role ID
        role_query = """
            SELECT id FROM roles WHERE name = $1
        """
        role_data = await conn.fetchrow(role_query, EMPLOYEE_ROLE)
        
        if not role_data:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Employee role not found in system"
            )
        
        role_id = role_data['id']
        hashed_password = hash_password(employee.password)
        
        async with conn.transaction():
            # Create user account with is_superuser field
            create_user_query = """
                INSERT INTO users (username, password, role_id, is_active, is_staff, is_superuser)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id, username, is_active
            """
            new_user = await conn.fetchrow(
                create_user_query,
                employee.username,
                hashed_password,
                role_id,
                True,      # is_active
                False,     # is_staff
                False      # is_superuser 
            )
            
            # Create client-employee association
            create_employee_query = """
                INSERT INTO client_employees (client_id, user_id)
                VALUES ($1, $2)
                RETURNING id
            """
            employee_record = await conn.fetchrow(
                create_employee_query,
                client_id,
                new_user['id']
            )
            
            return EmployeeResponse(
                id=employee_record['id'],
                user_id=new_user['id'],
                username=new_user['username'],
                is_active=new_user['is_active']
            )

@router.patch("/{client_id}/employees/{employee_id}/password")
async def update_employee_password(
    client_id: int,
    employee_id: int,
    password_data: EmployeeUpdatePassword,
    user_info: Dict = Depends(require_roles(['admin', 'onboarding', 'qa', 'client']))
):
    """
    Update password for a specific employee.
    Privileged roles: Can update password for any client's employee
    Client role: Can only update password for their own employees
    """
    user_id = user_info['user_id']
    roles = user_info['roles']
    
    pool = await get_db()
    
    async with pool.acquire() as conn:
        await verify_client_access(conn, client_id, user_id, roles)
        await verify_client_exists(conn, client_id)
        
        # Verify employee belongs to this client
        employee_query = """
            SELECT ce.id, ce.user_id, u.username
            FROM client_employees ce
            JOIN users u ON ce.user_id = u.id
            WHERE ce.id = $1 AND ce.client_id = $2
        """
        employee_data = await conn.fetchrow(employee_query, employee_id, client_id)
        
        if not employee_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Employee not found"
            )
        
        hashed_password = hash_password(password_data.password)
        
        update_query = """
            UPDATE users
            SET password = $1
            WHERE id = $2
            RETURNING id
        """
        await conn.fetchrow(update_query, hashed_password, employee_data['user_id'])
        
        return {
            "message": "Password updated successfully",
            "employee_id": employee_id,
            "username": employee_data['username']
        }


@router.delete("/{client_id}/employees/{employee_id}", status_code=status.HTTP_200_OK)
async def delete_client_employee(
    client_id: int,
    employee_id: int,
    user_info: Dict = Depends(require_roles(['admin', 'onboarding', 'qa', 'client']))
):
    """
    Delete an employee and their user account.
    Privileged roles: Can delete any client's employee
    Client role: Can only delete their own employees
    """
    user_id = user_info['user_id']
    roles = user_info['roles']
    
    pool = await get_db()
    
    async with pool.acquire() as conn:
        await verify_client_access(conn, client_id, user_id, roles)
        await verify_client_exists(conn, client_id)
        
        employee_query = """
            SELECT ce.id, ce.user_id, u.username
            FROM client_employees ce
            JOIN users u ON ce.user_id = u.id
            WHERE ce.id = $1 AND ce.client_id = $2
        """
        employee_data = await conn.fetchrow(employee_query, employee_id, client_id)
        
        if not employee_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Employee not found"
            )
        
        async with conn.transaction():
            # Delete association first
            delete_association_query = """
                DELETE FROM client_employees
                WHERE id = $1
            """
            await conn.execute(delete_association_query, employee_id)
            
            # Delete user account
            delete_user_query = """
                DELETE FROM users
                WHERE id = $1
            """
            await conn.execute(delete_user_query, employee_data['user_id'])
            
            return {
                "message": "Employee deleted successfully",
                "employee_id": employee_id,
                "username": employee_data['username']
            }


@router.patch("/{client_id}/employees/{employee_id}/toggle-active")
async def toggle_employee_active_status(
    client_id: int,
    employee_id: int,
    user_info: Dict = Depends(require_roles(['admin', 'onboarding', 'qa', 'client']))
):
    """
    Toggle employee account active status.
    Privileged roles: Can toggle any client's employee status
    Client role: Can only toggle their own employees' status
    """
    user_id = user_info['user_id']
    roles = user_info['roles']
    
    pool = await get_db()
    
    async with pool.acquire() as conn:
        await verify_client_access(conn, client_id, user_id, roles)
        await verify_client_exists(conn, client_id)
        
        employee_query = """
            SELECT ce.id, ce.user_id, u.username, u.is_active
            FROM client_employees ce
            JOIN users u ON ce.user_id = u.id
            WHERE ce.id = $1 AND ce.client_id = $2
        """
        employee_data = await conn.fetchrow(employee_query, employee_id, client_id)
        
        if not employee_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Employee not found"
            )
        
        new_status = not employee_data['is_active']
        update_query = """
            UPDATE users
            SET is_active = $1
            WHERE id = $2
            RETURNING is_active
        """
        result = await conn.fetchrow(update_query, new_status, employee_data['user_id'])
        
        return {
            "message": f"Employee {'activated' if new_status else 'deactivated'} successfully",
            "employee_id": employee_id,
            "username": employee_data['username'],
            "is_active": result['is_active']
        }