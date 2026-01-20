from typing import List
from datetime import timedelta

def group_calls_by_session(calls: List[dict], duration_minutes: int = 2) -> List[List[dict]]:
    """
    Group calls by number and timestamp proximity.
    Calls with the same number within duration_minutes are considered part of the same session.
    
    Args:
        calls: List of call dictionaries with 'number' and 'timestamp' keys
        duration_minutes: Maximum time difference in minutes to group calls together
    
    Returns:
        List of sessions, where each session is a list of calls
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
            within_window = time_diff <= timedelta(minutes=duration_minutes)
            
            if same_number and within_window:
                current_session.append(call)
            else:
                # Start new session
                sessions.append(current_session)
                current_session = [call]
    
    # Add last session
    if current_session:
        sessions.append(current_session)
    
    return sessions