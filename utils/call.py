from typing import List, Dict
from datetime import timedelta

def group_calls_by_call_id(calls: List[dict]) -> Dict[int, List[dict]]:
    """
    Group calls by call_id. Each call_id represents a unique call session.
    Returns a dictionary where keys are call_ids and values are lists of call records.
    """
    sessions = {}
    for call in calls:
        call_id = call.get('call_id')
        if call_id is not None:
            if call_id not in sessions:
                sessions[call_id] = []
            sessions[call_id].append(call)
    
    return sessions