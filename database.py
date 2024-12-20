"""
Telegram Star Shop Bot - Database Handler
Version: 1.0

Description:
Supabase database handler for serverless environment on Vercel.
Handles user tracking and referral management with cloud storage.

Technical Features:
1. Database Management:
   - PostgreSQL database via Supabase
   - Serverless-friendly implementation
   - Row Level Security (RLS) enabled
   - Service role authentication

2. Data Storage:
   - Permanent referral tracking
   - User registration tracking
   - Efficient data structure
   - Cloud-based persistent storage

3. Core Functions:
   - User registration tracking
   - Referral count management
   - Usage statistics
   - Unique user counting

Tables Structure:
- users: Stores user IDs and registration dates
- referrals: Tracks referral counts per user

Security:
- Uses Supabase service role key for admin access
- RLS policies for data protection
- Secure cloud storage
"""

from supabase import create_client
import os
from datetime import datetime, timedelta

# Initialize Supabase client with service role key
supabase = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_SERVICE_KEY')  # Use service role key instead of anon key
)

def init_db():
    """Initialize database tables if they don't exist"""
    # Tables are created via Supabase interface
    pass

def add_new_user(user_id: int):
    """Add a new user to the database"""
    try:
        supabase.table('users').insert({
            'user_id': user_id,
            'created_at': datetime.utcnow().isoformat()
        }).execute()
    except Exception:
        # User might already exist
        pass

def increment_referral_count(user_id: int):
    """Increment referral count for a user"""
    result = supabase.table('referrals').select('count').eq('user_id', user_id).execute()
    
    if len(result.data) == 0:
        # Create new record
        supabase.table('referrals').insert({
            'user_id': user_id,
            'count': 1
        }).execute()
    else:
        # Update existing record
        supabase.table('referrals')\
            .update({'count': result.data[0]['count'] + 1})\
            .eq('user_id', user_id)\
            .execute()

def get_referral_count(user_id: int) -> int:
    """Get referral count for a user"""
    result = supabase.table('referrals').select('count').eq('user_id', user_id).execute()
    return result.data[0]['count'] if result.data else 0

def get_unique_users_count() -> int:
    """Get count of unique users"""
    result = supabase.table('users').select('count', count='exact').execute()
    return result.count

def get_usage_stats():
    """Get usage statistics"""
    # For Vercel's serverless environment, we'll simplify this
    # and just return basic stats
    return {
        'current_connections': 0,  # Not applicable in serverless
        'peak_last_hour': 0,
        'avg_last_hour': 0,
        'all_time_max': 0
    } 
