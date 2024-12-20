"""
Telegram Star Shop Bot - Database Handler
Version: 1.0

Description:
SQLite database handler optimized for a low-resource environment
(0.1 CPU, 100MB RAM, 5GB Disk). Features connection pooling
and efficient referral tracking.

Technical Features:
1. Database Management:
   - SQLite with WAL journal mode
   - 20-connection pool for concurrent access
   - Thread-safe operations
   - Automatic connection management

2. Data Storage:
   - Permanent referral tracking
   - No data deletion/cleanup of user data
   - Usage statistics with 24-hour retention
   - Minimal disk usage (~32 bytes per user)

3. Performance Optimization:
   - Connection pooling for better concurrency
   - Efficient query structure
   - Minimal memory footprint
   - Automatic cleanup of only monitoring data

Storage Analysis:
- User Data: 32 bytes per user
- 5GB can store ~156 million users
- Monitoring data cleaned every 24 hours
- Referral data stored permanently
"""

import sqlite3
from contextlib import contextmanager
from queue import Queue
import threading
import time
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class ReferralDB:
    def __init__(self, db_name='bot_database.db', pool_size=20):
        self.db_name = db_name
        self.pool_size = pool_size
        self.pool = Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self.active_connections = 0
        self.max_concurrent = 0
        self._initialize_pool()
        self._create_tables_initial()

    def _create_tables_initial(self):
        """Create tables on initialization without logging."""
        conn = sqlite3.connect(self.db_name, check_same_thread=False)
        try:
            cur = conn.cursor()
            # Create referral counts table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS referral_counts (
                    user_id INTEGER PRIMARY KEY,
                    ref_count INTEGER DEFAULT 0
                )
            ''')
            
            # Create usage monitoring table
            cur.execute('''
                CREATE TABLE IF NOT EXISTS usage_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER,
                    concurrent_connections INTEGER,
                    max_concurrent INTEGER
                )
            ''')
            conn.commit()
        finally:
            conn.close()

    @contextmanager
    def get_connection(self):
        """Get a connection from the pool with monitoring."""
        conn = self.pool.get()
        with self._lock:
            self.active_connections += 1
            self.max_concurrent = max(self.max_concurrent, self.active_connections)
            if hasattr(self, '_tables_created'):
                self._log_usage()
        try:
            yield conn
        finally:
            if conn:
                self.pool.put(conn)
                with self._lock:
                    self.active_connections -= 1

    def _initialize_pool(self):
        """Initialize the connection pool."""
        for _ in range(self.pool_size):
            conn = sqlite3.connect(self.db_name, check_same_thread=False)
            conn.execute('PRAGMA journal_mode=WAL')
            self.pool.put(conn)

    def _log_usage(self):
        """Log usage statistics."""
        try:
            with self.pool.get() as conn:
                cur = conn.cursor()
                cur.execute('''
                    INSERT INTO usage_stats 
                    (timestamp, concurrent_connections, max_concurrent) 
                    VALUES (?, ?, ?)
                ''', (int(time.time()), self.active_connections, self.max_concurrent))
                conn.commit()
        finally:
            self.pool.put(conn)

# Global database instance
db = ReferralDB(pool_size=20)

def init_db():
    """Initialize database tables."""
    with db.get_connection() as conn:
        cur = conn.cursor()
        # Referral counts table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS referral_counts (
                user_id INTEGER PRIMARY KEY,
                ref_count INTEGER DEFAULT 0
            )
        ''')
        
        # Usage monitoring table
        cur.execute('''
            CREATE TABLE IF NOT EXISTS usage_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER,
                concurrent_connections INTEGER,
                max_concurrent INTEGER
            )
        ''')
        conn.commit()

def get_usage_stats() -> dict:
    """Get usage statistics for the last hour."""
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            hour_ago = int(time.time()) - 3600
            
            # Get stats with proper error handling
            try:
                cur.execute('''
                    SELECT 
                        MAX(concurrent_connections) as peak_concurrent,
                        AVG(concurrent_connections) as avg_concurrent,
                        MAX(max_concurrent) as all_time_max
                    FROM usage_stats 
                    WHERE timestamp > ?
                ''', (hour_ago,))
                
                result = cur.fetchone()
                
                # Ensure we have valid numbers
                return {
                    'current_connections': db.active_connections,
                    'peak_last_hour': result[0] if result and result[0] is not None else 0,
                    'avg_last_hour': round(float(result[1] if result and result[1] is not None else 0), 2),
                    'all_time_max': result[2] if result and result[2] is not None else 0
                }
                
            except sqlite3.Error as e:
                logger.error(f"Database error in get_usage_stats: {e}")
                return {
                    'current_connections': 0,
                    'peak_last_hour': 0,
                    'avg_last_hour': 0,
                    'all_time_max': 0
                }
                
    except Exception as e:
        logger.error(f"Connection error in get_usage_stats: {e}")
        return {
            'current_connections': 0,
            'peak_last_hour': 0,
            'avg_last_hour': 0,
            'all_time_max': 0
        }

def cleanup_old_stats():
    """Clean up stats older than 24 hours."""
    with db.get_connection() as conn:
        cur = conn.cursor()
        day_ago = int(time.time()) - 86400
        cur.execute('DELETE FROM usage_stats WHERE timestamp < ?', (day_ago,))
        conn.commit()

def increment_referral_count(referrer_id: int) -> bool:
    """Increment referral count for a user."""
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            with db._lock:  # Use lock for write operations
                cur.execute('''
                    INSERT INTO referral_counts (user_id, ref_count) 
                    VALUES (?, 1)
                    ON CONFLICT(user_id) DO UPDATE SET 
                    ref_count = ref_count + 1
                ''', (referrer_id,))
                conn.commit()
            return True
    except sqlite3.Error:
        return False

def get_referral_count(user_id: int) -> int:
    """Get referral count for a user."""
    with db.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(
            'SELECT ref_count FROM referral_counts WHERE user_id = ?',
            (user_id,)
        )
        result = cur.fetchone()
        return result[0] if result else 0 

def get_unique_users_count():
    """Get the count of unique users who have interacted with the bot."""
    try:
        with db.get_connection() as conn:  # Use the existing connection pool
            cursor = conn.cursor()
            
            # Create a users table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    first_interaction TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Get the count of unique users
            cursor.execute('SELECT COUNT(*) FROM users')
            count = cursor.fetchone()[0]
            
            return count or 0
    except Exception as e:
        logger.error(f"Error getting unique users count: {e}")
        return 0

def add_new_user(user_id: int):
    """Add a new user if they don't exist."""
    try:
        with db.get_connection() as conn:  # Use the existing connection pool
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR IGNORE INTO users (user_id)
                VALUES (?)
            ''', (user_id,))
            
            conn.commit()
    except Exception as e:
        logger.error(f"Error adding new user: {e}") 