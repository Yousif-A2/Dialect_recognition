#!/usr/bin/env python3
"""
Database Repair and Migration Script for Radio Recording Dashboard
Fixes database schema issues and adds missing columns/tables
"""

import sqlite3
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def backup_database(db_file):
    """Create a backup of the database before migration"""
    try:
        if os.path.exists(db_file):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"{db_file}.backup_{timestamp}"
            
            # Copy database file
            import shutil
            shutil.copy2(db_file, backup_file)
            logger.info(f"Database backed up to: {backup_file}")
            return backup_file
        else:
            logger.info("No existing database found, will create new one")
            return None
    except Exception as e:
        logger.error(f"Error creating backup: {e}")
        return None

def check_table_exists(cursor, table_name):
    """Check if a table exists in the database"""
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name=?
    """, (table_name,))
    return cursor.fetchone() is not None

def get_table_columns(cursor, table_name):
    """Get list of columns for a table"""
    try:
        cursor.execute(f"PRAGMA table_info({table_name})")
        return [column[1] for column in cursor.fetchall()]
    except:
        return []

def repair_database(db_file='radio_recordings.db'):
    """Repair and migrate the database schema"""
    try:
        logger.info("Starting database repair and migration...")
        
        # Create backup
        backup_file = backup_database(db_file)
        
        # Connect to database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # 1. Create missing tables
        logger.info("Checking and creating missing tables...")
        
        # Create recordings table
        if not check_table_exists(cursor, 'recordings'):
            logger.info("Creating recordings table...")
            cursor.execute('''
                CREATE TABLE recordings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_name TEXT NOT NULL,
                    station_url TEXT NOT NULL,
                    country TEXT,
                    city TEXT,
                    duration INTEGER,
                    file_path TEXT,
                    status TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    file_size INTEGER,
                    error_message TEXT
                )
            ''')
        
        # Create scheduled_recordings table
        if not check_table_exists(cursor, 'scheduled_recordings'):
            logger.info("Creating scheduled_recordings table...")
            cursor.execute('''
                CREATE TABLE scheduled_recordings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_name TEXT NOT NULL,
                    station_url TEXT NOT NULL,
                    schedule_time TEXT NOT NULL,
                    duration INTEGER NOT NULL,
                    repeat_type TEXT DEFAULT 'interval',
                    interval_minutes INTEGER,
                    country TEXT,
                    city TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        
        # Create connection_status table
        if not check_table_exists(cursor, 'connection_status'):
            logger.info("Creating connection_status table...")
            cursor.execute('''
                CREATE TABLE connection_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_name TEXT NOT NULL,
                    station_url TEXT NOT NULL,
                    status TEXT NOT NULL,
                    response_time REAL,
                    country TEXT,
                    city TEXT,
                    last_check DATETIME DEFAULT CURRENT_TIMESTAMP,
                    error_message TEXT
                )
            ''')
        
        # 2. Add missing columns to existing tables
        logger.info("Checking and adding missing columns...")
        
        # Migrate recordings table
        if check_table_exists(cursor, 'recordings'):
            recordings_columns = get_table_columns(cursor, 'recordings')
            
            if 'country' not in recordings_columns:
                cursor.execute('ALTER TABLE recordings ADD COLUMN country TEXT')
                logger.info("Added country column to recordings table")
            
            if 'city' not in recordings_columns:
                cursor.execute('ALTER TABLE recordings ADD COLUMN city TEXT')
                logger.info("Added city column to recordings table")
            
            if 'file_size' not in recordings_columns:
                cursor.execute('ALTER TABLE recordings ADD COLUMN file_size INTEGER')
                logger.info("Added file_size column to recordings table")
            
            if 'error_message' not in recordings_columns:
                cursor.execute('ALTER TABLE recordings ADD COLUMN error_message TEXT')
                logger.info("Added error_message column to recordings table")
        
        # Migrate scheduled_recordings table
        if check_table_exists(cursor, 'scheduled_recordings'):
            scheduled_columns = get_table_columns(cursor, 'scheduled_recordings')
            
            if 'interval_minutes' not in scheduled_columns:
                cursor.execute('ALTER TABLE scheduled_recordings ADD COLUMN interval_minutes INTEGER')
                logger.info("Added interval_minutes column to scheduled_recordings table")
            
            if 'country' not in scheduled_columns:
                cursor.execute('ALTER TABLE scheduled_recordings ADD COLUMN country TEXT')
                logger.info("Added country column to scheduled_recordings table")
            
            if 'city' not in scheduled_columns:
                cursor.execute('ALTER TABLE scheduled_recordings ADD COLUMN city TEXT')
                logger.info("Added city column to scheduled_recordings table")
        
        # Migrate connection_status table
        if check_table_exists(cursor, 'connection_status'):
            connection_columns = get_table_columns(cursor, 'connection_status')
            
            if 'country' not in connection_columns:
                cursor.execute('ALTER TABLE connection_status ADD COLUMN country TEXT')
                logger.info("Added country column to connection_status table")
            
            if 'city' not in connection_columns:
                cursor.execute('ALTER TABLE connection_status ADD COLUMN city TEXT')
                logger.info("Added city column to connection_status table")
            
            if 'error_message' not in connection_columns:
                cursor.execute('ALTER TABLE connection_status ADD COLUMN error_message TEXT')
                logger.info("Added error_message column to connection_status table")
        
        # 3. Create indexes for better performance
        logger.info("Creating database indexes...")
        
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_recordings_timestamp ON recordings(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_recordings_status ON recordings(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_recordings_country ON recordings(country)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_connection_status_name ON connection_status(station_name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_connection_status_last_check ON connection_status(last_check)')
            logger.info("Database indexes created successfully")
        except Exception as e:
            logger.warning(f"Some indexes may already exist: {e}")
        
        # Commit all changes
        conn.commit()
        conn.close()
        
        logger.info("✅ Database repair and migration completed successfully!")
        
        # Verify the repair
        verify_database_repair(db_file)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error during database repair: {e}")
        return False

def verify_database_repair(db_file='radio_recordings.db'):
    """Verify that the database repair was successful"""
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # Check tables exist
        required_tables = ['recordings', 'scheduled_recordings', 'connection_status']
        existing_tables = []
        
        for table in required_tables:
            if check_table_exists(cursor, table):
                existing_tables.append(table)
        
        logger.info(f"✅ Tables found: {existing_tables}")
        
        # Check key columns exist
        connection_columns = get_table_columns(cursor, 'connection_status')
        required_columns = ['country', 'city', 'error_message']
        
        missing_columns = [col for col in required_columns if col not in connection_columns]
        
        if missing_columns:
            logger.warning(f"⚠️ Missing columns in connection_status: {missing_columns}")
        else:
            logger.info("✅ All required columns present in connection_status table")
        
        # Get record counts
        for table in existing_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info(f"📊 {table}: {count} records")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error during verification: {e}")

def clean_old_data(db_file='radio_recordings.db', days_to_keep=30):
    """Optional: Clean old data from the database"""
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        
        # Clean old connection status records (keep last 30 days)
        cursor.execute("""
            DELETE FROM connection_status 
            WHERE last_check < datetime('now', '-{} days')
        """.format(days_to_keep))
        
        deleted_count = cursor.rowcount
        if deleted_count > 0:
            logger.info(f"🧹 Cleaned {deleted_count} old connection status records")
        
        conn.commit()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error cleaning old data: {e}")

def main():
    """Main function to run database repair"""
    print("🔧 Radio Recording Dashboard - Database Repair Tool")
    print("=" * 60)
    
    db_file = 'radio_recordings.db'
    
    # Check if database exists
    if os.path.exists(db_file):
        print(f"📁 Found existing database: {db_file}")
        print(f"📊 Size: {os.path.getsize(db_file)} bytes")
    else:
        print(f"📁 No existing database found, will create: {db_file}")
    
    # Ask for confirmation
    response = input("\n🔄 Proceed with database repair/migration? (y/N): ").strip().lower()
    
    if response in ['y', 'yes']:
        success = repair_database(db_file)
        
        if success:
            print("\n✅ Database repair completed successfully!")
            print("🚀 You can now run the radio dashboard without errors")
            
            # Ask about cleanup
            cleanup_response = input("\n🧹 Clean old connection status records? (y/N): ").strip().lower()
            if cleanup_response in ['y', 'yes']:
                clean_old_data(db_file)
                print("✅ Database cleanup completed")
        else:
            print("\n❌ Database repair failed. Check the logs for details.")
            if os.path.exists(f"{db_file}.backup_*"):
                print("💾 Database backup was created in case you need to restore")
    else:
        print("❌ Database repair cancelled")

if __name__ == "__main__":
    main()
