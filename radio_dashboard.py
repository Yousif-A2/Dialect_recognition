"""
Enhanced Radio Recording Dashboard with Automation and Monitoring
Features:
- Automated scheduled recording
- Real-time statistics
- Connection monitoring
- Recording management
"""

import gradio as gr
import json
import os
import subprocess
import time
import threading
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
import requests
from concurrent.futures import ThreadPoolExecutor
import schedule
import signal
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('radio_dashboard.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RadioDashboard:
    """Enhanced Radio Recording Dashboard with automation and monitoring"""
    
    def __init__(self, data_file: str = 'arab_stations_radio_browser.json'):
        self.data_file = data_file
        self.stations_by_country = {}
        self.stations_by_city = {}
        self.recording_scheduler = None
        self.scheduler_thread = None
        self.is_running = False
        self.connection_status = {}
        self.recording_stats = {
            'total_recordings': 0,
            'successful_recordings': 0,
            'failed_recordings': 0,
            'total_duration': 0,
            'last_recording': None
        }
        
        # Initialize database
        self.init_database()
        self.load_stations()
        self.load_statistics()
        
        # Start background tasks
        self.start_scheduler()
        self.start_connection_monitor()
    
    def init_database(self):
        """Initialize SQLite database for storing recording history"""
        try:
            conn = sqlite3.connect('radio_recordings.db')
            cursor = conn.cursor()
            
            # Create recordings table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS recordings (
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
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scheduled_recordings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    station_name TEXT NOT NULL,
                    station_url TEXT NOT NULL,
                    schedule_time TEXT NOT NULL,
                    duration INTEGER NOT NULL,
                    repeat_type TEXT DEFAULT 'once',
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
    
    def load_stations(self):
        """Load radio stations from JSON file"""
        try:
            with open(self.data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            self.stations_by_country = data.get('stations_by_country', {})
            self.stations_by_city = data.get('stations_by_city', {})
            
            logger.info(f"Loaded {len(self.stations_by_country)} countries and {len(self.stations_by_city)} cities")
            
        except FileNotFoundError:
            logger.error(f"Data file {self.data_file} not found")
            self.stations_by_country = {}
            self.stations_by_city = {}
        except Exception as e:
            logger.error(f"Error loading stations: {e}")
            self.stations_by_country = {}
            self.stations_by_city = {}
    
    def load_statistics(self):
        """Load recording statistics from database"""
        try:
            conn = sqlite3.connect('radio_recordings.db')
            cursor = conn.cursor()
            
            # Get total recordings
            cursor.execute("SELECT COUNT(*) FROM recordings")
            self.recording_stats['total_recordings'] = cursor.fetchone()[0]
            
            # Get successful recordings
            cursor.execute("SELECT COUNT(*) FROM recordings WHERE status = 'success'")
            self.recording_stats['successful_recordings'] = cursor.fetchone()[0]
            
            # Get failed recordings
            cursor.execute("SELECT COUNT(*) FROM recordings WHERE status = 'failed'")
            self.recording_stats['failed_recordings'] = cursor.fetchone()[0]
            
            # Get total duration
            cursor.execute("SELECT SUM(duration) FROM recordings WHERE status = 'success'")
            total_duration = cursor.fetchone()[0]
            self.recording_stats['total_duration'] = total_duration if total_duration else 0
            
            # Get last recording
            cursor.execute("SELECT timestamp FROM recordings ORDER BY timestamp DESC LIMIT 1")
            last_recording = cursor.fetchone()
            self.recording_stats['last_recording'] = last_recording[0] if last_recording else None
            
            conn.close()
        except Exception as e:
            logger.error(f"Error loading statistics: {e}")
    
    def check_station_connection(self, station_url: str, timeout: int = 10) -> bool:
        """Check if a radio station is accessible"""
        try:
            response = requests.head(station_url, timeout=timeout, allow_redirects=True)
            return response.status_code == 200
        except:
            return False
    
    def start_connection_monitor(self):
        """Start enhanced background connection monitoring"""
        def monitor_connections():
            while self.is_running:
                try:
                    # Monitor stations from different countries (rotating selection)
                    countries_to_monitor = list(self.stations_by_country.keys())[:10]  # Monitor 10 countries
                    
                    for country in countries_to_monitor:
                        stations = self.stations_by_country[country][:2]  # 2 stations per country
                        
                        for station in stations:
                            start_time = time.time()
                            status = self.check_station_connection(station['url'], timeout=8)
                            response_time = time.time() - start_time
                            
                            self.connection_status[station['name']] = {
                                'status': 'online' if status else 'offline',
                                'last_check': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                'response_time': response_time,
                                'country': country,
                                'city': station.get('state', 'Unknown'),
                                'url': station['url']
                            }
                            
                            # Save to database with better error handling
                            try:
                                conn = sqlite3.connect('radio_recordings.db')
                                cursor = conn.cursor()
                                
                                # Check if table exists first
                                cursor.execute("""
                                    SELECT name FROM sqlite_master 
                                    WHERE type='table' AND name='connection_status'
                                """)
                                
                                if cursor.fetchone():
                                    cursor.execute('''
                                        INSERT INTO connection_status 
                                        (station_name, station_url, status, response_time, country, city, last_check)
                                        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                                    ''', (station['name'], station['url'], 
                                          'online' if status else 'offline', 
                                          response_time, country, station.get('state', 'Unknown')))
                                    conn.commit()
                                else:
                                    logger.warning("connection_status table does not exist, skipping database save")
                                
                                conn.close()
                                
                            except sqlite3.Error as db_error:
                                logger.error(f"Database error in connection monitor: {db_error}")
                                # Continue monitoring even if database fails
                            except Exception as db_error:
                                logger.error(f"Unexpected database error in connection monitor: {db_error}")
                            
                            time.sleep(1)  # Small delay between checks
                    
                    # Sleep before next monitoring cycle (5 minutes)
                    time.sleep(300)
                    
                except Exception as e:
                    logger.error(f"Error in enhanced connection monitor: {e}")
                    time.sleep(60)
        
        self.is_running = True
        monitor_thread = threading.Thread(target=monitor_connections, daemon=True)
        monitor_thread.start()
        logger.info("Enhanced connection monitoring started")
    
    def start_scheduler(self):
        """Start the recording scheduler"""
        def run_scheduler():
            while self.is_running:
                try:
                    schedule.run_pending()
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"Error in scheduler: {e}")
                    time.sleep(60)
        
        self.is_running = True
        self.scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        self.scheduler_thread.start()
    
    def record_station(self, station_name: str, station_url: str, duration: int, 
                      country: str = None, city: str = None) -> Dict:
        """Record a single radio station"""
        try:
            # Create organized directory structure: recordings/Country/City/
            base_dir = "recordings"
            country_dir = country.replace(" ", "_") if country else "Unknown_Country"
            city_dir = city.replace(" ", "_") if city else "Unknown_City"
            
            # Create full directory path
            full_dir = os.path.join(base_dir, country_dir, city_dir)
            os.makedirs(full_dir, exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_name = "".join(c for c in station_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            filename = f"{safe_name}_{timestamp}.mp3"
            filepath = os.path.join(full_dir, filename)
            
            # Record using ffmpeg
            cmd = ['ffmpeg', '-i', station_url, '-t', str(duration), 
                   '-c:a', 'mp3', '-b:a', '128k', '-y', filepath]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=duration+30)
            
            # Check if recording was successful
            if result.returncode == 0 and os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                status = 'success'
                error_message = None
            else:
                file_size = 0
                status = 'failed'
                error_message = result.stderr[:500] if result.stderr else "Unknown error"
            
            # Save to database
            self.save_recording_to_db(station_name, station_url, country, city, 
                                    duration, filepath, status, file_size, error_message)
            
            # Update statistics
            self.update_statistics(status, duration)
            
            return {
                'status': status,
                'filename': filename,
                'duration': duration,
                'file_size': file_size,
                'error': error_message
            }
            
        except subprocess.TimeoutExpired:
            return {
                'status': 'failed',
                'filename': None,
                'duration': duration,
                'file_size': 0,
                'error': 'Recording timeout'
            }
        except Exception as e:
            logger.error(f"Error recording {station_name}: {e}")
            return {
                'status': 'failed',
                'filename': None,
                'duration': duration,
                'file_size': 0,
                'error': str(e)
            }
    
    def save_recording_to_db(self, station_name: str, station_url: str, country: str, 
                           city: str, duration: int, filepath: str, status: str, 
                           file_size: int, error_message: str):
        """Save recording information to database"""
        try:
            conn = sqlite3.connect('radio_recordings.db')
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO recordings 
                (station_name, station_url, country, city, duration, file_path, 
                 status, file_size, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (station_name, station_url, country, city, duration, filepath, 
                  status, file_size, error_message))
            
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error saving recording to database: {e}")
    
    def update_statistics(self, status: str, duration: int):
        """Update recording statistics"""
        self.recording_stats['total_recordings'] += 1
        if status == 'success':
            self.recording_stats['successful_recordings'] += 1
            self.recording_stats['total_duration'] += duration
        else:
            self.recording_stats['failed_recordings'] += 1
        
        self.recording_stats['last_recording'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def schedule_interval_recording(self, station_name: str, station_url: str, 
                                  interval_minutes: int, duration: int, 
                                  country: str = None, city: str = None) -> str:
        """Schedule recording at regular intervals (every X minutes)"""
        try:
            # Create recording job
            def interval_job():
                logger.info(f"Executing interval recording: {station_name} (every {interval_minutes} min)")
                result = self.record_station(station_name, station_url, duration, country, city)
                logger.info(f"Interval recording result: {result}")
            
            # Schedule the job based on interval
            if interval_minutes >= 60:
                # For intervals >= 60 minutes, schedule hourly
                hours = interval_minutes // 60
                if hours == 1:
                    schedule.every().hour.do(interval_job)
                else:
                    schedule.every(hours).hours.do(interval_job)
            else:
                # For intervals < 60 minutes, schedule by minutes
                schedule.every(interval_minutes).minutes.do(interval_job)
            
            # Save to database with interval info
            conn = sqlite3.connect('radio_recordings.db')
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO scheduled_recordings 
                (station_name, station_url, schedule_time, duration, repeat_type, country, city)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (station_name, station_url, f"every_{interval_minutes}_min", 
                  duration, "interval", country, city))
            conn.commit()
            conn.close()
            
            return f"✅ Interval recording scheduled for {station_name} every {interval_minutes} minutes"
            
        except Exception as e:
            logger.error(f"Error scheduling interval recording: {e}")
            return f"❌ Error scheduling interval recording: {str(e)}"
    
    def get_all_stations_status(self) -> Dict:
        """Get connection status for all radio stations"""
        try:
            all_stations_status = {
                'total_stations': 0,
                'online_stations': 0,
                'offline_stations': 0,
                'untested_stations': 0,
                'by_country': {},
                'detailed_status': []
            }
            
            # Process all countries and their stations
            for country, stations in self.stations_by_country.items():
                country_stats = {
                    'total': len(stations),
                    'online': 0,
                    'offline': 0,
                    'untested': 0
                }
                
                for station in stations:
                    all_stations_status['total_stations'] += 1
                    
                    # Check if we have cached status
                    if station['name'] in self.connection_status:
                        status_info = self.connection_status[station['name']]
                        if status_info['status'] == 'online':
                            all_stations_status['online_stations'] += 1
                            country_stats['online'] += 1
                        else:
                            all_stations_status['offline_stations'] += 1
                            country_stats['offline'] += 1
                    else:
                        all_stations_status['untested_stations'] += 1
                        country_stats['untested'] += 1
                    
                    # Add to detailed status
                    all_stations_status['detailed_status'].append({
                        'name': station['name'],
                        'country': country,
                        'city': station.get('state', 'Unknown'),
                        'url': station['url'],
                        'status': self.connection_status.get(station['name'], {}).get('status', 'untested'),
                        'last_check': self.connection_status.get(station['name'], {}).get('last_check', 'Never'),
                        'bitrate': station.get('bitrate', 'Unknown'),
                        'language': station.get('language', 'Unknown')
                    })
                
                all_stations_status['by_country'][country] = country_stats
            
            return all_stations_status
            
        except Exception as e:
            logger.error(f"Error getting all stations status: {e}")
            return {'error': str(e)}
    
    def test_all_stations_connection(self, max_workers: int = 10) -> str:
        """Test connection for all radio stations with threading"""
        try:
            all_stations = []
            for country, stations in self.stations_by_country.items():
                for station in stations:
                    all_stations.append({
                        'name': station['name'],
                        'url': station['url'],
                        'country': country,
                        'city': station.get('state', 'Unknown')
                    })
            
            total_stations = len(all_stations)
            tested_count = 0
            online_count = 0
            
            def test_station(station_info):
                nonlocal tested_count, online_count
                try:
                    status = self.check_station_connection(station_info['url'], timeout=5)
                    tested_count += 1
                    
                    if status:
                        online_count += 1
                    
                    # Update connection status cache
                    self.connection_status[station_info['name']] = {
                        'status': 'online' if status else 'offline',
                        'last_check': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'country': station_info['country'],
                        'city': station_info['city']
                    }
                    
                    # Save to database
                    conn = sqlite3.connect('radio_recordings.db')
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT OR REPLACE INTO connection_status 
                        (station_name, station_url, status, last_check)
                        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    ''', (station_info['name'], station_info['url'], 
                          'online' if status else 'offline'))
                    conn.commit()
                    conn.close()
                    
                    return f"{'✅' if status else '❌'} {station_info['name']} ({station_info['country']})"
                    
                except Exception as e:
                    return f"❌ {station_info['name']}: Error - {str(e)[:50]}"
            
            # Test stations with threading
            results = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_station = {executor.submit(test_station, station): station 
                                   for station in all_stations}
                
                for future in future_to_station:
                    try:
                        result = future.result(timeout=10)
                        results.append(result)
                    except Exception as e:
                        station = future_to_station[future]
                        results.append(f"❌ {station['name']}: Timeout")
            
            # Generate summary
            success_rate = (online_count / tested_count * 100) if tested_count > 0 else 0
            summary = f"""
🔍 **Connection Test Results**

📊 **Summary:**
• Total Stations: {total_stations}
• Tested: {tested_count}
• Online: {online_count}
• Offline: {tested_count - online_count}
• Success Rate: {success_rate:.1f}%

📋 **Detailed Results:**
{chr(10).join(results[:50])}  # Show first 50 results
"""
            
            if len(results) > 50:
                summary += f"\n... and {len(results) - 50} more stations"
            
            return summary
            
        except Exception as e:
            logger.error(f"Error testing all stations: {e}")
            return f"❌ Error testing stations: {str(e)}"
    
    def get_detailed_statistics(self) -> str:
        """Get comprehensive statistics with detailed breakdown"""
        try:
            # Get all stations status
            stations_status = self.get_all_stations_status()
            
            # Get recording statistics by country
            conn = sqlite3.connect('radio_recordings.db')
            cursor = conn.cursor()
            
            # Total recordings by status
            cursor.execute('''
                SELECT status, COUNT(*), SUM(duration), SUM(file_size)
                FROM recordings 
                GROUP BY status
            ''')
            status_stats = cursor.fetchall()
            
            # Recordings by country
            cursor.execute('''
                SELECT country, COUNT(*), SUM(duration)
                FROM recordings 
                WHERE country IS NOT NULL
                GROUP BY country 
                ORDER BY COUNT(*) DESC
            ''')
            country_stats = cursor.fetchall()
            
            # Recent activity (last 24 hours)
            cursor.execute('''
                SELECT COUNT(*) 
                FROM recordings 
                WHERE datetime(timestamp) > datetime('now', '-1 day')
            ''')
            recent_recordings = cursor.fetchone()[0]
            
            # Average recording duration
            cursor.execute('''
                SELECT AVG(duration) 
                FROM recordings 
                WHERE status = 'success'
            ''')
            avg_duration = cursor.fetchone()[0] or 0
            
            conn.close()
            
            # Build detailed statistics display
            stats_display = f"""
📊 **Comprehensive Dashboard Statistics**

🎵 **Radio Stations Overview:**
• Total Stations: {stations_status['total_stations']}
• Online Stations: {stations_status['online_stations']} ({(stations_status['online_stations']/stations_status['total_stations']*100) if stations_status['total_stations'] > 0 else 0:.1f}%)
• Offline Stations: {stations_status['offline_stations']} ({(stations_status['offline_stations']/stations_status['total_stations']*100) if stations_status['total_stations'] > 0 else 0:.1f}%)
• Untested Stations: {stations_status['untested_stations']} ({(stations_status['untested_stations']/stations_status['total_stations']*100) if stations_status['total_stations'] > 0 else 0:.1f}%)

📈 **Recording Statistics:**"""
            
            total_recordings = 0
            total_duration = 0
            total_size = 0
            
            for status, count, duration, size in status_stats:
                total_recordings += count
                total_duration += duration or 0
                total_size += size or 0
                
                size_mb = (size or 0) / (1024 * 1024)
                stats_display += f"\n• {status.title()}: {count} recordings, {(duration or 0)/60:.1f} min, {size_mb:.1f} MB"
            
            success_rate = 0
            if total_recordings > 0:
                successful = next((count for status, count, _, _ in status_stats if status == 'success'), 0)
                success_rate = (successful / total_recordings * 100)
            
            stats_display += f"""

📊 **Performance Metrics:**
• Total Recordings: {total_recordings}
• Success Rate: {success_rate:.1f}%
• Average Duration: {avg_duration/60:.1f} minutes
• Total Storage Used: {total_size/(1024*1024*1024):.2f} GB
• Recent Activity (24h): {recent_recordings} recordings

🌍 **Recordings by Country:**"""
            
            for country, count, duration in country_stats[:10]:  # Top 10 countries
                stats_display += f"\n• {country}: {count} recordings, {(duration or 0)/60:.1f} minutes"
            
            if len(country_stats) > 10:
                stats_display += f"\n• ... and {len(country_stats) - 10} more countries"
            
            # Add stations by country breakdown
            stats_display += f"\n\n🗺️ **Stations by Country:**"
            for country, country_data in sorted(stations_status['by_country'].items(), 
                                              key=lambda x: x[1]['total'], reverse=True)[:15]:
                stats_display += f"\n• {country}: {country_data['total']} stations "
                stats_display += f"({country_data['online']} online, {country_data['offline']} offline, {country_data['untested']} untested)"
            
            return stats_display
            
        except Exception as e:
            logger.error(f"Error getting detailed statistics: {e}")
            return f"❌ Error loading detailed statistics: {str(e)}"
    
    def get_countries(self) -> List[str]:
        """Get list of available countries"""
        return sorted(list(self.stations_by_country.keys()))
    
    def get_cities_by_country(self, country: str) -> List[str]:
        """Get cities for a specific country"""
        try:
            if not country:
                return []
            
            cities = set()
            stations = self.stations_by_country.get(country, [])
            
            for station in stations:
                if station.get('state'):
                    cities.add(station['state'])
            
            return sorted(list(cities))
        except Exception as e:
            logger.error(f"Error getting cities for {country}: {e}")
            return []
    
    def get_stations_by_location(self, country: str, city: str = None) -> List[Dict]:
        """Get stations by location"""
        if city:
            return self.stations_by_city.get(city, [])
        elif country:
            return self.stations_by_country.get(country, [])
        return []
    
    def get_statistics_display(self) -> str:
        """Get formatted statistics display"""
        stats = self.recording_stats
        success_rate = (stats['successful_recordings'] / stats['total_recordings'] * 100) if stats['total_recordings'] > 0 else 0
        
        display = f"""
📊 **Recording Statistics**

📈 Total Recordings: {stats['total_recordings']}
✅ Successful: {stats['successful_recordings']}
❌ Failed: {stats['failed_recordings']}
📊 Success Rate: {success_rate:.1f}%
⏱️ Total Duration: {stats['total_duration']/60:.1f} minutes
🕐 Last Recording: {stats['last_recording'] or 'Never'}

🔗 **Connection Status**
"""
        
        for station, status in self.connection_status.items():
            status_icon = "🟢" if status['status'] == 'online' else "🔴"
            display += f"{status_icon} {station}: {status['status']} (checked: {status['last_check']})\n"
        
        return display
    
    def get_recent_recordings(self) -> str:
        """Get recent recordings from database"""
        try:
            conn = sqlite3.connect('radio_recordings.db')
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT station_name, status, duration, timestamp, file_size
                FROM recordings 
                ORDER BY timestamp DESC 
                LIMIT 10
            ''')
            
            recordings = cursor.fetchall()
            conn.close()
            
            if not recordings:
                return "No recordings found"
            
            display = "🎵 **Recent Recordings**\n\n"
            for record in recordings:
                station, status, duration, timestamp, file_size = record
                status_icon = "✅" if status == 'success' else "❌"
                file_size_mb = file_size / (1024*1024) if file_size else 0
                
                display += f"{status_icon} {station}\n"
                display += f"   📅 {timestamp}\n"
                display += f"   ⏱️ {duration}s, 📁 {file_size_mb:.1f}MB\n\n"
            
            return display
            
        except Exception as e:
            logger.error(f"Error getting recent recordings: {e}")
            return f"Error loading recordings: {str(e)}"
    
    def manual_record(self, country: str, city: str, station_name: str, 
                     duration: int) -> str:
        """Manual recording trigger"""
        try:
            stations = self.get_stations_by_location(country, city)
            
            # Find the selected station
            selected_station = None
            for station in stations:
                if station['name'] == station_name:
                    selected_station = station
                    break
            
            if not selected_station:
                return f"❌ Station '{station_name}' not found"
            
            # Record the station
            result = self.record_station(
                selected_station['name'],
                selected_station['url'],
                duration,
                country,
                city
            )
            
            if result['status'] == 'success':
                return f"✅ Recording successful: {result['filename']}"
            else:
                return f"❌ Recording failed: {result['error']}"
                
        except Exception as e:
            logger.error(f"Error in manual recording: {e}")
            return f"❌ Error: {str(e)}"
    
    def get_scheduled_recordings(self) -> str:
        """Get list of scheduled recordings"""
        try:
            conn = sqlite3.connect('radio_recordings.db')
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT station_name, schedule_time, duration, repeat_type, is_active, created_at
                FROM scheduled_recordings 
                WHERE is_active = TRUE
                ORDER BY schedule_time
            ''')
            
            recordings = cursor.fetchall()
            conn.close()
            
            if not recordings:
                return "No scheduled recordings"
            
            display = "⏰ **Scheduled Recordings**\n\n"
            for record in recordings:
                station, sched_time, duration, repeat_type, is_active, created_at = record
                
                display += f"🎵 {station}\n"
                display += f"   ⏰ Time: {sched_time} ({repeat_type})\n"
                display += f"   ⏱️ Duration: {duration}s\n"
                display += f"   📅 Created: {created_at}\n\n"
            
            return display
            
        except Exception as e:
            logger.error(f"Error getting scheduled recordings: {e}")
            return f"Error loading scheduled recordings: {str(e)}"
    
    def cancel_scheduled_recording(self, station_name: str, schedule_time: str) -> str:
        """Cancel a scheduled recording"""
        try:
            conn = sqlite3.connect('radio_recordings.db')
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE scheduled_recordings 
                SET is_active = FALSE 
                WHERE station_name = ? AND schedule_time = ?
            ''', (station_name, schedule_time))
            
            if cursor.rowcount > 0:
                conn.commit()
                conn.close()
                return f"✅ Cancelled scheduled recording for {station_name} at {schedule_time}"
            else:
                conn.close()
                return f"❌ No scheduled recording found for {station_name} at {schedule_time}"
                
        except Exception as e:
            logger.error(f"Error cancelling scheduled recording: {e}")
            return f"❌ Error: {str(e)}"
    
    def export_recordings_data(self) -> str:
        """Export recordings data to CSV"""
        try:
            import csv
            conn = sqlite3.connect('radio_recordings.db')
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT station_name, station_url, country, city, duration, 
                       status, timestamp, file_size, error_message
                FROM recordings 
                ORDER BY timestamp DESC
            ''')
            
            recordings = cursor.fetchall()
            conn.close()
            
            if not recordings:
                return "No recordings to export"
            
            # Create CSV file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"recordings_export_{timestamp}.csv"
            
            with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Station Name', 'Station URL', 'Country', 'City', 
                               'Duration', 'Status', 'Timestamp', 'File Size', 'Error Message'])
                writer.writerows(recordings)
            
            return f"✅ Exported {len(recordings)} recordings to {csv_filename}"
            
        except Exception as e:
            logger.error(f"Error exporting recordings: {e}")
            return f"❌ Export error: {str(e)}"
    
    def schedule_all_stations_recording(self, interval_minutes: int, duration: int, 
                                       country_filter: str = None, max_stations: int = None) -> str:
        """Schedule recording for all radio stations with optional filtering"""
        try:
            all_stations = []
            total_stations = 0
            
            # Collect all stations based on filter
            if country_filter and country_filter != "All Countries":
                # Record all stations from specific country
                stations = self.stations_by_country.get(country_filter, [])
                for station in stations:
                    all_stations.append({
                        'name': station['name'],
                        'url': station['url'],
                        'country': country_filter,
                        'city': station.get('state', 'Unknown')
                    })
                total_stations = len(stations)
            else:
                # Record all stations from all countries
                for country, stations in self.stations_by_country.items():
                    for station in stations:
                        all_stations.append({
                            'name': station['name'],
                            'url': station['url'],
                            'country': country,
                            'city': station.get('state', 'Unknown')
                        })
                        total_stations += 1
            
            # Apply max stations limit if specified
            if max_stations and max_stations < len(all_stations):
                all_stations = all_stations[:max_stations]
                limited_count = max_stations
            else:
                limited_count = len(all_stations)
            
            # Create staggered recording jobs to avoid overwhelming the system
            scheduled_count = 0
            failed_count = 0
            
            # Calculate stagger delay (spread recordings over time to avoid system overload)
            stagger_seconds = max(10, interval_minutes * 60 // limited_count) if limited_count > 0 else 60
            
            for i, station_info in enumerate(all_stations):
                try:
                    # Create recording job for each station
                    def create_job(station_data, delay_offset):
                        def job():
                            # Add small delay to stagger the recordings
                            time.sleep(delay_offset)
                            logger.info(f"Executing bulk recording: {station_data['name']} ({station_data['country']})")
                            result = self.record_station(
                                station_data['name'],
                                station_data['url'],
                                duration,
                                station_data['country'],
                                station_data['city']
                            )
                            logger.info(f"Bulk recording result for {station_data['name']}: {result['status']}")
                        return job
                    
                    # Calculate delay for this station (stagger recordings)
                    delay_offset = (i % 10) * stagger_seconds  # Group in batches of 10
                    
                    # Schedule the job based on interval
                    job = create_job(station_info, delay_offset)
                    
                    if interval_minutes >= 60:
                        # For intervals >= 60 minutes, schedule hourly
                        hours = interval_minutes // 60
                        if hours == 1:
                            schedule.every().hour.do(job)
                        else:
                            schedule.every(hours).hours.do(job)
                    else:
                        # For intervals < 60 minutes, schedule by minutes
                        schedule.every(interval_minutes).minutes.do(job)
                    
                    # Save to database
                    conn = sqlite3.connect('radio_recordings.db')
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO scheduled_recordings 
                        (station_name, station_url, schedule_time, duration, repeat_type, 
                         interval_minutes, country, city)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (station_info['name'], station_info['url'], 
                          f"every_{interval_minutes}_min_bulk", duration, "interval_bulk",
                          interval_minutes, station_info['country'], station_info['city']))
                    conn.commit()
                    conn.close()
                    
                    scheduled_count += 1
                    
                except Exception as e:
                    logger.error(f"Error scheduling {station_info['name']}: {e}")
                    failed_count += 1
            
            result_message = f"""
✅ **Bulk Recording Scheduled Successfully!**

📊 **Summary:**
• Total Available Stations: {total_stations}
• Stations Scheduled: {scheduled_count}
• Failed to Schedule: {failed_count}
• Recording Interval: Every {interval_minutes} minutes
• Recording Duration: {duration} seconds per station
• Country Filter: {country_filter or 'All Countries'}

⚙️ **System Optimization:**
• Recordings are staggered by {stagger_seconds} seconds to prevent system overload
• Maximum {min(10, scheduled_count)} concurrent recordings at any time
• Folder structure: recordings/Country/City/station_timestamp.mp3

⚠️ **Important Notes:**
• This will consume significant disk space and bandwidth
• Estimated storage per cycle: ~{(scheduled_count * duration * 128) // 8192} MB
• Monitor system resources during bulk recording
• Use 'Schedule Management' tab to cancel if needed
"""
            
            if country_filter and country_filter != "All Countries":
                result_message += f"\n🌍 Recording all stations from: {country_filter}"
            else:
                result_message += f"\n🌐 Recording ALL stations from ALL countries"
            
            return result_message
            
        except Exception as e:
            logger.error(f"Error scheduling bulk recording: {e}")
            return f"❌ Error scheduling bulk recording: {str(e)}"
    
    def record_all_stations_now(self, duration: int, country_filter: str = None, 
                               max_concurrent: int = 5, max_stations: int = None) -> str:
        """Record all radio stations immediately with parallel processing"""
        try:
            all_stations = []
            total_stations = 0
            
            # Collect all stations based on filter
            if country_filter and country_filter != "All Countries":
                stations = self.stations_by_country.get(country_filter, [])
                for station in stations:
                    all_stations.append({
                        'name': station['name'],
                        'url': station['url'],
                        'country': country_filter,
                        'city': station.get('state', 'Unknown')
                    })
                total_stations = len(stations)
            else:
                for country, stations in self.stations_by_country.items():
                    for station in stations:
                        all_stations.append({
                            'name': station['name'],
                            'url': station['url'],
                            'country': country,
                            'city': station.get('state', 'Unknown')
                        })
                        total_stations += 1
            
            # Apply max stations limit
            if max_stations and max_stations < len(all_stations):
                all_stations = all_stations[:max_stations]
            
            if not all_stations:
                return f"❌ No stations found for recording"
            
            # Record all stations with threading
            results = []
            successful_recordings = 0
            failed_recordings = 0
            
            def record_single_station(station_info):
                try:
                    result = self.record_station(
                        station_info['name'],
                        station_info['url'],
                        duration,
                        station_info['country'],
                        station_info['city']
                    )
                    return {
                        'station': station_info['name'],
                        'country': station_info['country'],
                        'status': result['status'],
                        'filename': result.get('filename', 'N/A'),
                        'error': result.get('error', None)
                    }
                except Exception as e:
                    return {
                        'station': station_info['name'],
                        'country': station_info['country'],
                        'status': 'failed',
                        'filename': 'N/A',
                        'error': str(e)
                    }
            
            # Process in batches to avoid overwhelming the system
            batch_size = max_concurrent
            total_batches = (len(all_stations) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min(start_idx + batch_size, len(all_stations))
                batch_stations = all_stations[start_idx:end_idx]
                
                logger.info(f"Processing batch {batch_num + 1}/{total_batches} ({len(batch_stations)} stations)")
                
                with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                    batch_futures = [executor.submit(record_single_station, station) 
                                   for station in batch_stations]
                    
                    for future in batch_futures:
                        try:
                            result = future.result(timeout=duration + 60)
                            results.append(result)
                            
                            if result['status'] == 'success':
                                successful_recordings += 1
                            else:
                                failed_recordings += 1
                                
                        except Exception as e:
                            failed_recordings += 1
                            results.append({
                                'station': 'Unknown',
                                'country': 'Unknown',
                                'status': 'failed',
                                'filename': 'N/A',
                                'error': f"Timeout or error: {str(e)}"
                            })
                
                # Small delay between batches
                if batch_num < total_batches - 1:
                    time.sleep(2)
            
            # Generate detailed results
            success_rate = (successful_recordings / len(results) * 100) if results else 0
            
            result_summary = f"""
🎵 **Bulk Recording Completed!**

📊 **Summary:**
• Total Stations Processed: {len(results)}
• Successful Recordings: {successful_recordings}
• Failed Recordings: {failed_recordings}
• Success Rate: {success_rate:.1f}%
• Recording Duration: {duration} seconds each
• Country Filter: {country_filter or 'All Countries'}

📁 **Storage:**
• Files saved in: recordings/Country/City/
• Estimated total size: ~{(successful_recordings * duration * 128) // 8192} MB

🎯 **Results by Country:**
"""
            
            # Group results by country
            results_by_country = {}
            for result in results:
                country = result['country']
                if country not in results_by_country:
                    results_by_country[country] = {'success': 0, 'failed': 0, 'stations': []}
                
                if result['status'] == 'success':
                    results_by_country[country]['success'] += 1
                else:
                    results_by_country[country]['failed'] += 1
                
                results_by_country[country]['stations'].append(result)
            
            # Display country summaries
            for country, country_data in sorted(results_by_country.items()):
                total_country = country_data['success'] + country_data['failed']
                country_rate = (country_data['success'] / total_country * 100) if total_country > 0 else 0
                
                result_summary += f"\n🌍 {country}: {country_data['success']}/{total_country} successful ({country_rate:.1f}%)"
            
            # Show first 20 detailed results
            result_summary += f"\n\n📋 **Detailed Results** (showing first 20):"
            for i, result in enumerate(results[:20]):
                status_icon = "✅" if result['status'] == 'success' else "❌"
                result_summary += f"\n{status_icon} {result['station']} ({result['country']})"
                if result['status'] == 'success':
                    result_summary += f" → {result['filename']}"
                else:
                    result_summary += f" → {result['error'][:50]}..."
            
            if len(results) > 20:
                result_summary += f"\n... and {len(results) - 20} more results"
            
            return result_summary
            
        except Exception as e:
            logger.error(f"Error in bulk recording: {e}")
            return f"❌ Bulk recording error: {str(e)}"
    
    def get_storage_info(self) -> str:
        """Get detailed storage information for recordings with folder structure"""
        try:
            recordings_dir = "recordings"
            if not os.path.exists(recordings_dir):
                return "📁 No recordings directory found"
            
            total_size = 0
            file_count = 0
            folder_structure = {}
            
            # Walk through the directory structure
            for root, dirs, files in os.walk(recordings_dir):
                path_parts = root.replace(recordings_dir, "").strip(os.sep).split(os.sep)
                
                for file in files:
                    if file.endswith('.mp3'):
                        file_path = os.path.join(root, file)
                        file_size = os.path.getsize(file_path)
                        total_size += file_size
                        file_count += 1
                        
                        # Organize by country/city structure
                        if len(path_parts) >= 2 and path_parts[0]:
                            country = path_parts[0].replace("_", " ")
                            city = path_parts[1].replace("_", " ") if len(path_parts) > 1 else "Unknown"
                            
                            if country not in folder_structure:
                                folder_structure[country] = {}
                            if city not in folder_structure[country]:
                                folder_structure[country][city] = {'files': 0, 'size': 0}
                            
                            folder_structure[country][city]['files'] += 1
                            folder_structure[country][city]['size'] += file_size
            
            total_size_mb = total_size / (1024 * 1024)
            total_size_gb = total_size_mb / 1024
            
            storage_info = f"""
📁 **Storage Information**

📊 **Summary:**
• Total Files: {file_count}
• Total Size: {total_size_mb:.1f} MB ({total_size_gb:.2f} GB)
• Directory: {os.path.abspath(recordings_dir)}

🗂️ **Folder Structure:**
"""
            
            # Display folder structure organized by country/city
            for country, cities in sorted(folder_structure.items()):
                country_files = sum(city_data['files'] for city_data in cities.values())
                country_size = sum(city_data['size'] for city_data in cities.values()) / (1024 * 1024)
                
                storage_info += f"\n📍 {country} ({country_files} files, {country_size:.1f} MB)\n"
                
                for city, city_data in sorted(cities.items()):
                    city_size_mb = city_data['size'] / (1024 * 1024)
                    storage_info += f"   └── {city}: {city_data['files']} files, {city_size_mb:.1f} MB\n"
            
            # Add warnings and recommendations
            if total_size_gb > 5:
                storage_info += "\n⚠️ **Storage Warning**: Consider archiving old recordings (>5GB used)"
            
            if file_count > 1000:
                storage_info += "\n💡 **Tip**: Use the cleanup feature to manage old files"
            
            storage_info += f"\n\n📂 **Folder Path Pattern**: recordings/Country/City/station_timestamp.mp3"
            
            return storage_info
            
        except Exception as e:
            logger.error(f"Error getting storage info: {e}")
            return f"❌ Storage info error: {str(e)}"

    def create_interface(self):
        """Create the Gradio interface"""
        with gr.Blocks(title="Radio Recording Dashboard", theme=gr.themes.Soft()) as app:
            gr.Markdown("# 🎵 Radio Recording Dashboard")
            gr.Markdown("Automated radio recording with scheduling, monitoring, and statistics")
            
            with gr.Tabs():
                # Tab 1: Manual Recording
                with gr.Tab("🎙️ Manual Recording"):
                    with gr.Row():
                        with gr.Column(scale=1):
                            country_dropdown = gr.Dropdown(
                                choices=self.get_countries(),
                                label="Select Country",
                                interactive=True
                            )
                            
                            city_dropdown = gr.Dropdown(
                                choices=[],
                                label="Select City (optional)",
                                interactive=True
                            )
                            
                            station_dropdown = gr.Dropdown(
                                choices=[],
                                label="Select Station",
                                interactive=True
                            )
                            
                            duration_slider = gr.Slider(
                                minimum=10,
                                maximum=300,
                                value=60,
                                step=10,
                                label="Recording Duration (seconds)"
                            )
                            
                            record_button = gr.Button("🎬 Start Recording", variant="primary")
                            
                            def update_cities(country):
                                cities = self.get_cities_by_country(country)
                                return gr.Dropdown(choices=cities, value=None)
                            
                            def update_stations(country, city):
                                stations = self.get_stations_by_location(country, city)
                                station_names = [s['name'] for s in stations[:20]]  # Limit to 20
                                return gr.Dropdown(choices=station_names, value=None)
                            
                            country_dropdown.change(
                                fn=update_cities,
                                inputs=[country_dropdown],
                                outputs=[city_dropdown]
                            )
                            
                            country_dropdown.change(
                                fn=update_stations,
                                inputs=[country_dropdown, city_dropdown],
                                outputs=[station_dropdown]
                            )
                            
                            city_dropdown.change(
                                fn=update_stations,
                                inputs=[country_dropdown, city_dropdown],
                                outputs=[station_dropdown]
                            )
                        
                        with gr.Column(scale=1):
                            recording_output = gr.Textbox(
                                label="Recording Status",
                                lines=5,
                                interactive=False
                            )
                            
                            record_button.click(
                                fn=self.manual_record,
                                inputs=[country_dropdown, city_dropdown, station_dropdown, duration_slider],
                                outputs=[recording_output]
                            )
                
                # Tab 2: Interval Scheduling (Enhanced with All Stations Option)
                with gr.Tab("🔄 Automated Recording"):
                    with gr.Tabs():
                        # Sub-tab 1: Single Station Interval Recording
                        with gr.Tab("🎵 Single Station"):
                            with gr.Row():
                                with gr.Column():
                                    interval_country = gr.Dropdown(
                                        choices=self.get_countries(),
                                        label="Select Country"
                                    )
                                    
                                    interval_city = gr.Dropdown(
                                        choices=[],
                                        label="Select City (optional)"
                                    )
                                    
                                    interval_station = gr.Dropdown(
                                        choices=[],
                                        label="Select Station"
                                    )
                                    
                                    interval_minutes = gr.Slider(
                                        minimum=5,
                                        maximum=720,  # 12 hours
                                        value=60,     # 1 hour default
                                        step=5,
                                        label="Recording Interval (minutes)"
                                    )
                                    
                                    interval_duration = gr.Slider(
                                        minimum=10,
                                        maximum=300,
                                        value=60,
                                        step=10,
                                        label="Recording Duration (seconds)"
                                    )
                                    
                                    interval_button = gr.Button("🔄 Start Interval Recording", variant="primary")
                                    
                                    def update_interval_cities(country):
                                        cities = self.get_cities_by_country(country)
                                        return gr.Dropdown(choices=cities, value=None)
                                    
                                    def update_interval_stations(country, city):
                                        stations = self.get_stations_by_location(country, city)
                                        station_names = [s['name'] for s in stations[:20]]
                                        return gr.Dropdown(choices=station_names, value=None)
                                    
                                    interval_country.change(
                                        fn=update_interval_cities,
                                        inputs=[interval_country],
                                        outputs=[interval_city]
                                    )
                                    
                                    interval_country.change(
                                        fn=update_interval_stations,
                                        inputs=[interval_country, interval_city],
                                        outputs=[interval_station]
                                    )
                                    
                                    interval_city.change(
                                        fn=update_interval_stations,
                                        inputs=[interval_country, interval_city],
                                        outputs=[interval_station]
                                    )
                                
                                with gr.Column():
                                    interval_output = gr.Textbox(
                                        label="Interval Schedule Status",
                                        lines=10,
                                        interactive=False
                                    )
                                    
                                    def schedule_interval_wrapper(country, city, station, interval_min, duration):
                                        try:
                                            stations = self.get_stations_by_location(country, city)
                                            selected_station = None
                                            for s in stations:
                                                if s['name'] == station:
                                                    selected_station = s
                                                    break
                                            
                                            if not selected_station:
                                                return f"❌ Station '{station}' not found"
                                            
                                            return self.schedule_interval_recording(
                                                selected_station['name'],
                                                selected_station['url'],
                                                interval_min,
                                                duration,
                                                country,
                                                city
                                            )
                                        except Exception as e:
                                            return f"❌ Error: {str(e)}"
                                    
                                    interval_button.click(
                                        fn=schedule_interval_wrapper,
                                        inputs=[interval_country, interval_city, interval_station, interval_minutes, interval_duration],
                                        outputs=[interval_output]
                                    )
                        
                        # Sub-tab 2: All Stations Automation
                        with gr.Tab("🌐 All Stations"):
                            with gr.Row():
                                with gr.Column():
                                    gr.Markdown("### 🚨 **Bulk Recording Configuration**")
                                    gr.Markdown("⚠️ **Warning**: This will record ALL radio stations. Use with caution!")
                                    
                                    bulk_country_filter = gr.Dropdown(
                                        choices=["All Countries"] + self.get_countries(),
                                        value="All Countries",
                                        label="Country Filter"
                                    )
                                    
                                    bulk_interval_minutes = gr.Slider(
                                        minimum=30,  # Minimum 30 minutes for bulk operations
                                        maximum=1440,  # 24 hours max
                                        value=120,     # 2 hours default
                                        step=30,
                                        label="Recording Interval (minutes)"
                                    )
                                    
                                    bulk_duration = gr.Slider(
                                        minimum=10,
                                        maximum=180,  # Max 3 minutes per station for bulk
                                        value=30,     # 30 seconds default
                                        step=10,
                                        label="Recording Duration per Station (seconds)"
                                    )
                                    
                                    bulk_max_stations = gr.Slider(
                                        minimum=10,
                                        maximum=1000,
                                        value=100,
                                        step=10,
                                        label="Max Stations to Schedule (0 = all)"
                                    )
                                    
                                    with gr.Row():
                                        bulk_schedule_button = gr.Button("⏰ Schedule All Stations", variant="primary")
                                        bulk_record_now_button = gr.Button("🔴 Record All Now", variant="secondary")
                                    
                                    gr.Markdown("### 📊 **Instant Recording Options**")
                                    
                                    instant_max_concurrent = gr.Slider(
                                        minimum=1,
                                        maximum=10,
                                        value=3,
                                        step=1,
                                        label="Max Concurrent Recordings"
                                    )
                                    
                                    instant_max_stations = gr.Slider(
                                        minimum=5,
                                        maximum=200,
                                        value=50,
                                        step=5,
                                        label="Max Stations for Instant Recording"
                                    )
                                
                                with gr.Column():
                                    bulk_output = gr.Textbox(
                                        label="Bulk Recording Status",
                                        lines=20,
                                        interactive=False,
                                        placeholder="Configure settings and click 'Schedule All Stations' or 'Record All Now'..."
                                    )
                                    
                                    def get_bulk_preview(country_filter, max_stations_limit):
                                        """Preview how many stations will be affected"""
                                        try:
                                            total_count = 0
                                            country_breakdown = {}
                                            
                                            if country_filter == "All Countries":
                                                for country, stations in self.stations_by_country.items():
                                                    count = len(stations)
                                                    total_count += count
                                                    country_breakdown[country] = count
                                            else:
                                                stations = self.stations_by_country.get(country_filter, [])
                                                total_count = len(stations)
                                                country_breakdown[country_filter] = total_count
                                            
                                            limited_count = min(total_count, max_stations_limit) if max_stations_limit > 0 else total_count
                                            
                                            preview = f"""
📊 **Recording Preview:**

🎯 **Scope:** {country_filter}
📻 **Total Stations Available:** {total_count}
📋 **Stations to Process:** {limited_count}
"""
                                            
                                            if country_filter == "All Countries":
                                                preview += f"\n🌍 **Breakdown by Country:**"
                                                for country, count in sorted(country_breakdown.items(), key=lambda x: x[1], reverse=True)[:10]:
                                                    preview += f"\n• {country}: {count} stations"
                                                if len(country_breakdown) > 10:
                                                    preview += f"\n• ... and {len(country_breakdown) - 10} more countries"
                                            
                                            return preview
                                            
                                        except Exception as e:
                                            return f"❌ Error generating preview: {str(e)}"
                                    
                                    # Preview updates
                                    def update_preview():
                                        return get_bulk_preview(bulk_country_filter.value, bulk_max_stations.value)
                                    
                                    bulk_country_filter.change(
                                        fn=lambda country, max_stations: get_bulk_preview(country, max_stations),
                                        inputs=[bulk_country_filter, bulk_max_stations],
                                        outputs=[bulk_output]
                                    )
                                    
                                    bulk_max_stations.change(
                                        fn=lambda country, max_stations: get_bulk_preview(country, max_stations),
                                        inputs=[bulk_country_filter, bulk_max_stations],
                                        outputs=[bulk_output]
                                    )
                                    
                                    # Button actions
                                    def schedule_all_wrapper(country_filter, interval_min, duration, max_stations):
                                        max_stations_value = max_stations if max_stations > 0 else None
                                        return self.schedule_all_stations_recording(
                                            interval_min, duration, country_filter, max_stations_value
                                        )
                                    
                                    def record_all_now_wrapper(country_filter, duration, max_concurrent, max_stations):
                                        max_stations_value = max_stations if max_stations > 0 else None
                                        return self.record_all_stations_now(
                                            duration, country_filter, max_concurrent, max_stations_value
                                        )
                                    
                                    bulk_schedule_button.click(
                                        fn=schedule_all_wrapper,
                                        inputs=[bulk_country_filter, bulk_interval_minutes, bulk_duration, bulk_max_stations],
                                        outputs=[bulk_output]
                                    )
                                    
                                    bulk_record_now_button.click(
                                        fn=record_all_now_wrapper,
                                        inputs=[bulk_country_filter, bulk_duration, instant_max_concurrent, instant_max_stations],
                                        outputs=[bulk_output]
                                    )
                
                # Tab 3: Detailed Statistics & Monitoring
                with gr.Tab("📊 Detailed Statistics"):
                    with gr.Row():
                        with gr.Column():
                            detailed_stats_display = gr.Textbox(
                                label="Comprehensive Statistics",
                                lines=20,
                                interactive=False,
                                value=self.get_detailed_statistics()
                            )
                            
                            refresh_detailed_stats_button = gr.Button("🔄 Refresh Detailed Statistics")
                            
                            def refresh_detailed_stats():
                                self.load_statistics()
                                return self.get_detailed_statistics()
                            
                            refresh_detailed_stats_button.click(
                                fn=refresh_detailed_stats,
                                outputs=[detailed_stats_display]
                            )
                        
                        with gr.Column():
                            recent_recordings = gr.Textbox(
                                label="Recent Recordings",
                                lines=20,
                                interactive=False,
                                value=self.get_recent_recordings()
                            )
                            
                            refresh_recordings_button = gr.Button("🔄 Refresh Recent Recordings")
                            
                            refresh_recordings_button.click(
                                fn=self.get_recent_recordings,
                                outputs=[recent_recordings]
                            )
                
                # Tab 4: Batch Recording
                with gr.Tab("📦 Batch Recording"):
                    with gr.Row():
                        with gr.Column():
                            batch_country = gr.Dropdown(
                                choices=self.get_countries(),
                                label="Select Country"
                            )
                            
                            batch_city = gr.Dropdown(
                                choices=[],
                                label="Select City (optional)"
                            )
                            
                            batch_duration = gr.Slider(
                                minimum=10,
                                maximum=300,
                                value=30,
                                step=10,
                                label="Recording Duration (seconds)"
                            )
                            
                            batch_max_stations = gr.Slider(
                                minimum=1,
                                maximum=10,
                                value=3,
                                step=1,
                                label="Max Stations to Record"
                            )
                            
                            batch_record_button = gr.Button("📦 Start Batch Recording", variant="primary")
                            
                            def update_batch_cities(country):
                                cities = self.get_cities_by_country(country)
                                return gr.Dropdown(choices=cities, value=None)
                            
                            batch_country.change(
                                fn=update_batch_cities,
                                inputs=[batch_country],
                                outputs=[batch_city]
                            )
                        
                        with gr.Column():
                            batch_output = gr.Textbox(
                                label="Batch Recording Results",
                                lines=15,
                                interactive=False
                            )
                            
                            def batch_record_enhanced(country, city, duration, max_stations, max_concurrent):
                                """Enhanced batch recording with better control"""
                                try:
                                    stations = self.get_stations_by_location(country, city)
                                    
                                    if not stations:
                                        return f"❌ No stations found for {city or country}"
                                    
                                    # Limit number of stations
                                    stations = stations[:max_stations]
                                    results = []
                                    successful = 0
                                    failed = 0
                                    
                                    def record_station_wrapper(station):
                                        return {
                                            'station_name': station['name'],
                                            'result': self.record_station(
                                                station['name'],
                                                station['url'],
                                                duration,
                                                country,
                                                city
                                            )
                                        }
                                    
                                    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
                                        # Submit recording jobs
                                        futures = []
                                        for station in stations:
                                            future = executor.submit(record_station_wrapper, station)
                                            futures.append(future)
                                        
                                        # Collect results
                                        for future in futures:
                                            try:
                                                result_data = future.result(timeout=duration + 60)
                                                station_name = result_data['station_name']
                                                result = result_data['result']
                                                
                                                if result['status'] == 'success':
                                                    results.append(f"✅ {station_name}: {result['filename']}")
                                                    successful += 1
                                                else:
                                                    results.append(f"❌ {station_name}: {result['error']}")
                                                    failed += 1
                                            except Exception as e:
                                                results.append(f"❌ Unknown station: Timeout or error")
                                                failed += 1
                                    
                                    summary = f"""
📊 **Batch Recording Summary:**
• Location: {city or country}
• Total Stations: {len(stations)}
• Successful: {successful}
• Failed: {failed}
• Success Rate: {(successful/len(stations)*100):.1f}%

📋 **Detailed Results:**
{chr(10).join(results)}
"""
                                    return summary
                                    
                                except Exception as e:
                                    logger.error(f"Error in batch recording: {e}")
                                    return f"❌ Batch recording error: {str(e)}"
                            
                            batch_record_button.click(
                                fn=batch_record_enhanced,
                                inputs=[batch_country, batch_city, batch_duration, batch_max_stations],
                                outputs=[batch_output]
                            )
                
                # Tab 5: Scheduled Recordings Management
                with gr.Tab("⏰ Schedule Management"):
                    with gr.Row():
                        with gr.Column():
                            scheduled_display = gr.Textbox(
                                label="Active Scheduled Recordings",
                                lines=15,
                                interactive=False,
                                value=self.get_scheduled_recordings()
                            )
                            
                            refresh_scheduled_button = gr.Button("🔄 Refresh Scheduled")
                            
                            refresh_scheduled_button.click(
                                fn=self.get_scheduled_recordings,
                                outputs=[scheduled_display]
                            )
                        
                        with gr.Column():
                            gr.Markdown("### Cancel Scheduled Recording")
                            
                            cancel_station = gr.Textbox(
                                label="Station Name",
                                placeholder="Enter exact station name"
                            )
                            
                            cancel_time = gr.Textbox(
                                label="Schedule Time",
                                placeholder="HH:MM format"
                            )
                            
                            cancel_button = gr.Button("❌ Cancel Recording", variant="secondary")
                            
                            cancel_output = gr.Textbox(
                                label="Cancel Status",
                                lines=3,
                                interactive=False
                            )
                            
                            cancel_button.click(
                                fn=self.cancel_scheduled_recording,
                                inputs=[cancel_station, cancel_time],
                                outputs=[cancel_output]
                            )
                
                # Tab 6: Storage & Export
                with gr.Tab("💾 Storage & Export"):
                    with gr.Row():
                        with gr.Column():
                            storage_display = gr.Textbox(
                                label="Storage Information",
                                lines=10,
                                interactive=False,
                                value=self.get_storage_info()
                            )
                            
                            refresh_storage_button = gr.Button("🔄 Refresh Storage Info")
                            
                            refresh_storage_button.click(
                                fn=self.get_storage_info,
                                outputs=[storage_display]
                            )
                        
                        with gr.Column():
                            gr.Markdown("### Export Data")
                            
                            export_button = gr.Button("📊 Export Recordings to CSV", variant="primary")
                            
                            export_output = gr.Textbox(
                                label="Export Status",
                                lines=5,
                                interactive=False
                            )
                            
                            export_button.click(
                                fn=self.export_recordings_data,
                                outputs=[export_output]
                            )
                            
                            gr.Markdown("### Cleanup")
                            gr.Markdown("⚠️ **Warning**: This will delete old recording files")
                            
                            cleanup_days = gr.Slider(
                                minimum=1,
                                maximum=30,
                                value=7,
                                step=1,
                                label="Delete files older than (days)"
                            )
                            
                            cleanup_button = gr.Button("🗑️ Cleanup Old Files", variant="secondary")
                            
                            def cleanup_old_files(days):
                                try:
                                    recordings_dir = "recordings"
                                    if not os.path.exists(recordings_dir):
                                        return "No recordings directory found"
                                    
                                    cutoff_date = datetime.now() - timedelta(days=days)
                                    deleted_count = 0
                                    total_size = 0
                                    
                                    for root, dirs, files in os.walk(recordings_dir):
                                        for file in files:
                                            if file.endswith('.mp3'):
                                                file_path = os.path.join(root, file)
                                                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                                                
                                                if file_time < cutoff_date:
                                                    file_size = os.path.getsize(file_path)
                                                    os.remove(file_path)
                                                    deleted_count += 1
                                                    total_size += file_size
                                    
                                    total_size_mb = total_size / (1024 * 1024)
                                    return f"🗑️ Deleted {deleted_count} files, freed {total_size_mb:.1f} MB"
                                    
                                except Exception as e:
                                    return f"❌ Cleanup error: {str(e)}"
                            
                            cleanup_output = gr.Textbox(
                                label="Cleanup Status",
                                lines=3,
                                interactive=False
                            )
                            
                            cleanup_button.click(
                                fn=cleanup_old_files,
                                inputs=[cleanup_days],
                                outputs=[cleanup_output]
                            )
                
                # Tab 7: Complete Station Status
                with gr.Tab("🌐 All Stations Status"):
                    with gr.Row():
                        with gr.Column(scale=1):
                            gr.Markdown("### Connection Testing")
                            
                            test_all_button = gr.Button("🔍 Test All Stations", variant="primary", size="lg")
                            
                            gr.Markdown("### Quick Filters")
                            filter_country = gr.Dropdown(
                                choices=["All Countries"] + self.get_countries(),
                                value="All Countries",
                                label="Filter by Country"
                            )
                            
                            filter_status = gr.Dropdown(
                                choices=["All Status", "Online", "Offline", "Untested"],
                                value="All Status",
                                label="Filter by Status"
                            )
                            
                            max_results = gr.Slider(
                                minimum=10,
                                maximum=500,
                                value=100,
                                step=10,
                                label="Max Results to Show"
                            )
                            
                            apply_filters_button = gr.Button("🔍 Apply Filters")
                        
                        with gr.Column(scale=3):
                            all_stations_display = gr.Textbox(
                                label="Complete Station Status",
                                lines=25,
                                interactive=False,
                                placeholder="Click 'Test All Stations' to check connection status for all radio stations..."
                            )
                    
                    def get_filtered_stations_status(country_filter, status_filter, max_results_count):
                        """Get filtered station status display"""
                        try:
                            stations_status = self.get_all_stations_status()
                            
                            if 'error' in stations_status:
                                return f"❌ Error loading stations: {stations_status['error']}"
                            
                            # Filter stations
                            filtered_stations = []
                            for station in stations_status['detailed_status']:
                                # Apply country filter
                                if country_filter != "All Countries" and station['country'] != country_filter:
                                    continue
                                
                                # Apply status filter
                                if status_filter != "All Status":
                                    if status_filter.lower() != station['status'].lower():
                                        continue
                                
                                filtered_stations.append(station)
                            
                            # Limit results
                            filtered_stations = filtered_stations[:max_results_count]
                            
                            # Build display
                            display = f"""
🌐 **Complete Radio Stations Status**

📊 **Summary:**
• Total Stations: {stations_status['total_stations']}
• Online: {stations_status['online_stations']} ({(stations_status['online_stations']/stations_status['total_stations']*100) if stations_status['total_stations'] > 0 else 0:.1f}%)
• Offline: {stations_status['offline_stations']} ({(stations_status['offline_stations']/stations_status['total_stations']*100) if stations_status['total_stations'] > 0 else 0:.1f}%)
• Untested: {stations_status['untested_stations']} ({(stations_status['untested_stations']/stations_status['total_stations']*100) if stations_status['total_stations'] > 0 else 0:.1f}%)

🔍 **Filtered Results:** ({len(filtered_stations)} stations shown)

"""
                            
                            # Group by country for better organization
                            stations_by_country = {}
                            for station in filtered_stations:
                                country = station['country']
                                if country not in stations_by_country:
                                    stations_by_country[country] = []
                                stations_by_country[country].append(station)
                            
                            # Display stations grouped by country
                            for country, country_stations in sorted(stations_by_country.items()):
                                display += f"\n🇾🇪 **{country}** ({len(country_stations)} stations):\n"
                                
                                for station in country_stations:
                                    status_icon = {
                                        'online': '🟢',
                                        'offline': '🔴',
                                        'untested': '⚪'
                                    }.get(station['status'], '❓')
                                    
                                    display += f"{status_icon} {station['name']}\n"
                                    display += f"   📍 {station['city']}\n"
                                    display += f"   🎵 {station['bitrate']} kbps, 🗣️ {station['language']}\n"
                                    display += f"   🔗 Status: {station['status'].title()}\n"
                                    display += f"   🕐 Last Check: {station['last_check']}\n\n"
                            
                            return display
                            
                        except Exception as e:
                            logger.error(f"Error getting filtered stations status: {e}")
                            return f"❌ Error: {str(e)}"
                    
                    # Button actions
                    test_all_button.click(
                        fn=self.test_all_stations_connection,
                        outputs=[all_stations_display]
                    )
                    
                    apply_filters_button.click(
                        fn=get_filtered_stations_status,
                        inputs=[filter_country, filter_status, max_results],
                        outputs=[all_stations_display]
                    )
            
            gr.Markdown("---")
            gr.Markdown("### 📁 Recordings Structure: `recordings/Country/City/station_timestamp.mp3`")
            gr.Markdown("### 🔄 Interval Recording: Automatic recording every X minutes/hours")
            gr.Markdown("### 🗄️ All data stored in `radio_recordings.db`")
        
        return app
    
    def cleanup(self):
        """Cleanup resources"""
        self.is_running = False
        schedule.clear()

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print("Shutting down dashboard...")
    sys.exit(0)

def main():
    """Main function to run the dashboard"""
    try:
        # Set up signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        dashboard = RadioDashboard()
        interface = dashboard.create_interface()
        
        print("🚀 Starting Radio Recording Dashboard...")
        print("📱 Open your browser and go to the URL shown below")
        print("🎵 Features: Manual recording, Scheduling, Statistics, Connection monitoring")
        
        interface.launch(
            share=True,
            show_error=True
        )
        
    except Exception as e:
        logger.error(f"Error starting dashboard: {e}")
        print(f"❌ Error: {e}")
    finally:
        if 'dashboard' in locals():
            dashboard.cleanup()

if __name__ == "__main__":
    main()