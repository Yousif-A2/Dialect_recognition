#!/usr/bin/env python3
"""
Gradio Web App for Arabic Radio Station Scraping and Recording
A user-friendly interface for browsing and recording Arabic radio stations.
"""

import gradio as gr
import json
import os
import subprocess
import time
from datetime import datetime
from typing import Dict, List
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gradio_app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GradioRadioApp:
    """Gradio web application for Arabic radio station management"""
    
    def __init__(self, data_file: str = 'arab_stations_radio_browser.json'):
        self.data_file = data_file
        self.stations_by_country = {}
        self.stations_by_city = {}
        self.load_stations()
    
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
    
    def get_countries(self) -> List[str]:
        """Get list of available Arabic countries only"""
        arabic_countries = [
            'Algeria', 'Bahrain', 'Jordan', 'Kuwait', 'Lebanon', 'Egypt', 'Iraq',
            'Libya', 'Morocco', 'Oman', 'Palestine', 'Saudi Arabia', 'Somalia',
            'Sudan', 'United Arab Emirates', 'Tunisia', 'Syria', 'Yemen', 'Qatar'
        ]
        
        # Filter to only include countries that have data
        available_countries = []
        for country in arabic_countries:
            if country in self.stations_by_country:
                available_countries.append(country)
        
        return sorted(available_countries)
    
    def get_cities_by_country(self, country: str) -> List[str]:
        """Get cities for a specific country"""
        try:
            if not country or not country.strip():
                return []
            
            cities = set()
            stations = self.stations_by_country.get(country, [])
            
            for station in stations:
                try:
                    if station.get('state') and station['state'].strip():
                        cities.add(station['state'].strip())
                except Exception as e:
                    logger.debug(f"Error processing station state: {e}")
                    continue
            
            return sorted(list(cities))
            
        except Exception as e:
            logger.error(f"Error getting cities for {country}: {e}")
            return []
    
    def get_stations_display(self, country: str, city: str) -> str:
        """Get stations for a specific location"""
        try:
            # Handle None values and empty strings
            country = country if country else ""
            city = city if city else ""
            
            if city and city.strip():  # Check if city is not empty
                stations = self.stations_by_city.get(city, [])
                location = city
            elif country and country.strip():  # Check if country is not empty
                stations = self.stations_by_country.get(country, [])
                location = country
            else:
                return "Please select a country or city"
            
            if not stations:
                return f"No stations found for {location}"
            
            display = f"📻 Stations in {location} ({len(stations)} total):\n\n"
            for i, station in enumerate(stations[:10], 1):
                try:
                    display += f"{i}. {station.get('name', 'Unknown Station')}\n"
                    if station.get('state'):
                        display += f"   📍 {station['state']}\n"
                    if station.get('language'):
                        display += f"   🗣️ {station['language']}\n"
                    display += f"   👍 {station.get('votes', 0)} votes\n"
                    display += f"   🎵 {station.get('bitrate', 'Unknown')} kbps\n\n"
                except Exception as e:
                    display += f"{i}. Error displaying station: {str(e)}\n\n"
            
            if len(stations) > 10:
                display += f"... and {len(stations) - 10} more stations"
            
            return display
            
        except Exception as e:
            logger.error(f"Error in get_stations_display: {e}")
            return f"Error loading stations: {str(e)}"
    
    def record_stations(self, country: str, city: str, duration: int, count: int) -> str:
        """Record multiple radio streams"""
        try:
            if city and city.strip():  # Check if city is not empty
                stations = self.stations_by_city.get(city, [])
                location = city
            elif country and country.strip():  # Check if country is not empty
                stations = self.stations_by_country.get(country, [])
                location = country
            else:
                return "Please select a country or city"
            
            if not stations:
                return f"No stations found for {location}"
            
            stations = stations[:count]
            os.makedirs("recordings", exist_ok=True)
            
            results = []
            for i, station in enumerate(stations):
                print(f"Recording {i+1}/{len(stations)}: {station['name']}")
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                safe_name = "".join(c for c in station['name'] if c.isalnum() or c in (' ', '-', '_')).rstrip()
                filename = f"{safe_name}_{timestamp}.mp3"
                filepath = os.path.join("recordings", filename)
                
                cmd = ['ffmpeg', '-i', station['url'], '-t', str(duration), 
                       '-c:a', 'mp3', '-b:a', '128k', '-y', filepath]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=duration+30)
                
                if result.returncode == 0:
                    results.append(f"✅ {station['name']}: {filename}")
                else:
                    results.append(f"❌ {station['name']}: Failed - {result.stderr[:100]}")
                
                if i < len(stations) - 1:
                    time.sleep(2)
            
            return f"Recording completed!\n\n" + "\n".join(results)
            
        except Exception as e:
            return f"Error: {str(e)}"
    
    def create_interface(self):
        """Create the Gradio interface"""
        
        with gr.Blocks(title="Arabic Radio Recorder", theme=gr.themes.Soft()) as app:
            gr.Markdown("# 🎵 Arabic Radio Station Recorder")
            gr.Markdown("Browse and record Arabic radio stations from different countries and cities")
            
            with gr.Row():
                with gr.Column(scale=1):
                    gr.Markdown("## 📍 Location Selection")
                    
                    # Get initial country for default value
                    initial_country = self.get_countries()[0] if self.get_countries() else ""
                    initial_stations = self.get_stations_display(initial_country, "") if initial_country else "Please select a country"
                    
                    country_dropdown = gr.Dropdown(
                        choices=self.get_countries(),
                        label="Select Country",
                        value=initial_country,
                        interactive=True
                    )
                    
                    city_dropdown = gr.Dropdown(
                        choices=[],
                        label="Select City (optional)",
                        interactive=True
                    )
                    
                    # Update cities when country changes
                    def update_cities(country):
                        try:
                            cities = self.get_cities_by_country(country)
                            return gr.Dropdown(choices=cities, value=None)
                        except Exception as e:
                            print(f"Error updating cities: {e}")
                            return gr.Dropdown(choices=[], value=None)
                    
                    country_dropdown.change(
                        fn=update_cities,
                        inputs=[country_dropdown],
                        outputs=[city_dropdown]
                    )
                    
                    gr.Markdown("## ⚙️ Recording Settings")
                    
                    duration_slider = gr.Slider(
                        minimum=10,
                        maximum=300,
                        value=30,
                        step=10,
                        label="Recording Duration (seconds)"
                    )
                    
                    count_slider = gr.Slider(
                        minimum=1,
                        maximum=20,
                        value=5,
                        step=1,
                        label="Number of Stations to Record"
                    )
                    
                    start_button = gr.Button("🚀 Start Recording Session", variant="primary")
                
                with gr.Column(scale=2):
                    gr.Markdown("## 📻 Available Stations")
                    
                    station_display = gr.Textbox(
                        label="Stations",
                        value=initial_stations,
                        lines=15,
                        max_lines=20,
                        interactive=False
                    )
                    
                    # Update station display when location changes
                    def update_stations(country, city):
                        try:
                            return self.get_stations_display(country, city)
                        except Exception as e:
                            print(f"Error updating stations: {e}")
                            return f"Error loading stations: {str(e)}"
                    
                    country_dropdown.change(
                        fn=update_stations,
                        inputs=[country_dropdown, city_dropdown],
                        outputs=[station_display]
                    )
                    
                    city_dropdown.change(
                        fn=update_stations,
                        inputs=[country_dropdown, city_dropdown],
                        outputs=[station_display]
                    )
                    
                    gr.Markdown("## 📊 Recording Results")
                    
                    results_display = gr.Textbox(
                        label="Recording Results",
                        lines=10,
                        max_lines=15,
                        interactive=False
                    )
                    
                    # Start recording session
                    start_button.click(
                        fn=self.record_stations,
                        inputs=[country_dropdown, city_dropdown, duration_slider, count_slider],
                        outputs=[results_display]
                    )
            
            gr.Markdown("---")
            gr.Markdown("### 📁 Recordings are saved in the `recordings/` folder")
            gr.Markdown("### 📊 Check the console for detailed progress")
            
        return app

def main():
    """Main function to run the Gradio app"""
    try:
        app = GradioRadioApp()
        interface = app.create_interface()
        
        print("🚀 Starting Gradio app...")
        print("📱 Open your browser and go to the URL shown below")
        print("🎵 Browse and record Arabic radio stations!")
        
        interface.launch(
            server_port=7860,
            share=True,
            show_error=True
        )
        
    except Exception as e:
        logger.error(f"Error starting Gradio app: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main() 