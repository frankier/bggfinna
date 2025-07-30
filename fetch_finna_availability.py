#!/usr/bin/env python3
"""
Fetch availability/location information for board games from Finna API.

This script reads the existing Finna board games data and fetches detailed 
location information for each game, showing which library branches have copies.
"""

import requests
import csv
import sys
import json
import time
from urllib.parse import urlencode
from tqdm import tqdm
from bggfinna import get_data_path, get_unprocessed_items, should_write_header, is_test_mode, get_test_limit, is_smoke_test_mode


def fetch_game_availability(game_id, max_retries=3):
    """
    Fetch detailed availability/location information for a single game.
    
    Args:
        game_id: Finna record ID (e.g., 'keski.3376040')
        max_retries: Number of retry attempts for failed requests
    
    Returns:
        dict: Availability information or None if failed
    """
    url = f"https://api.finna.fi/v1/record?id={game_id}"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') != 'OK' or not data.get('records'):
                tqdm.write(f"No record found for {game_id}")
                return None
            
            record = data['records'][0]
            
            # Extract location/availability information
            availability_info = {
                'finna_id': game_id,
                'title': record.get('title', ''),
                'buildings': [],
                'locations': [],
                'organizations': []
            }
            
            # Process buildings information (library locations)
            buildings = record.get('buildings', [])
            for building in buildings:
                building_info = {
                    'value': building.get('value', ''),
                    'name': building.get('translated', building.get('value', ''))
                }
                availability_info['buildings'].append(building_info)
                
                # Extract organization and location details
                value_parts = building.get('value', '').split('/')
                if len(value_parts) >= 2:
                    org = value_parts[1] if value_parts[1] else 'Unknown'
                    if org not in availability_info['organizations']:
                        availability_info['organizations'].append(org)
                
                # Add location name to locations list
                location_name = building.get('translated', building.get('value', ''))
                if location_name and location_name not in availability_info['locations']:
                    availability_info['locations'].append(location_name)
            
            # Convert lists to strings for CSV storage
            availability_info['buildings_json'] = json.dumps(availability_info['buildings'])
            availability_info['locations_str'] = '; '.join(availability_info['locations'])
            availability_info['organizations_str'] = '; '.join(availability_info['organizations'])
            availability_info['num_locations'] = len(availability_info['buildings'])
            
            time.sleep(0.5)  # Be respectful to the API
            return availability_info
            
        except requests.exceptions.RequestException as e:
            tqdm.write(f"Request failed for {game_id} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
        except json.JSONDecodeError as e:
            tqdm.write(f"JSON decode error for {game_id}: {e}")
            break
        except Exception as e:
            tqdm.write(f"Unexpected error for {game_id}: {e}")
            break
    
    return None


def main():
    """Main function to fetch availability for all games"""
    # Set file paths with test mode support
    input_file = sys.argv[1] if len(sys.argv) > 1 else get_data_path('finna_board_games.csv')
    output_file = sys.argv[2] if len(sys.argv) > 2 else get_data_path('finna_availability.csv')
    
    if is_smoke_test_mode():
        print("Running in SMOKE TEST mode - outputs will go to data/smoke/")
    elif is_test_mode():
        print("Running in TEST mode - outputs will go to data/test/")
    
    # Read all Finna games
    print(f"Reading games from {input_file}...")
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            all_games = list(reader)
    except FileNotFoundError:
        print(f"Error: Input file {input_file} not found.")
        print("Please run fetch_finna_games.py first to create the games data.")
        return 1
    
    if not all_games:
        print("No games found in input file.")
        return 1
    
    # Get unprocessed games (skip already processed ones for incremental updates)
    unprocessed_games = get_unprocessed_items(
        all_games, output_file, 'id', 'finna_id'
    )
    
    total_games = len(all_games)
    processed_count = total_games - len(unprocessed_games)
    
    if processed_count > 0:
        print(f"Found {processed_count} already processed games, {len(unprocessed_games)} remaining")
    
    if not unprocessed_games:
        print("All games already processed!")
        return 0
    
    # Apply test mode limits
    test_limit = get_test_limit()
    if test_limit is not None and len(unprocessed_games) > test_limit:
        print(f"Limiting to {test_limit} games for test mode")
        unprocessed_games = unprocessed_games[:test_limit]
    
    print(f"Fetching availability information for {len(unprocessed_games)} games...")
    
    # CSV fieldnames for output
    fieldnames = [
        'finna_id', 'title', 'num_locations', 'locations_str', 
        'organizations_str', 'buildings_json'
    ]
    
    # Determine file mode and whether to write header
    write_header = should_write_header(output_file)
    mode = 'w' if write_header else 'a'
    
    success_count = 0
    
    with open(output_file, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if write_header:
            writer.writeheader()
        
        with tqdm(total=len(unprocessed_games), desc="Fetching availability", unit="games") as pbar:
            for game in unprocessed_games:
                game_id = game['id']
                pbar.set_description(f"Processing: {game.get('title', game_id)[:30]}...")
                
                availability_info = fetch_game_availability(game_id)
                
                if availability_info:
                    # Write the availability information
                    writer.writerow({
                        'finna_id': availability_info['finna_id'],
                        'title': availability_info['title'],
                        'num_locations': availability_info['num_locations'],
                        'locations_str': availability_info['locations_str'],
                        'organizations_str': availability_info['organizations_str'],
                        'buildings_json': availability_info['buildings_json']
                    })
                    csvfile.flush()  # Flush after each write for safety
                    success_count += 1
                else:
                    # Write empty record for failed fetches to avoid reprocessing
                    writer.writerow({
                        'finna_id': game_id,
                        'title': game.get('title', ''),
                        'num_locations': 0,
                        'locations_str': '',
                        'organizations_str': '',
                        'buildings_json': '[]'
                    })
                    csvfile.flush()
                
                pbar.update(1)
    
    print(f"\nCompleted! Successfully processed {success_count}/{len(unprocessed_games)} games")
    print(f"Results saved to {output_file}")
    
    if success_count == 0:
        print("Warning: No games were successfully processed")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())