#!/usr/bin/env python3

import requests
import csv
import sys
from urllib.parse import urlencode
from tqdm import tqdm
from bggfinna import get_data_path, is_test_mode

def fetch_and_save_board_games(filename=None):
    """Fetch all board games from keski.finna.fi and save directly to CSV"""
    
    if filename is None:
        filename = get_data_path('finna_board_games.csv')
    
    base_url = "https://api.finna.fi/v1/search"
    
    params = {
        'lookfor': '',
        'filter[]': [
            'building:"0/Keski/"',
            '~format:"1/Game/BoardGame/"'
        ],
        'field[]': [
            'id',
            'title', 
            'alternativeTitles',
            'year',
            'publicationDates',
            'humanReadablePublicationDates',
            'languages',
            'originalLanguages',
            'authors',
            'publishers',
            'summary',
            'genres',
            'subjects',
            'playingTimes',
            'targetAudienceNotes',
            'physicalDescriptions'
        ],
        'limit': 100,
        'resumptionToken': '*'
    }
    
    # First request to get total count
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if data.get('status') != 'OK':
            print(f"API error: {data.get('statusMessage', 'Unknown error')}")
            return 0
        
        total_count = data.get('resultCount', 0)
        if is_test_mode():
            total_count = min(total_count, 10)
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return 0
    
    fieldnames = [
        'id', 'title', 'alternativeTitles', 'year', 'publicationDates', 
        'humanReadablePublicationDates', 'languages', 'originalLanguages',
        'authors', 'publishers', 'summary', 'genres', 'subjects', 
        'playingTimes', 'targetAudienceNotes', 'physicalDescriptions'
    ]
    
    records_written = 0
    pbar = tqdm(total=total_count, desc="Fetching and saving records", unit=" records")
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Reset params for pagination
        params = {
            'lookfor': '',
            'filter[]': [
                'building:"0/Keski/"',
                '~format:"1/Game/BoardGame/"'
            ],
            'field[]': fieldnames,
            'limit': 100,
            'resumptionToken': '*'
        }
        
        while records_written < total_count:
            try:
                response = requests.get(base_url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if data.get('status') != 'OK':
                    print(f"API error: {data.get('statusMessage', 'Unknown error')}")
                    break
                
                records = data.get('records', [])
                
                for record in records:
                    if records_written >= total_count:
                        break
                        
                    row = {}
                    for field in fieldnames:
                        value = record.get(field, '')
                        
                        # Handle list fields by joining with semicolons
                        if isinstance(value, list):
                            if field == 'authors':
                                # Extract author names from complex author structure
                                names = []
                                for author_group in value:
                                    if isinstance(author_group, dict):
                                        for category in ['primary', 'secondary', 'corporate']:
                                            if category in author_group:
                                                names.extend(author_group[category].keys())
                                value = '; '.join(names)
                            else:
                                value = '; '.join(str(item) for item in value)
                        
                        row[field] = value
                    
                    writer.writerow(row)
                    records_written += 1
                    pbar.update(1)
                
                # Check for resumption token for next batch
                resumption_token = data.get('resumptionToken', {}).get('token')
                if not resumption_token or records_written >= total_count:
                    break
                    
                # Update params for next request
                params = {'resumptionToken': resumption_token}
                
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                break
            except Exception as e:
                print(f"Error processing response: {e}")
                break
    
    pbar.close()
    print(f"\nSaved {records_written} records to {filename}")
    return records_written


def main():
    if is_test_mode():
        print("Running in TEST mode - limiting to 10 records, outputs will go to data/test/")
    
    total_records = fetch_and_save_board_games()
    if total_records > 0:
        print(f"\nTotal board games found: {total_records}")
    else:
        print("No board games found")

if __name__ == "__main__":
    main()
