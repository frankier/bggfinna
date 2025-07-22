#!/usr/bin/env python3

import requests
import csv
import sys
from urllib.parse import urlencode

def fetch_board_games():
    """Fetch all board games from keski.finna.fi"""
    
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
    
    all_records = []
    
    print("Fetching board games from Keski library...")
    
    while True:
        try:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') != 'OK':
                print(f"API error: {data.get('statusMessage', 'Unknown error')}")
                break
            
            records = data.get('records', [])
            all_records.extend(records)
            
            print(f"Fetched {len(records)} records, total: {len(all_records)}")
            
            # Check for resumption token for next batch
            resumption_token = data.get('resumptionToken', {}).get('token')
            if not resumption_token:
                break
                
            # Update params for next request
            params = {'resumptionToken': resumption_token}
            
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            break
        except Exception as e:
            print(f"Error processing response: {e}")
            break
    
    return all_records

def save_to_csv(records, filename='data/finna_board_games.csv'):
    """Save records to CSV file"""
    
    if not records:
        print("No records to save")
        return
    
    fieldnames = [
        'id', 'title', 'alternativeTitles', 'year', 'publicationDates', 
        'humanReadablePublicationDates', 'languages', 'originalLanguages',
        'authors', 'publishers', 'summary', 'genres', 'subjects', 
        'playingTimes', 'targetAudienceNotes', 'physicalDescriptions'
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for record in records:
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
    
    print(f"Saved {len(records)} records to {filename}")

def main():
    records = fetch_board_games()
    if records:
        save_to_csv(records)
        print(f"\nTotal board games found: {len(records)}")
    else:
        print("No board games found")

if __name__ == "__main__":
    main()