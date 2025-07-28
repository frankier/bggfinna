#!/usr/bin/env python3

import csv
import sys
import argparse
import os
from tqdm import tqdm
from bggfinna import (get_processed_ids, truncate_incomplete_output, should_write_header,
                     get_bgg_game_details, get_unique_bgg_ids, get_data_path, is_test_mode,
                     get_stale_bgg_ids, get_current_timestamp)


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Fetch BGG game details with incremental updates')
    parser.add_argument('relations_file', nargs='?', default=None,
                       help='Input relations CSV file')
    parser.add_argument('output_file', nargs='?', default=None,
                       help='Output BGG games CSV file')
    parser.add_argument('--reset', action='store_true',
                       help='Force full refresh, ignoring existing timestamps')
    parser.add_argument('--max-age', type=int, default=30,
                       help='Maximum age in days before a record is considered stale (default: 30)')
    
    args = parser.parse_args()
    
    # Set file paths with test mode support
    relations_file = args.relations_file or get_data_path('finna_bgg_relations.csv')
    output_file = args.output_file or get_data_path('bgg_games.csv')
    
    if is_test_mode():
        print("Running in TEST mode - outputs will go to data/test/")
    
    # Truncate any incomplete output
    truncate_incomplete_output(output_file)
    
    # Get unique BGG IDs from relations
    print(f"Reading BGG IDs from {relations_file}...")
    all_bgg_ids = get_unique_bgg_ids(relations_file)
    
    # Get already processed BGG IDs
    processed_bgg_ids = get_processed_ids(output_file, 'bgg_id')
    
    # Determine which BGG IDs need updating
    if args.reset:
        print("Full reset requested - will fetch all BGG IDs")
        unprocessed_bgg_ids = all_bgg_ids
        processed_bgg_ids = set()  # Reset processed set for accurate counting
    else:
        # Get stale BGG IDs (old or missing timestamps)
        stale_bgg_ids = get_stale_bgg_ids(output_file, args.max_age)
        
        # Get unprocessed BGG IDs (not in CSV at all)
        new_bgg_ids = [bgg_id for bgg_id in all_bgg_ids if bgg_id not in processed_bgg_ids]
        
        # Combine new and stale IDs
        unprocessed_bgg_ids = list(set(new_bgg_ids) | stale_bgg_ids)
        
        if stale_bgg_ids:
            print(f"Found {len(stale_bgg_ids)} stale records (older than {args.max_age} days)")
    
    total_ids = len(all_bgg_ids)
    processed_count = len(processed_bgg_ids)
    
    print(f"Found {total_ids} unique BGG IDs, {processed_count} already processed, {len(unprocessed_bgg_ids)} remaining")
    
    if not unprocessed_bgg_ids:
        print("All BGG games already processed!")
        return
    
    # For incremental updates, we need to rebuild the entire file
    # Load existing records that don't need updating
    existing_records = {}
    if not args.reset and os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    bgg_id = row.get('bgg_id', '').strip()
                    if bgg_id and bgg_id not in unprocessed_bgg_ids:
                        # This record doesn't need updating, keep it
                        existing_records[bgg_id] = row
        except Exception:
            # If file is corrupt, just start fresh
            existing_records = {}
    
    # Write the complete file (existing + updated records)
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['bgg_id', 'primary_name', 'all_names', 'year', 'description', 
                     'min_players', 'max_players', 'playing_time', 'min_play_time', 
                     'max_play_time', 'min_age', 'categories', 'mechanics', 'families', 
                     'designers', 'artists', 'publishers', 'bgg_rank', 'average_rating', 
                     'bayes_average', 'users_rated', 'weight', 'owned', 'last_updated']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # First, write existing records that don't need updating
        for record in existing_records.values():
            # Ensure the record has all required fields (for backward compatibility)
            complete_record = {field: record.get(field, '') for field in fieldnames}
            writer.writerow(complete_record)
        
        # Then fetch and write updated records
        with tqdm(total=len(unprocessed_bgg_ids), desc="Fetching BGG games", unit="games") as pbar:
            for bgg_id in unprocessed_bgg_ids:
                pbar.set_description(f"Fetching BGG ID: {bgg_id}")
                
                bgg_details = get_bgg_game_details(bgg_id)
                
                if bgg_details:
                    # Convert lists to semicolon-separated strings for CSV
                    result = {
                        'bgg_id': bgg_details['bgg_id'],
                        'primary_name': bgg_details['primary_name'],
                        'all_names': '; '.join(bgg_details['all_names']),
                        'year': bgg_details['year'],
                        'description': bgg_details['description'][:500] + '...' if len(bgg_details['description']) > 500 else bgg_details['description'],
                        'min_players': bgg_details['min_players'],
                        'max_players': bgg_details['max_players'],
                        'playing_time': bgg_details['playing_time'],
                        'min_play_time': bgg_details['min_play_time'],
                        'max_play_time': bgg_details['max_play_time'],
                        'min_age': bgg_details['min_age'],
                        'categories': '; '.join(bgg_details['categories']),
                        'mechanics': '; '.join(bgg_details['mechanics']),
                        'families': '; '.join(bgg_details['families']),
                        'designers': '; '.join(bgg_details['designers']),
                        'artists': '; '.join(bgg_details['artists']),
                        'publishers': '; '.join(bgg_details['publishers']),
                        'bgg_rank': bgg_details['bgg_rank'],
                        'average_rating': bgg_details['average_rating'],
                        'bayes_average': bgg_details['bayes_average'],
                        'users_rated': bgg_details['users_rated'],
                        'weight': bgg_details['weight'],
                        'owned': bgg_details['owned'],
                        'last_updated': get_current_timestamp()
                    }
                    
                    pbar.set_postfix_str(f"✓ {result['primary_name'][:20]}...")
                else:
                    # Failed to fetch details, create minimal record
                    result = {
                        'bgg_id': bgg_id,
                        'primary_name': '',
                        'all_names': '',
                        'year': '',
                        'description': '',
                        'min_players': '',
                        'max_players': '',
                        'playing_time': '',
                        'min_play_time': '',
                        'max_play_time': '',
                        'min_age': '',
                        'categories': '',
                        'mechanics': '',
                        'families': '',
                        'designers': '',
                        'artists': '',
                        'publishers': '',
                        'bgg_rank': '',
                        'average_rating': '',
                        'bayes_average': '',
                        'users_rated': '',
                        'weight': '',
                        'owned': '',
                        'last_updated': get_current_timestamp()
                    }
                    pbar.set_postfix_str(f"✗ Failed: {bgg_id}")
                
                writer.writerow(result)
                csvfile.flush()  # Flush after each write for safety
                pbar.update(1)
    
    print(f"\nCompleted! BGG game details saved to {output_file}")


if __name__ == "__main__":
    main()