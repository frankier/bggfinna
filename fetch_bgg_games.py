#!/usr/bin/env python3

import csv
import sys
from tqdm import tqdm
from bggfinna import (get_processed_ids, truncate_incomplete_output, should_write_header,
                     get_bgg_game_details, get_unique_bgg_ids, get_data_path, is_test_mode)


def main():
    # Parse arguments with test mode support
    relations_file = sys.argv[1] if len(sys.argv) > 1 else get_data_path('finna_bgg_relations.csv')
    output_file = sys.argv[2] if len(sys.argv) > 2 else get_data_path('bgg_games.csv')
    
    if is_test_mode():
        print("Running in TEST mode - outputs will go to data/test/")
    
    # Truncate any incomplete output
    truncate_incomplete_output(output_file)
    
    # Get unique BGG IDs from relations
    print(f"Reading BGG IDs from {relations_file}...")
    all_bgg_ids = get_unique_bgg_ids(relations_file)
    
    # Get already processed BGG IDs
    processed_bgg_ids = get_processed_ids(output_file, 'bgg_id')
    
    # Get unprocessed BGG IDs using set difference
    unprocessed_bgg_ids = [bgg_id for bgg_id in all_bgg_ids if bgg_id not in processed_bgg_ids]
    
    total_ids = len(all_bgg_ids)
    processed_count = len(processed_bgg_ids)
    
    print(f"Found {total_ids} unique BGG IDs, {processed_count} already processed, {len(unprocessed_bgg_ids)} remaining")
    
    if not unprocessed_bgg_ids:
        print("All BGG games already processed!")
        return
    
    # Determine file mode and whether to write header
    write_header = should_write_header(output_file)
    mode = 'w' if write_header else 'a'
    
    with open(output_file, mode, newline='', encoding='utf-8') as csvfile:
        fieldnames = ['bgg_id', 'primary_name', 'all_names', 'year', 'description', 
                     'min_players', 'max_players', 'playing_time', 'min_play_time', 
                     'max_play_time', 'min_age', 'categories', 'mechanics', 'families', 
                     'designers', 'artists', 'publishers', 'bgg_rank', 'average_rating', 
                     'bayes_average', 'users_rated', 'weight', 'owned']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if write_header:
            writer.writeheader()
        
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
                        'owned': bgg_details['owned']
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
                        'owned': ''
                    }
                    pbar.set_postfix_str(f"✗ Failed: {bgg_id}")
                
                writer.writerow(result)
                csvfile.flush()  # Flush after each write for safety
                pbar.update(1)
    
    print(f"\nCompleted! BGG game details saved to {output_file}")


if __name__ == "__main__":
    main()