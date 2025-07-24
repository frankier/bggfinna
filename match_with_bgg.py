#!/usr/bin/env python3

import requests
import csv
import xml.etree.ElementTree as ET
import time
import sys
import os
import re
import string
from urllib.parse import quote
from tqdm import tqdm
from bggfinna import get_unprocessed_items, should_write_header, get_bgg_game_details, get_data_path, is_test_mode

def extract_authors_from_finna(authors_json):
    """Extract author names from Finna authors JSON"""
    if not authors_json:
        return []
    
    try:
        if isinstance(authors_json, str):
            authors_data = json.loads(authors_json.replace("'", '"'))
        else:
            authors_data = authors_json
            
        author_names = []
        
        # Extract from primary authors
        if 'primary' in authors_data:
            for author_name in authors_data['primary'].keys():
                # Clean author name (remove role info after comma)
                clean_name = author_name.split(',')[0].strip()
                if clean_name:
                    author_names.append(clean_name)
                    
        return author_names
    except:
        return []

def parse_bgg_search_response(xml_content):
    """Parse BGG search API response and return list of games"""
    try:
        root = ET.fromstring(xml_content)
        games = []
        
        for item in root.findall('.//item'):
            game = {
                'bgg_id': item.get('id'),
                'type': item.get('type'),
                'names': [],
                'year': None
            }
            
            # Get all names (primary and alternate)
            for name in item.findall('name'):
                game['names'].append(name.get('value'))
            
            # Get year
            year_elem = item.find('yearpublished')
            if year_elem is not None:
                game['year'] = int(year_elem.get('value'))
            
            if game['type'] == 'boardgame':
                games.append(game)
        
        return games
    except ET.ParseError as e:
        tqdm.write(f"Error parsing BGG XML: {e}")
        return []

def parse_bgg_thing_response(xml_content):
    """Parse BGG thing API response and return detailed game info"""
    try:
        root = ET.fromstring(xml_content)
        item = root.find('.//item[@type="boardgame"]')
        
        if item is None:
            return None
        
        game = {
            'bgg_id': item.get('id'),
            'primary_name': '',
            'all_names': [],
            'year': None,
            'description': '',
            'min_players': None,
            'max_players': None,
            'playing_time': None,
            'min_play_time': None,
            'max_play_time': None,
            'min_age': None,
            'categories': [],
            'mechanics': [],
            'families': [],
            'designers': [],
            'artists': [],
            'publishers': [],
            'bgg_rank': None,
            'average_rating': None,
            'bayes_average': None,
            'users_rated': None,
            'weight': None,
            'owned': None
        }
        
        # Names
        for name in item.findall('name'):
            name_val = name.get('value')
            game['all_names'].append(name_val)
            if name.get('type') == 'primary':
                game['primary_name'] = name_val
        
        # Basic info
        year_elem = item.find('yearpublished')
        if year_elem is not None:
            game['year'] = year_elem.get('value')
            
        # Player counts and times
        for field, attr in [('min_players', 'minplayers'), ('max_players', 'maxplayers'),
                           ('playing_time', 'playingtime'), ('min_play_time', 'minplaytime'),
                           ('max_play_time', 'maxplaytime'), ('min_age', 'minage')]:
            elem = item.find(attr)
            if elem is not None:
                game[field] = elem.get('value')
        
        # Description
        desc_elem = item.find('description')
        if desc_elem is not None:
            game['description'] = desc_elem.text or ''
        
        # Categories, mechanics, families
        for link in item.findall('link'):
            link_type = link.get('type')
            link_value = link.get('value')
            if link_type == 'boardgamecategory':
                game['categories'].append(link_value)
            elif link_type == 'boardgamemechanic':
                game['mechanics'].append(link_value)
            elif link_type == 'boardgamefamily':
                game['families'].append(link_value)
            elif link_type == 'boardgamedesigner':
                game['designers'].append(link_value)
            elif link_type == 'boardgameartist':
                game['artists'].append(link_value)
            elif link_type == 'boardgamepublisher':
                game['publishers'].append(link_value)
        
        # Statistics
        stats = item.find('statistics/ratings')
        if stats is not None:
            avg_elem = stats.find('average')
            if avg_elem is not None:
                game['average_rating'] = avg_elem.get('value')
                
            bayes_elem = stats.find('bayesaverage')
            if bayes_elem is not None:
                game['bayes_average'] = bayes_elem.get('value')
                
            users_elem = stats.find('usersrated')
            if users_elem is not None:
                game['users_rated'] = users_elem.get('value')
                
            weight_elem = stats.find('averageweight')
            if weight_elem is not None:
                game['weight'] = weight_elem.get('value')
                
            owned_elem = stats.find('owned')
            if owned_elem is not None:
                game['owned'] = owned_elem.get('value')
            
            # BGG rank
            for rank in stats.findall('ranks/rank'):
                if rank.get('name') == 'boardgame':
                    rank_val = rank.get('value')
                    if rank_val != 'Not Ranked':
                        game['bgg_rank'] = rank_val
        
        return game
        
    except ET.ParseError as e:
        tqdm.write(f"Error parsing BGG thing XML: {e}")
        return None

def get_bgg_game_details(bgg_id, max_retries=3):
    """Get detailed game info from BGG thing API"""
    url = f"https://boardgamegeek.com/xmlapi2/thing?id={bgg_id}&stats=1"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            if response.status_code == 202:
                # BGG is processing, wait and retry
                time.sleep(2)
                continue
            else:
                time.sleep(1)
                
            return parse_bgg_thing_response(response.content)
            
        except requests.exceptions.RequestException as e:
            tqdm.write(f"Request failed for BGG ID {bgg_id} (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
    
    return None

def search_bgg_by_title(title, max_retries=3):
    """Search BGG API for a game title"""
    url = f"https://boardgamegeek.com/xmlapi2/search?query={quote(title)}&type=boardgame"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            if response.status_code == 202:
                # BGG is processing, wait and retry
                time.sleep(2)
                continue
            else:
                time.sleep(1)
                
            return parse_bgg_search_response(response.content)
            
        except requests.exceptions.RequestException as e:
            tqdm.write(f"Request failed for '{title}' (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
    
    return []

def normalize_title_for_matching(title):
    """Remove punctuation and normalize title for matching"""
    if not title:
        return ""
    # Convert to lowercase and remove all punctuation
    title = title.lower()
    title = title.translate(str.maketrans('', '', string.punctuation))
    # Replace multiple spaces with single space and strip
    title = re.sub(r'\s+', ' ', title).strip()
    return title

def check_matches(bgg_games, finna_titles, match_type='exact'):
    """Check for matches between BGG games and Finna titles"""
    matches = []
    
    for game in bgg_games:
        for bgg_name in game['names']:
            for finna_title in finna_titles:
                if match_type == 'exact':
                    bgg_normalized = normalize_title_for_matching(bgg_name)
                    finna_normalized = normalize_title_for_matching(finna_title)
                    if bgg_normalized == finna_normalized:
                        matches.append({**game, 'match_type': 'exact'})
                        tqdm.write(f"    Found exact match: {bgg_name} (ID: {game['bgg_id']}, Year: {game['year']})")
                        break
                elif match_type == 'substring':
                    bgg_normalized = normalize_title_for_matching(bgg_name)
                    finna_normalized = normalize_title_for_matching(finna_title)
                    if len(finna_title.split()) > 1 and finna_normalized in bgg_normalized:
                        matches.append({**game, 'match_type': 'substring'})
                        tqdm.write(f"    Found substring match: '{finna_title}' in '{bgg_name}' (ID: {game['bgg_id']}, Year: {game['year']})")
                        break
    
    return matches

def find_best_bgg_match(finna_game):
    """Find the best BGG match for a Finna game with multiple fallback strategies"""
    finna_titles = []
    
    # Add main title
    if finna_game['title']:
        finna_titles.append(finna_game['title'])
    
    # Add alternative titles
    if finna_game['alternativeTitles']:
        alt_titles = finna_game['alternativeTitles'].split(';')
        finna_titles.extend([title.strip() for title in alt_titles if title.strip()])
    
    # Get year and authors for advanced matching
    finna_year = None
    if finna_game['year']:
        try:
            finna_year = int(finna_game['year'])
        except (ValueError, TypeError):
            pass
    
    finna_authors = extract_authors_from_finna(finna_game.get('authors', ''))
    
    tqdm.write(f"Searching for: {finna_titles} (year: {finna_year}, authors: {finna_authors})")
    
    all_matches = []
    
    # STRATEGY 1: Try exact matches
    for title in finna_titles:
        if not title:
            continue
            
        tqdm.write(f"  Strategy 1 - Exact search for: '{title}'")
        bgg_games = search_bgg_by_title(title)
        
        exact_matches = check_matches(bgg_games, finna_titles, 'exact')
        all_matches.extend(exact_matches)
    
    # STRATEGY 2: If no exact matches, try substring matching for multi-word titles
    if not all_matches:
        tqdm.write("  Strategy 2 - Substring matching...")
        
        for title in finna_titles:
            if not title or len(title.split()) <= 1:
                continue  # Skip single words to avoid false positives
                
            tqdm.write(f"  Substring search for: '{title}'")
            bgg_games = search_bgg_by_title(title)
            
            substring_matches = check_matches(bgg_games, finna_titles, 'substring')
            all_matches.extend(substring_matches)
    
    # STRATEGY 3: Author + fuzzy title matching
    if not all_matches and finna_authors:
        tqdm.write("  Strategy 3 - Author + fuzzy title matching...")
        
        for author in finna_authors[:2]:  # Try first 2 authors to avoid too many API calls
            tqdm.write(f"  Searching by author: '{author}'")
            author_game_ids = search_bgg_by_author(author)
            
            if author_game_ids:
                # Get details for author's games
                author_games = []
                for game_id in author_game_ids:
                    game_details = get_bgg_game_details(game_id)
                    if game_details:
                        # Check if author is actually in the designers
                        if any(author.lower() in designer.lower() for designer in game_details.get('designers', [])):
                            bgg_game = {
                                'bgg_id': game_id,
                                'names': game_details.get('all_names', []),
                                'year': game_details.get('year'),
                                'designers': game_details.get('designers', [])
                            }
                            author_games.append(bgg_game)
                
                # Try fuzzy matching with author's games
                fuzzy_matches = check_fuzzy_matches(author_games, finna_titles, threshold=75)
                if fuzzy_matches:
                    for match in fuzzy_matches:
                        match['match_type'] = 'author_fuzzy_title'
                    all_matches.extend(fuzzy_matches)
                    break
    
    # STRATEGY 4: Author + exact year match (last resort)
    if not all_matches and finna_authors and finna_year:
        tqdm.write("  Strategy 4 - Author + exact year matching...")
        
        for author in finna_authors[:2]:
            tqdm.write(f"  Searching by author + year: '{author}' ({finna_year})")
            author_game_ids = search_bgg_by_author(author)
            
            if author_game_ids:
                for game_id in author_game_ids:
                    game_details = get_bgg_game_details(game_id)
                    if game_details:
                        game_year = game_details.get('year')
                        if game_year and int(game_year) == finna_year:
                            # Check if author is in designers
                            if any(author.lower() in designer.lower() for designer in game_details.get('designers', [])):
                                year_match = {
                                    'bgg_id': game_id,
                                    'names': game_details.get('all_names', []),
                                    'year': game_year,
                                    'match_type': 'author_year'
                                }
                                all_matches.append(year_match)
                                tqdm.write(f"    Found author+year match: {game_details.get('primary_name')} (ID: {game_id}, Year: {game_year})")
                                break
                
                if all_matches:
                    break
    
    if not all_matches:
        tqdm.write("  No matches found")
        return None
    
    # Remove duplicates based on BGG ID
    unique_matches = {}
    for match in all_matches:
        unique_matches[match['bgg_id']] = match
    all_matches = list(unique_matches.values())
    
    if len(all_matches) == 1:
        match_type = all_matches[0].get('match_type', 'exact')
        tqdm.write(f"  Single {match_type} match: {all_matches[0]['names'][0]}")
        return all_matches[0]
    
    # Multiple matches - prioritize by match type and year
    match_priority = {'exact': 0, 'substring': 1, 'author_fuzzy_title': 2, 'author_year': 3}
    
    # Sort by match type priority first
    all_matches.sort(key=lambda x: match_priority.get(x.get('match_type', 'exact'), 99))
    
    # If same match type, use year to disambiguate
    if finna_year and len(all_matches) > 1:
        tqdm.write(f"  Multiple matches found, using year {finna_year} for disambiguation")
        year_matches = [m for m in all_matches if m.get('year') and abs(int(m['year']) - finna_year) <= 1]
        if year_matches:
            best_match = year_matches[0]
            match_type = best_match.get('match_type', 'exact')
            tqdm.write(f"  Best {match_type} + year match: {best_match['names'][0]} (BGG year: {best_match['year']})")
            return best_match
    
    # Return the highest priority match
    best_match = all_matches[0]
    match_type = best_match.get('match_type', 'exact')
    tqdm.write(f"  Best {match_type} match: {best_match['names'][0]}")
    return best_match


def main():
    # Parse arguments with test mode support
    input_file = sys.argv[1] if len(sys.argv) > 1 else get_data_path('finna_board_games.csv')
    output_file = sys.argv[2] if len(sys.argv) > 2 else get_data_path('finna_bgg_relations.csv')
    
    if is_test_mode():
        print("Running in TEST mode - outputs will go to data/test/")
    
    # Read all Finna games
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        all_finna_games = list(reader)
    
    # Get unprocessed games using set difference
    unprocessed_games = get_unprocessed_items(
        all_finna_games, output_file, 'id', 'finna_id'
    )
    
    total_games = len(all_finna_games)
    processed_count = total_games - len(unprocessed_games)
    
    if processed_count > 0:
        print(f"Found {processed_count} already processed games, {len(unprocessed_games)} remaining")
    
    if not unprocessed_games:
        print("All games already processed!")
        return
    
    # Determine file mode and whether to write header
    write_header = should_write_header(output_file)
    mode = 'w' if write_header else 'a'
    
    with open(output_file, mode, newline='', encoding='utf-8') as csvfile:
        fieldnames = ['finna_id', 'bgg_id', 'match_type']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if write_header:
            writer.writeheader()
        
        with tqdm(total=len(unprocessed_games), desc="Matching games", unit="games") as pbar:
            for finna_game in unprocessed_games:
                pbar.set_description(f"Processing: {finna_game['title'][:30]}...")
                
                bgg_match = find_best_bgg_match(finna_game)
                
                # Create minimal relation record
                result = {
                    'finna_id': finna_game['id'],
                    'bgg_id': bgg_match['bgg_id'] if bgg_match else '',
                    'match_type': bgg_match.get('match_type', 'none') if bgg_match else 'none'
                }
                
                writer.writerow(result)
                csvfile.flush()  # Flush after each write for safety
                pbar.update(1)
    
    print(f"\nCompleted! Results saved to {output_file}")

def save_results(games, filename):
    """Save matched games to CSV"""
    if not games:
        return
    
    fieldnames = list(games[0].keys())
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(games)
    
    print(f"Saved {len(games)} games to {filename}")

if __name__ == "__main__":
    main()
