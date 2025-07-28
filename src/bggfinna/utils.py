#!/usr/bin/env python3
"""
Common utilities for the bggfinna project.
"""

import csv
import os
import requests
import xml.etree.ElementTree as ET
import time
from tqdm import tqdm


# Test mode configuration
def is_test_mode():
    """Check if running in test mode"""
    return bool(os.environ.get('BGGFINNA_TEST'))


def get_data_path(filename):
    """Get the appropriate data path (test or production)"""
    if is_test_mode():
        test_dir = 'data/test'
        os.makedirs(test_dir, exist_ok=True)
        return os.path.join(test_dir, filename)
    else:
        os.makedirs('data', exist_ok=True)
        return os.path.join('data', filename)


def truncate_incomplete_output(output_file):
    """
    Truncate any incomplete output (non-newline terminated) from the output file.
    Returns True if the file was modified, False otherwise.
    """
    if not os.path.exists(output_file):
        return False
    
    with open(output_file, 'rb') as f:
        content = f.read()
    
    if not content:
        return False
    
    # Check if file ends with newline
    if not content.endswith(b'\n'):
        # Truncate to last complete line
        last_newline = content.rfind(b'\n')
        if last_newline != -1:
            with open(output_file, 'wb') as f:
                f.write(content[:last_newline + 1])
            return True
        else:
            # No complete lines, remove file
            os.remove(output_file)
            return True
    
    return False


def get_processed_ids(output_file, id_column):
    """
    Get set of already processed IDs from output CSV file.
    
    Args:
        output_file: Path to the CSV output file
        id_column: Name of the column containing the IDs
    
    Returns:
        set: Set of processed IDs
    """
    processed_ids = set()
    
    if not os.path.exists(output_file):
        return processed_ids
    
    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                id_value = row.get(id_column, '').strip()
                if id_value:
                    processed_ids.add(id_value)
    except Exception:
        # If file is corrupt or empty, return empty set
        return set()
    
    return processed_ids


def get_unprocessed_items(input_items, output_file, input_id_field, output_id_field):
    """
    Get list of unprocessed items by comparing input with already processed output.
    
    Args:
        input_items: List of input items (dicts)
        output_file: Path to output CSV file
        input_id_field: Field name for ID in input items
        output_id_field: Field name for ID in output file
    
    Returns:
        list: List of unprocessed input items
    """
    # First, truncate any incomplete output
    truncate_incomplete_output(output_file)
    
    # Get already processed IDs
    processed_ids = get_processed_ids(output_file, output_id_field)
    
    # Filter unprocessed items
    unprocessed = []
    for item in input_items:
        item_id = item.get(input_id_field, '').strip()
        if item_id and item_id not in processed_ids:
            unprocessed.append(item)
    
    return unprocessed


def should_write_header(output_file):
    """
    Determine if CSV header should be written.
    Returns True if file doesn't exist or is empty after truncation.
    """
    if not os.path.exists(output_file):
        return True
    
    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            return not first_line.strip()
    except Exception:
        return True


# BGG API Functions

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


def get_unique_bgg_ids(relations_file):
    """Read relations file and return unique BGG IDs"""
    bgg_ids = set()
    
    with open(relations_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            bgg_id = row.get('bgg_id', '').strip()
            if bgg_id:
                bgg_ids.add(bgg_id)
    
    return sorted(list(bgg_ids))