#!/usr/bin/env python3
"""
BGG (BoardGameGeek) API functions for searching and retrieving game data.
"""

import requests
import xml.etree.ElementTree as ET
import time
import json
from urllib.parse import quote
from tqdm import tqdm
from .retry_config import bgg_api_retry


@bgg_api_retry
def search_bgg_by_title(title):
    """Search BGG API for a game title"""
    url = f"https://boardgamegeek.com/xmlapi2/search?query={quote(title)}&type=boardgame"
    
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    
    if response.status_code == 202:
        # BGG is processing, return response to trigger retry
        return response
    
    time.sleep(1)  # Rate limiting
    return parse_bgg_search_response(response.content)


@bgg_api_retry
def _search_designer_id(author_name):
    """Find designer ID from BGG API."""
    designer_search_url = f"https://boardgamegeek.com/xmlapi2/search?query={quote(author_name)}&type=boardgamedesigner"
    
    response = requests.get(designer_search_url, timeout=10)
    response.raise_for_status()
    
    if response.status_code == 202:
        return response  # Trigger retry
    
    time.sleep(1)
    
    # Parse XML to find designer ID
    try:
        root = ET.fromstring(response.content)
        designer_items = root.findall('.//item[@type="boardgamedesigner"]')
        
        if not designer_items:
            tqdm.write(f"No designer found for '{author_name}'")
            return None
        
        designer_id = designer_items[0].get('id')
        tqdm.write(f"Found designer ID {designer_id} for '{author_name}'")
        return designer_id
        
    except ET.ParseError as e:
        tqdm.write(f"Error parsing designer search XML: {e}")
        return None


@bgg_api_retry
def _get_games_by_designer_id(designer_id, author_name):
    """Get games by designer ID using undocumented API."""
    games_url = f"https://api.geekdo.com/api/geekitem/linkeditems?linkdata_index=boardgamedesigner&objectid={designer_id}&objecttype=boardgamedesigner&pageid=1&showcount=100"
    
    games_response = requests.get(games_url, timeout=10)
    games_response.raise_for_status()
    time.sleep(1)  # Be respectful with undocumented API
    
    # Parse JSON response
    games_data = json.loads(games_response.content)
    game_ids = []
    
    for item in games_data.get('items', []):
        if item.get('objecttype') == 'thing' and item.get('subtype') == 'boardgame':
            game_ids.append(item.get('objectid'))
    
    tqdm.write(f"Found {len(game_ids)} games for designer '{author_name}'")
    return game_ids


def search_bgg_by_author(author_name):
    """
    Search BGG for games by a specific author using two-step API approach.
    
    First uses documented API to find designer ID, then undocumented API to get their games.
    """
    designer_id = _search_designer_id(author_name)
    if not designer_id:
        return []
    
    return _get_games_by_designer_id(designer_id, author_name)


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