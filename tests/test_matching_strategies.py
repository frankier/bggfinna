#!/usr/bin/env python3

import pytest
from match_with_bgg import (
    try_exact_matches, 
    try_substring_matches_with_fuzzy_ranking,
    try_author_fuzzy_title_matching,
    try_author_year_matching,
    find_best_bgg_match
)
from bggfinna.bggapi import search_bgg_by_author


def test_try_exact_matches(monkeypatch):
    """Test the exact matching strategy"""
    
    def mock_search_bgg_by_title(title):
        if title == "Arkham Horror":
            return [
                {'bgg_id': '15987', 'names': ['Arkham Horror'], 'year': 2005},
                {'bgg_id': '34', 'names': ['Arkham Horror'], 'year': 1987}
            ]
        return []
    
    monkeypatch.setattr('match_with_bgg.search_bgg_by_title', mock_search_bgg_by_title)
    
    # Test successful exact match
    matches = try_exact_matches(['Arkham Horror'])
    assert len(matches) == 2
    assert matches[0]['match_type'] == 'exact'
    assert matches[0]['bgg_id'] == '15987'
    
    # Test no matches
    matches = try_exact_matches(['Nonexistent Game'])
    assert len(matches) == 0
    
    # Test empty titles
    matches = try_exact_matches(['', None])
    assert len(matches) == 0


def test_try_substring_matches_with_fuzzy_ranking(monkeypatch):
    """Test the substring matching with fuzzy ranking strategy"""
    
    def mock_search_bgg_by_title(title):
        if "Arkham Horror" in title:
            return [
                {'bgg_id': '1', 'names': ['Arkham Horror: The Card Game'], 'year': 2016},
                {'bgg_id': '2', 'names': ['Arkham Horror: The Card Game – The Dunwich Legacy'], 'year': 2017}
            ]
        return []
    
    monkeypatch.setattr('match_with_bgg.search_bgg_by_title', mock_search_bgg_by_title)
    
    # Test successful substring match with fuzzy ranking
    matches = try_substring_matches_with_fuzzy_ranking(['Arkham Horror'])
    assert len(matches) == 1
    assert matches[0]['match_type'] == 'substring'
    assert matches[0]['bgg_id'] == '1'  # Should pick the base game
    assert 'fuzzy_score' in matches[0]
    
    # Test single-word titles are excluded
    matches = try_substring_matches_with_fuzzy_ranking(['Chess'])
    assert len(matches) == 0
    
    # Test no substring matches
    matches = try_substring_matches_with_fuzzy_ranking(['Completely Different Game'])
    assert len(matches) == 0


def test_try_author_fuzzy_title_matching(monkeypatch):
    """Test the author + fuzzy title matching strategy"""
    
    def mock_search_bgg_by_author(author):
        if author == "Reiner Knizia":
            return ['12345', '67890']
        return []
    
    def mock_get_bgg_game_details(bgg_id):
        if bgg_id == '12345':
            return {
                'all_names': ['Ra', 'Ra: The Dice Game'],
                'year': '1999',
                'designers': ['Reiner Knizia'],
                'primary_name': 'Ra'
            }
        elif bgg_id == '67890':
            return {
                'all_names': ['Lost Cities'],
                'year': '1999', 
                'designers': ['Reiner Knizia'],
                'primary_name': 'Lost Cities'
            }
        return None
    
    monkeypatch.setattr('match_with_bgg.search_bgg_by_author', mock_search_bgg_by_author)
    monkeypatch.setattr('match_with_bgg.get_bgg_game_details', mock_get_bgg_game_details)
    
    # Test successful author + fuzzy match
    matches = try_author_fuzzy_title_matching(['Reiner Knizia'], ['Ra'])
    assert len(matches) == 1
    assert matches[0]['match_type'] == 'author_fuzzy_title'
    assert matches[0]['bgg_id'] == '12345'
    
    # Test no author matches
    matches = try_author_fuzzy_title_matching(['Unknown Author'], ['Some Game'])
    assert len(matches) == 0
    
    # Test empty authors list
    matches = try_author_fuzzy_title_matching([], ['Some Game'])
    assert len(matches) == 0


def test_try_author_year_matching(monkeypatch):
    """Test the author + year matching strategy"""
    
    def mock_search_bgg_by_author(author):
        if author == "Reiner Knizia":
            return ['12345', '67890']
        return []
    
    def mock_get_bgg_game_details(bgg_id):
        if bgg_id == '12345':
            return {
                'all_names': ['Ra'],
                'year': '1999',
                'designers': ['Reiner Knizia'],
                'primary_name': 'Ra'
            }
        elif bgg_id == '67890':
            return {
                'all_names': ['Lost Cities'],
                'year': '2000',
                'designers': ['Reiner Knizia'], 
                'primary_name': 'Lost Cities'
            }
        return None
    
    monkeypatch.setattr('match_with_bgg.search_bgg_by_author', mock_search_bgg_by_author)
    monkeypatch.setattr('match_with_bgg.get_bgg_game_details', mock_get_bgg_game_details)
    
    # Test successful author + year match
    matches = try_author_year_matching(['Reiner Knizia'], 1999)
    assert len(matches) == 1
    assert matches[0]['match_type'] == 'author_year'
    assert matches[0]['bgg_id'] == '12345'
    assert matches[0]['year'] == '1999'
    
    # Test no year match
    matches = try_author_year_matching(['Reiner Knizia'], 2010)
    assert len(matches) == 0
    
    # Test no author matches
    matches = try_author_year_matching(['Unknown Author'], 1999)
    assert len(matches) == 0


def test_find_best_bgg_match_exact_strategy(monkeypatch):
    """Test find_best_bgg_match using exact matching strategy"""
    
    def mock_search_bgg_by_title(title):
        if title == "Arkham Horror":
            return [{'bgg_id': '15987', 'names': ['Arkham Horror'], 'year': 2005}]
        return []
    
    monkeypatch.setattr('match_with_bgg.search_bgg_by_title', mock_search_bgg_by_title)
    
    finna_game = {
        'title': 'Arkham Horror',
        'alternativeTitles': '',
        'year': '2005',
        'authors': ''
    }
    
    match = find_best_bgg_match(finna_game)
    assert match is not None
    assert match['bgg_id'] == '15987'
    assert match['match_type'] == 'exact'


def test_find_best_bgg_match_substring_fallback(monkeypatch):
    """Test find_best_bgg_match falling back to substring matching"""
    
    def mock_search_bgg_by_title(title):
        # No exact matches, but substring matches exist
        if title == "Arkham Horror":
            return [
                {'bgg_id': '1', 'names': ['Arkham Horror: The Card Game'], 'year': 2016},
                {'bgg_id': '2', 'names': ['Arkham Horror: The Card Game – The Dunwich Legacy'], 'year': 2017}
            ]
        return []
    
    monkeypatch.setattr('match_with_bgg.search_bgg_by_title', mock_search_bgg_by_title)
    
    finna_game = {
        'title': 'Arkham Horror',
        'alternativeTitles': '',
        'year': '2016',
        'authors': ''
    }
    
    match = find_best_bgg_match(finna_game)
    assert match is not None
    assert match['bgg_id'] == '1'  # Should pick base game over expansion
    assert match['match_type'] == 'substring'


def test_find_best_bgg_match_no_matches(monkeypatch):
    """Test find_best_bgg_match when no matches are found"""
    
    def mock_search_bgg_by_title(title):
        return []
    
    def mock_search_bgg_by_author(author):
        return []
    
    monkeypatch.setattr('match_with_bgg.search_bgg_by_title', mock_search_bgg_by_title)
    monkeypatch.setattr('match_with_bgg.search_bgg_by_author', mock_search_bgg_by_author)
    
    finna_game = {
        'title': 'Completely Unknown Game',
        'alternativeTitles': '',
        'year': '2020',
        'authors': '{"primary": {"Unknown Author": {"role": ["-"]}}}'
    }
    
    match = find_best_bgg_match(finna_game)
    assert match is None


def test_find_best_bgg_match_multiple_matches_year_disambiguation(monkeypatch):
    """Test find_best_bgg_match with multiple matches using year for disambiguation"""
    
    def mock_search_bgg_by_title(title):
        if title == "Arkham Horror":
            return [
                {'bgg_id': '15987', 'names': ['Arkham Horror'], 'year': 2005},
                {'bgg_id': '34', 'names': ['Arkham Horror'], 'year': 1987}
            ]
        return []
    
    monkeypatch.setattr('match_with_bgg.search_bgg_by_title', mock_search_bgg_by_title)
    
    finna_game = {
        'title': 'Arkham Horror',
        'alternativeTitles': '', 
        'year': '2005',
        'authors': ''
    }
    
    match = find_best_bgg_match(finna_game)
    assert match is not None
    assert match['bgg_id'] == '15987'  # Should pick 2005 version
    assert match['year'] == 2005


def test_find_best_bgg_match_alternative_titles(monkeypatch):
    """Test find_best_bgg_match using alternative titles"""
    
    def mock_search_bgg_by_title(title):
        if title == "Menolippu":
            return [{'bgg_id': '9209', 'names': ['Ticket to Ride'], 'year': 2004}]
        return []
    
    monkeypatch.setattr('match_with_bgg.search_bgg_by_title', mock_search_bgg_by_title)
    
    finna_game = {
        'title': 'Ticket to Ride',
        'alternativeTitles': 'Menolippu',
        'year': '2004',
        'authors': ''
    }
    
    match = find_best_bgg_match(finna_game)
    assert match is not None
    assert match['bgg_id'] == '9209'
    assert match['match_type'] == 'exact'


def test_search_bgg_by_author_success(monkeypatch):
    """Test successful author search using mocked APIs"""
    
    def mock_requests_get(url, **kwargs):
        class MockResponse:
            def __init__(self, content, status_code=200):
                self.content = content
                self.status_code = status_code
            
            def raise_for_status(self):
                pass
        
        
        if "search" in url and ("Reiner+Knizia" in url or "Reiner%20Knizia" in url):
            # Mock designer search response
            xml_content = '''<?xml version="1.0" encoding="utf-8"?>
            <items total="1" termsofuse="https://boardgamegeek.com/xmlapi/termsofuse">
                <item type="boardgamedesigner" id="2">
                    <name type="primary" value="Reiner Knizia"/>
                </item>
            </items>'''
            return MockResponse(xml_content.encode())
        
        elif "api.geekdo.com" in url and "objectid=2" in url:
            # Mock games by designer response
            json_content = '''{
                "items": [
                    {"objecttype": "thing", "subtype": "boardgame", "objectid": "12", "name": "Ra"},
                    {"objecttype": "thing", "subtype": "boardgame", "objectid": "6249", "name": "Lost Cities"},
                    {"objecttype": "thing", "subtype": "boardgame", "objectid": "15", "name": "Samurai"}
                ]
            }'''
            return MockResponse(json_content.encode())
        
        return MockResponse(b'', 404)
    
    import bggfinna.bggapi
    import requests
    
    # Create a proper mock requests module with exceptions
    class MockRequests:
        def __init__(self):
            self.get = mock_requests_get
            self.exceptions = requests.exceptions
    
    monkeypatch.setattr(bggfinna.bggapi, 'requests', MockRequests())
    
    game_ids = search_bgg_by_author('Reiner Knizia')
    assert len(game_ids) == 3
    assert '12' in game_ids  # Ra
    assert '6249' in game_ids  # Lost Cities
    assert '15' in game_ids  # Samurai


def test_search_bgg_by_author_no_designer_found(monkeypatch):
    """Test author search when designer is not found"""
    
    def mock_requests_get(url, **kwargs):
        class MockResponse:
            def __init__(self, content, status_code=200):
                self.content = content
                self.status_code = status_code
            
            def raise_for_status(self):
                pass
        
        if "search" in url:
            # Mock empty designer search response
            xml_content = '''<?xml version="1.0" encoding="utf-8"?>
            <items total="0" termsofuse="https://boardgamegeek.com/xmlapi/termsofuse">
            </items>'''
            return MockResponse(xml_content.encode())
        
        return MockResponse(b'', 404)
    
    import bggfinna.bggapi
    import requests
    
    class MockRequests:
        def __init__(self):
            self.get = mock_requests_get
            self.exceptions = requests.exceptions
    
    monkeypatch.setattr(bggfinna.bggapi, 'requests', MockRequests())
    
    game_ids = search_bgg_by_author('Unknown Designer')
    assert len(game_ids) == 0


def test_search_bgg_by_author_api_error(monkeypatch):
    """Test author search when API requests fail"""
    
    def mock_requests_get(url, **kwargs):
        import requests
        raise requests.exceptions.RequestException("API error")
    
    import bggfinna.bggapi
    import requests
    
    class MockRequests:
        def __init__(self):
            self.get = mock_requests_get
            self.exceptions = requests.exceptions
    
    monkeypatch.setattr(bggfinna.bggapi, 'requests', MockRequests())
    
    game_ids = search_bgg_by_author('Some Designer')
    assert len(game_ids) == 0


def test_search_bgg_by_author_invalid_json(monkeypatch):
    """Test author search when games API returns invalid JSON"""
    
    def mock_requests_get(url, **kwargs):
        class MockResponse:
            def __init__(self, content, status_code=200):
                self.content = content
                self.status_code = status_code
            
            def raise_for_status(self):
                pass
        
        if "search" in url:
            # Mock valid designer search response
            xml_content = '''<?xml version="1.0" encoding="utf-8"?>
            <items total="1" termsofuse="https://boardgamegeek.com/xmlapi/termsofuse">
                <item type="boardgamedesigner" id="123">
                    <name type="primary" value="Test Designer"/>
                </item>
            </items>'''
            return MockResponse(xml_content.encode())
        
        elif "api.geekdo.com" in url:
            # Mock invalid JSON response
            return MockResponse(b'{invalid json}')
        
        return MockResponse(b'', 404)
    
    import bggfinna.bggapi
    import requests
    
    class MockRequests:
        def __init__(self):
            self.get = mock_requests_get
            self.exceptions = requests.exceptions
    
    monkeypatch.setattr(bggfinna.bggapi, 'requests', MockRequests())
    
    game_ids = search_bgg_by_author('Test Designer')
    assert len(game_ids) == 0