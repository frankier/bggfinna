#!/usr/bin/env python3

import pytest
from match_with_bgg import calculate_fuzzy_score, normalize_title_for_matching, rank_substring_matches_by_fuzzy_score


def test_normalize_title_for_matching():
    """Test title normalization"""
    assert normalize_title_for_matching("The Quest for El Dorado!") == "the quest for el dorado"
    assert normalize_title_for_matching("Ticket to Ride: Germany") == "ticket to ride germany"
    assert normalize_title_for_matching("") == ""
    assert normalize_title_for_matching(None) == ""


def test_fuzzy_score_exact_matches():
    """Test fuzzy scoring for exact matches"""
    score = calculate_fuzzy_score("Arkham Horror", "Arkham Horror")
    assert score == 100
    
    score = calculate_fuzzy_score("Ticket to Ride", "Ticket to Ride")
    assert score == 100


def test_fuzzy_score_similar_titles():
    """Test fuzzy scoring for similar but not identical titles"""
    # Test clear differentiation between base game and expansion
    ticket_base_score = calculate_fuzzy_score("Ticket to ride", "Ticket to Ride")
    ticket_expansion_score = calculate_fuzzy_score("Ticket to ride", "Ticket to Ride: Germany")
    
    # The base game should score higher than the expansion
    assert ticket_base_score > ticket_expansion_score
    assert ticket_base_score == 100  # Should be perfect match
    assert ticket_expansion_score > 70  # Still a good match
    
    # The problematic El Dorado case - now properly differentiated
    eldorado_base_score = calculate_fuzzy_score("Quest for Eldorado", "The Quest for El Dorado")
    eldorado_expansion_score = calculate_fuzzy_score("Quest for Eldorado", "The Quest for El Dorado: The Golden Temples")
    
    # Base game should score higher than expansion
    assert eldorado_base_score > eldorado_expansion_score
    assert eldorado_base_score > 80  # Should be a good match
    assert eldorado_expansion_score > 50  # Still reasonable match but lower


def test_fuzzy_score_poor_matches():
    """Test fuzzy scoring for poor matches"""
    score = calculate_fuzzy_score("Monopoly", "Scrabble")
    assert score < 30
    
    score = calculate_fuzzy_score("Chess", "The Quest for El Dorado")
    assert score < 30


def test_substring_ranking():
    """Test that substring matches are properly ranked by fuzzy score"""
    # Mock BGG games data - use a clearer substring case
    mock_bgg_games = [
        {
            'bgg_id': '1',
            'names': ['Arkham Horror: The Card Game'],
            'year': 2016
        },
        {
            'bgg_id': '2', 
            'names': ['Arkham Horror: The Card Game – The Dunwich Legacy'],
            'year': 2017
        }
    ]
    
    finna_titles = ['Arkham Horror']
    
    # Both should match as substrings, but base game should score higher
    matches = rank_substring_matches_by_fuzzy_score(mock_bgg_games, finna_titles)
    
    assert len(matches) == 1  # Should return the best match
    assert matches[0]['bgg_id'] == '1'  # Should be the base game
    assert matches[0]['match_type'] == 'substring'
    assert 'fuzzy_score' in matches[0]
    assert matches[0]['fuzzy_score'] > 60


def test_no_substring_matches():
    """Test behavior when no substring matches are found"""
    mock_bgg_games = [
        {
            'bgg_id': '1',
            'names': ['Completely Different Game'],
            'year': 2020
        }
    ]
    
    finna_titles = ['Quest for Eldorado']
    
    matches = rank_substring_matches_by_fuzzy_score(mock_bgg_games, finna_titles)
    assert len(matches) == 0


def test_single_word_titles_excluded():
    """Test that single-word titles are excluded from substring matching"""
    mock_bgg_games = [
        {
            'bgg_id': '1',
            'names': ['Chess Master Championship'],
            'year': 2020
        }
    ]
    
    # Single word should not match
    finna_titles = ['Chess']
    matches = rank_substring_matches_by_fuzzy_score(mock_bgg_games, finna_titles)
    assert len(matches) == 0