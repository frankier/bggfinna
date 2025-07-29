#!/usr/bin/env python3

import duckdb
import pandas as pd
import sys
import os
import tempfile
import shutil
from bggfinna import get_data_path, is_test_mode, is_smoke_test_mode

def load_csv_to_duckdb(data_dir=None, db_file=None):
    """Load all CSV data into DuckDB database"""
    if data_dir is None:
        data_dir = get_data_path('')
    if db_file is None:
        db_file = get_data_path('boardgames.db')
    
    # Define the CSV files to load
    finna_csv = os.path.join(data_dir, 'finna_board_games.csv')
    relations_csv = os.path.join(data_dir, 'finna_bgg_relations.csv')
    bgg_csv = os.path.join(data_dir, 'bgg_games.csv')
    
    # Check if all CSV files exist
    for csv_file in [finna_csv, relations_csv, bgg_csv]:
        if not os.path.exists(csv_file):
            print(f"Error: CSV file '{csv_file}' not found")
            return False
    
    # Create temporary database file path
    temp_db_fd, temp_db_path = tempfile.mkstemp(suffix='.db')
    os.close(temp_db_fd)  # Close the file descriptor, we just need the path
    os.unlink(temp_db_path)  # Remove the empty file so DuckDB can create it
    
    # Connect to temporary DuckDB
    conn = duckdb.connect(temp_db_path)
    
    print(f"Loading CSV files from {data_dir} into temporary DuckDB...")
    
    try:
        # Load Finna games
        conn.execute("""
            CREATE TABLE finna_games AS 
            SELECT * FROM read_csv_auto(?, header=true)
        """, (finna_csv,))
        
        # Load BGG-Finna relations
        conn.execute("""
            CREATE TABLE finna_bgg_relations AS 
            SELECT * FROM read_csv_auto(?, header=true)
        """, (relations_csv,))
        
        # Load BGG games data
        conn.execute("""
            CREATE TABLE bgg_games AS 
            SELECT * FROM read_csv_auto(?, header=true)
        """, (bgg_csv,))
        
        print("Normalizing categories and mechanics...")
        
        # Create categories table
        conn.execute("""
            CREATE TABLE categories AS
            SELECT DISTINCT 
                ROW_NUMBER() OVER (ORDER BY category) as category_id,
                category
            FROM (
                SELECT DISTINCT TRIM(unnest(string_split(categories, ';'))) as category
                FROM bgg_games 
                WHERE categories IS NOT NULL AND categories != ''
            ) t
            WHERE category != ''
        """)
        
        # Create mechanics table
        conn.execute("""
            CREATE TABLE mechanics AS
            SELECT DISTINCT 
                ROW_NUMBER() OVER (ORDER BY mechanic) as mechanic_id,
                mechanic
            FROM (
                SELECT DISTINCT TRIM(unnest(string_split(mechanics, ';'))) as mechanic
                FROM bgg_games 
                WHERE mechanics IS NOT NULL AND mechanics != ''
            ) t
            WHERE mechanic != ''
        """)
        
        # Create main games view by joining Finna and BGG data
        conn.execute("""
            CREATE VIEW games AS
            SELECT 
                f.*,
                r.bgg_id,
                r.match_type,
                b.primary_name as bgg_primary_name,
                b.all_names as bgg_all_names,
                b.year as bgg_year,
                b.description as bgg_description,
                b.min_players as bgg_min_players,
                b.max_players as bgg_max_players,
                b.playing_time as bgg_playing_time,
                b.min_play_time as bgg_min_play_time,
                b.max_play_time as bgg_max_play_time,
                b.min_age as bgg_min_age,
                b.categories as bgg_categories,
                b.mechanics as bgg_mechanics,
                b.families as bgg_families,
                b.designers as bgg_designers,
                b.artists as bgg_artists,
                b.publishers as bgg_publishers,
                b.bgg_rank,
                b.average_rating as bgg_average_rating,
                b.bayes_average as bgg_bayes_average,
                b.users_rated as bgg_users_rated,
                b.weight as bgg_weight,
                b.owned as bgg_owned,
                (r.bgg_id IS NOT NULL AND r.bgg_id::VARCHAR != '') as has_bgg_match,
                TRUE as library_available
            FROM finna_games f
            LEFT JOIN finna_bgg_relations r ON f.id = r.finna_id
            LEFT JOIN bgg_games b ON r.bgg_id = b.bgg_id
        """)
        
        # Create game_categories junction table
        conn.execute("""
            CREATE TABLE game_categories AS
            SELECT DISTINCT
                g.bgg_id as game_id,
                c.category_id
            FROM bgg_games g
            CROSS JOIN unnest(string_split(g.categories, ';')) as t(category)
            JOIN categories c ON TRIM(t.category) = c.category
            WHERE g.categories IS NOT NULL AND g.categories != ''
        """)
        
        # Create game_mechanics junction table
        conn.execute("""
            CREATE TABLE game_mechanics AS
            SELECT DISTINCT
                g.bgg_id as game_id,
                m.mechanic_id
            FROM bgg_games g
            CROSS JOIN unnest(string_split(g.mechanics, ';')) as t(mechanic)
            JOIN mechanics m ON TRIM(t.mechanic) = m.mechanic
            WHERE g.mechanics IS NOT NULL AND g.mechanics != ''
        """)
        
        # Drop the raw table
        # Keep the original tables for reference
        
        print("Creating indexes on base tables...")
        # Create indexes on base tables for better query performance
        conn.execute("CREATE INDEX idx_finna_games_id ON finna_games(id)")
        conn.execute("CREATE INDEX idx_finna_bgg_relations_finna_id ON finna_bgg_relations(finna_id)")
        conn.execute("CREATE INDEX idx_finna_bgg_relations_bgg_id ON finna_bgg_relations(bgg_id)")
        conn.execute("CREATE INDEX idx_bgg_games_bgg_id ON bgg_games(bgg_id)")
        conn.execute("CREATE INDEX idx_bgg_games_rank ON bgg_games(bgg_rank)")
        conn.execute("CREATE INDEX idx_bgg_games_rating ON bgg_games(bayes_average)")
        
        # Add index on timestamp if column exists (for backward compatibility)
        try:
            conn.execute("CREATE INDEX idx_bgg_games_updated ON bgg_games(last_updated)")
        except Exception:
            # Column might not exist in older CSV files, skip index creation
            pass
        
        # Junction table indexes
        conn.execute("CREATE INDEX idx_game_categories_game ON game_categories(game_id)")
        conn.execute("CREATE INDEX idx_game_categories_cat ON game_categories(category_id)")
        conn.execute("CREATE INDEX idx_game_mechanics_game ON game_mechanics(game_id)")
        conn.execute("CREATE INDEX idx_game_mechanics_mech ON game_mechanics(mechanic_id)")
        
        # Get some stats
        total_games = conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]
        games_with_bgg = conn.execute("SELECT COUNT(*) FROM games WHERE has_bgg_match").fetchone()[0]
        total_categories = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        total_mechanics = conn.execute("SELECT COUNT(*) FROM mechanics").fetchone()[0]
        
        print(f"✅ Successfully loaded {total_games} games to DuckDB")
        print(f"   - {games_with_bgg} games have BGG data ({games_with_bgg/total_games*100:.1f}%)")
        print(f"   - {total_categories} unique categories normalized")
        print(f"   - {total_mechanics} unique mechanics normalized")
        print(f"   - Database saved to: {db_file}")
        
        # Show sample data
        print("\nSample of loaded data:")
        sample = conn.execute("""
            SELECT title, bgg_primary_name, bgg_bayes_average, bgg_rank
            FROM games 
            WHERE has_bgg_match = TRUE AND bgg_bayes_average IS NOT NULL
            ORDER BY bgg_bayes_average DESC 
            LIMIT 5
        """).fetchall()
        
        for row in sample:
            print(f"  {row[0]} -> {row[1]} (⭐{row[2]:.1f}, Rank #{row[3]})")
        
        # Show normalization sample
        print("\nSample categories:")
        cats = conn.execute("SELECT category FROM categories ORDER BY category LIMIT 5").fetchall()
        for cat in cats:
            print(f"  - {cat[0]}")
        
        print("\nSample mechanics:")
        mechs = conn.execute("SELECT mechanic FROM mechanics ORDER BY mechanic LIMIT 5").fetchall()
        for mech in mechs:
            print(f"  - {mech[0]}")
        
    except Exception as e:
        conn.close()
        # Clean up temporary file on error
        if os.path.exists(temp_db_path):
            os.unlink(temp_db_path)
        raise e
    else:
        # Close connection before moving file
        conn.close()
        
        # Move temporary database to final location
        print(f"Moving database to final location: {db_file}")
        shutil.move(temp_db_path, db_file)
        
        return True

def main():
    # Parse command line arguments with test mode support  
    data_dir = sys.argv[1] if len(sys.argv) > 1 else get_data_path('')
    db_file = sys.argv[2] if len(sys.argv) > 2 else get_data_path('boardgames.db')
    
    if is_smoke_test_mode():
        print("Running in SMOKE TEST mode - outputs will go to data/smoke/")
    elif is_test_mode():
        print("Running in TEST mode - outputs will go to data/test/")
    
    success = load_csv_to_duckdb(data_dir, db_file)
    
    if success:
        print(f"\n🎲 Ready to use! Run: streamlit run dashboard.py")
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
