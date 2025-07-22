#!/usr/bin/env python3

import duckdb
import pandas as pd
import sys
import os

def load_csv_to_duckdb(csv_file='data/finna_bgg_matches.csv', db_file='data/boardgames.db'):
    """Load CSV data into DuckDB database"""
    
    # Check if CSV file exists
    if not os.path.exists(csv_file):
        print(f"Error: CSV file '{csv_file}' not found")
        return False
    
    # Connect to DuckDB
    conn = duckdb.connect(db_file)
    
    print(f"Loading {csv_file} into DuckDB...")
    
    try:
        # Read CSV and create temporary table first
        conn.execute("""
            CREATE OR REPLACE TABLE games_raw AS 
            SELECT * FROM read_csv_auto(?, header=true)
        """, (csv_file,))
        
        print("Normalizing categories and mechanics...")
        
        # Create categories table
        conn.execute("""
            CREATE OR REPLACE TABLE categories AS
            SELECT DISTINCT 
                ROW_NUMBER() OVER (ORDER BY category) as category_id,
                category
            FROM (
                SELECT DISTINCT TRIM(unnest(string_split(bgg_categories, ';'))) as category
                FROM games_raw 
                WHERE bgg_categories IS NOT NULL AND bgg_categories != ''
            ) t
            WHERE category != ''
        """)
        
        # Create mechanics table
        conn.execute("""
            CREATE OR REPLACE TABLE mechanics AS
            SELECT DISTINCT 
                ROW_NUMBER() OVER (ORDER BY mechanic) as mechanic_id,
                mechanic
            FROM (
                SELECT DISTINCT TRIM(unnest(string_split(bgg_mechanics, ';'))) as mechanic
                FROM games_raw 
                WHERE bgg_mechanics IS NOT NULL AND bgg_mechanics != ''
            ) t
            WHERE mechanic != ''
        """)
        
        # Create main games table without raw categories/mechanics columns
        conn.execute("""
            CREATE OR REPLACE TABLE games AS
            SELECT 
                *,
                (bgg_id IS NOT NULL AND bgg_id::VARCHAR != '') as has_bgg_match,
                TRUE as library_available
            FROM games_raw
        """)
        
        # Create game_categories junction table
        conn.execute("""
            CREATE OR REPLACE TABLE game_categories AS
            SELECT DISTINCT
                g.id as game_id,
                c.category_id
            FROM games_raw g
            CROSS JOIN unnest(string_split(g.bgg_categories, ';')) as t(category)
            JOIN categories c ON TRIM(t.category) = c.category
            WHERE g.bgg_categories IS NOT NULL AND g.bgg_categories != ''
        """)
        
        # Create game_mechanics junction table
        conn.execute("""
            CREATE OR REPLACE TABLE game_mechanics AS
            SELECT DISTINCT
                g.id as game_id,
                m.mechanic_id
            FROM games_raw g
            CROSS JOIN unnest(string_split(g.bgg_mechanics, ';')) as t(mechanic)
            JOIN mechanics m ON TRIM(t.mechanic) = m.mechanic
            WHERE g.bgg_mechanics IS NOT NULL AND g.bgg_mechanics != ''
        """)
        
        # Drop the raw table
        conn.execute("DROP TABLE games_raw")
        
        print("Creating indexes...")
        # Create indexes for better query performance
        conn.execute("CREATE INDEX idx_games_bgg_rating ON games(bgg_average_rating)")
        conn.execute("CREATE INDEX idx_games_bgg_rank ON games(bgg_rank)")
        conn.execute("CREATE INDEX idx_games_title ON games(title)")
        conn.execute("CREATE INDEX idx_games_has_bgg ON games(has_bgg_match)")
        conn.execute("CREATE INDEX idx_games_id ON games(id)")
        
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
            SELECT title, bgg_primary_name, bgg_average_rating, bgg_rank
            FROM games 
            WHERE has_bgg_match = TRUE AND bgg_average_rating IS NOT NULL
            ORDER BY bgg_average_rating DESC 
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
        
        return True
        
    except Exception as e:
        print(f"Error loading data: {e}")
        return False
    finally:
        conn.close()

def main():
    # Parse command line arguments
    csv_file = sys.argv[1] if len(sys.argv) > 1 else 'data/finna_bgg_matches.csv'
    db_file = sys.argv[2] if len(sys.argv) > 2 else 'data/boardgames.db'
    
    success = load_csv_to_duckdb(csv_file, db_file)
    
    if success:
        print(f"\n🎲 Ready to use! Run: streamlit run dashboard.py")
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()