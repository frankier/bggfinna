import duckdb
import os
from bggfinna import get_data_path

def check_duplicates():
    db_file = get_data_path('boardgames.db')
    
    if not os.path.exists(db_file):
        print(f"Database not found at {db_file}. Please run the pipeline first.")
        return
    
    conn = duckdb.connect(db_file)
    
    # Get total games with BGG matches
    total_result = conn.execute("""
        SELECT COUNT(*) as total_games,
               COUNT(CASE WHEN has_bgg_match THEN 1 END) as games_with_bgg
        FROM games
    """).fetchone()
    
    total_games, games_with_bgg = total_result
    
    # Get unique BGG IDs among matched games
    unique_bgg_result = conn.execute("""
        SELECT COUNT(DISTINCT bgg_id) as unique_bgg_ids
        FROM games 
        WHERE has_bgg_match AND bgg_id IS NOT NULL
    """).fetchone()
    
    unique_bgg_ids = unique_bgg_result[0]
    duplicates_count = games_with_bgg - unique_bgg_ids
    
    print('Total games in database:', total_games)
    print('Games with BGG matches:', games_with_bgg)
    print('Unique BGG IDs:', unique_bgg_ids)
    print('Duplicate BGG IDs:', duplicates_count)
    
    if duplicates_count > 0:
        # Find top 5 most duplicated BGG IDs
        duplicates_query = """
        SELECT bgg_id, COUNT(*) as duplicate_count
        FROM games 
        WHERE has_bgg_match AND bgg_id IS NOT NULL
        GROUP BY bgg_id
        HAVING COUNT(*) > 1
        ORDER BY duplicate_count DESC
        LIMIT 5
        """
        
        top_duplicated = conn.execute(duplicates_query).fetchall()

        print('\nTop 5 most duplicated games:')
        for bgg_id, count in top_duplicated:
            print(f'\nBGG ID {bgg_id} ({count} duplicates):')
            
            matches_query = """
            SELECT id, title, bgg_primary_name, publishers, year
            FROM games 
            WHERE bgg_id = ?
            ORDER BY title
            """
            
            matches = conn.execute(matches_query, (bgg_id,)).fetchall()
            for row in matches:
                finna_id, title, bgg_name, publishers, year = row
                publishers_str = publishers or 'Unknown'
                year_str = year or 'Unknown'
                print(f'  - {finna_id}: {title} ({year_str}) - {publishers_str}')
    
    conn.close()

if __name__ == "__main__":
    check_duplicates()