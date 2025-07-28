import duckdb
import os
from bggfinna import get_data_path

def analyze_matching_methods():
    db_file = get_data_path('boardgames.db')
    
    if not os.path.exists(db_file):
        print(f"Database not found at {db_file}. Please run the pipeline first.")
        return
    
    conn = duckdb.connect(db_file)
    
    # Get overall stats
    total_games = conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]
    
    # Get matching method breakdown
    match_stats = conn.execute("""
        SELECT 
            COALESCE(match_type, 'no_match') as method,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / ?, 1) as percentage
        FROM games
        GROUP BY match_type
        ORDER BY count DESC
    """, (total_games,)).fetchall()
    
    print(f"BGG Matching Methods Analysis")
    print(f"=" * 40)
    print(f"Total games in database: {total_games:,}")
    print()
    
    print("Matching method breakdown:")
    print("-" * 30)
    for method, count, percentage in match_stats:
        method_display = method if method != 'no_match' else 'No BGG match'
        print(f"{method_display:20} | {count:4,} ({percentage:5.1f}%)")
    
    # Show examples for each matching method
    print("\nExamples by matching method:")
    print("=" * 50)
    
    for method, _, _ in match_stats:
        if method == 'no_match':
            continue
            
        print(f"\n{method.upper()} matches:")
        print("-" * 20)
        
        examples = conn.execute("""
            SELECT title, bgg_primary_name, bgg_bayes_average
            FROM games 
            WHERE match_type = ?
            AND bgg_primary_name IS NOT NULL
            ORDER BY bgg_bayes_average DESC NULLS LAST
            LIMIT 5
        """, (method,)).fetchall()
        
        for title, bgg_name, rating in examples:
            rating_str = f"⭐{rating:.1f}" if rating else "Unrated"
            print(f"  Finna: {title} | BGG: {bgg_name} ({rating_str})")
        
        if not examples:
            # Show examples without BGG names for methods that might not have matches
            examples = conn.execute("""
                SELECT title
                FROM games 
                WHERE match_type = ?
                LIMIT 5
            """, (method,)).fetchall()
            
            for (title,) in examples:
                print(f"  {title}")
    
    # Show no-match examples
    print(f"\nNO BGG MATCH examples:")
    print("-" * 20)
    
    no_match_examples = conn.execute("""
        SELECT title, year, publishers
        FROM games 
        WHERE match_type IS NULL OR NOT has_bgg_match
        ORDER BY year DESC NULLS LAST
        LIMIT 5
    """).fetchall()
    
    for title, year, publishers in no_match_examples:
        year_str = f"({year})" if year else ""
        pub_str = f" - {publishers}" if publishers else ""
        print(f"  {title} {year_str}{pub_str}")
    
    conn.close()

if __name__ == "__main__":
    analyze_matching_methods()