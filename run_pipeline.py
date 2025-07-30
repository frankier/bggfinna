#!/usr/bin/env python3
"""
Pipeline script to run the entire database creation process:
1. Fetch Finna board games data
2. Match Finna games with BGG (creates relations)
3. Fetch unique BGG game details
4. Fetch Finna availability/location information
5. Load all data into DuckDB
"""

import subprocess
import sys
import os
import time
from bggfinna import get_data_path, is_test_mode, is_smoke_test_mode

def run_command(cmd, description):
    """Run a command and handle errors"""
    print(f"\n{'='*60}")
    print(f"STEP: {description}")
    print(f"COMMAND: {cmd}")
    print(f"{'='*60}")
    
    start_time = time.time()
    result = subprocess.run(cmd, shell=True)
    end_time = time.time()
    
    duration = end_time - start_time
    print(f"\nCompleted in {duration:.1f} seconds")
    
    if result.returncode != 0:
        print(f"ERROR: Command failed with exit code {result.returncode}")
        return False
    
    return True

def check_file_exists(filepath, description):
    """Check if a file exists and show info"""
    if os.path.exists(filepath):
        size = os.path.getsize(filepath)
        print(f"✓ {description}: {filepath} ({size:,} bytes)")
        return True
    else:
        print(f"✗ {description}: {filepath} (missing)")
        return False

def main():
    """Run the complete pipeline"""
    print("BOARD GAME LIBRARY DATABASE CREATION PIPELINE")
    print("=" * 60)
    
    if is_smoke_test_mode():
        print("🚨 SMOKE TEST MODE ENABLED - Processing 1 record, outputs will go to data/smoke/")
        print("=" * 60)
    elif is_test_mode():
        print("🧪 TEST MODE ENABLED - All outputs will go to data/test/")
        print("=" * 60)
    
    # Ensure data directory exists (get_data_path handles test mode)
    get_data_path('')  # This will create the appropriate directory
    
    # Step 1: Fetch Finna games
    step1_cmd = '. "$HOME/.cargo/env" && uv run python fetch_finna_games.py'
    if not run_command(step1_cmd, "Fetch Finna board games data"):
        print("Pipeline failed at step 1")
        return 1
    
    # Check Step 1 output
    if not check_file_exists(get_data_path('finna_board_games.csv'), 'Finna games CSV'):
        print("Step 1 output file missing")
        return 1
    
    # Step 2: Match with BGG (create relations)
    step2_cmd = '. "$HOME/.cargo/env" && uv run python match_with_bgg.py'
    if not run_command(step2_cmd, "Match Finna games with BGG (create relations)"):
        print("Pipeline failed at step 2")
        return 1
    
    # Check Step 2 output
    if not check_file_exists(get_data_path('finna_bgg_relations.csv'), 'Finna-BGG relations CSV'):
        print("Step 2 output file missing")
        return 1
    
    # Step 3: Fetch BGG game details
    step3_cmd = '. "$HOME/.cargo/env" && uv run python fetch_bgg_games.py'
    if not run_command(step3_cmd, "Fetch detailed BGG game information"):
        print("Pipeline failed at step 3")
        return 1
    
    # Check Step 3 output
    if not check_file_exists(get_data_path('bgg_games.csv'), 'BGG games CSV'):
        print("Step 3 output file missing")
        return 1
    
    # Step 4: Fetch Finna availability information
    step4_cmd = '. "$HOME/.cargo/env" && uv run python fetch_finna_availability.py'
    if not run_command(step4_cmd, "Fetch Finna availability/location information"):
        print("Pipeline failed at step 4")
        return 1
    
    # Check Step 4 output
    if not check_file_exists(get_data_path('finna_availability.csv'), 'Finna availability CSV'):
        print("Step 4 output file missing")
        return 1
    
    # Step 5: Load into DuckDB
    step5_cmd = '. "$HOME/.cargo/env" && uv run python load_to_duckdb.py'
    if not run_command(step5_cmd, "Load data into DuckDB database"):
        print("Pipeline failed at step 5")
        return 1
    
    # Check Step 5 output
    if not check_file_exists(get_data_path('boardgames.db'), 'DuckDB database'):
        print("Step 5 output file missing")
        return 1
    
    # Final summary
    print(f"\n{'='*60}")
    print("PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"{'='*60}")
    print("\nGenerated files:")
    check_file_exists(get_data_path('finna_board_games.csv'), 'Finna games data')
    check_file_exists(get_data_path('finna_bgg_relations.csv'), 'Finna-BGG relations')
    check_file_exists(get_data_path('bgg_games.csv'), 'BGG game details')
    check_file_exists(get_data_path('finna_availability.csv'), 'Finna availability data')
    check_file_exists(get_data_path('boardgames.db'), 'DuckDB database')
    
    print(f"\nYou can now run the dashboard with:")
    print(f". \"$HOME/.cargo/env\" && uv run python dashboard.py")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())