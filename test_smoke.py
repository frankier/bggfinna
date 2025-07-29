#!/usr/bin/env python3
"""
Smoke test for the bggfinna pipeline.

This test runs the full pipeline in smoke test mode (BGGFINNA_TEST=2) 
to ensure all components work together with minimal data.
"""

import os
import sys
import subprocess
import tempfile
import shutil
from pathlib import Path


def run_command(cmd, description, cwd=None):
    """Run a command and return success status"""
    print(f"\n{'='*50}")
    print(f"RUNNING: {description}")
    print(f"COMMAND: {cmd}")
    print(f"{'='*50}")
    
    result = subprocess.run(
        cmd, 
        shell=True, 
        cwd=cwd or os.getcwd(),
        capture_output=True,
        text=True
    )
    
    if result.stdout:
        print("STDOUT:", result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    success = result.returncode == 0
    print(f"RESULT: {'✅ SUCCESS' if success else '❌ FAILED'}")
    
    return success


def check_file_exists(file_path, description):
    """Check if a file exists and report its size"""
    if os.path.exists(file_path):
        size = os.path.getsize(file_path)
        print(f"✅ {description}: {file_path} ({size:,} bytes)")
        return True
    else:
        print(f"❌ {description}: {file_path} (missing)")
        return False


def main():
    """Run the smoke test"""
    print("BGG+FINNA PIPELINE SMOKE TEST")
    print("=" * 60)
    print("This test runs the complete pipeline with BGGFINNA_TEST=2")
    print("to process just 1 record and verify all steps work.")
    print("=" * 60)
    
    # Set smoke test environment
    env = os.environ.copy()
    env['BGGFINNA_TEST'] = '2'
    
    # Clean up any existing smoke test data
    smoke_dir = Path('data/smoke')
    if smoke_dir.exists():
        print(f"🧹 Cleaning up existing smoke test data: {smoke_dir}")
        shutil.rmtree(smoke_dir)
    
    # Run the complete pipeline
    pipeline_cmd = '. "$HOME/.cargo/env" && uv run python run_pipeline.py'
    
    print("\n🚀 Starting pipeline smoke test...")
    result = subprocess.run(
        pipeline_cmd,
        shell=True,
        env=env,
        capture_output=True,
        text=True
    )
    
    print(f"\nPipeline exit code: {result.returncode}")
    if result.stdout:
        print(f"Pipeline stdout:\n{result.stdout}")
    if result.stderr:
        print(f"Pipeline stderr:\n{result.stderr}")
    
    success = result.returncode == 0
    
    if not success:
        print("❌ SMOKE TEST FAILED: Pipeline execution failed")
        return 1
    
    # Verify all expected output files exist
    print("\n🔍 Verifying output files...")
    smoke_data_dir = Path('data/smoke')
    
    expected_files = [
        ('finna_board_games.csv', 'Finna games data'),
        ('finna_bgg_relations.csv', 'Finna-BGG relations'),
        ('bgg_games.csv', 'BGG game details'),
        ('boardgames.db', 'DuckDB database')
    ]
    
    all_files_exist = True
    for filename, description in expected_files:
        file_path = smoke_data_dir / filename
        if not check_file_exists(file_path, description):
            all_files_exist = False
    
    if not all_files_exist:
        print("❌ SMOKE TEST FAILED: Missing output files")
        return 1
    
    # Additional validation - check that files have content
    finna_games_file = smoke_data_dir / 'finna_board_games.csv'
    try:
        with open(finna_games_file, 'r') as f:
            lines = f.readlines()
            if len(lines) < 2:  # Header + at least 1 data row
                print("❌ SMOKE TEST FAILED: Finna games file has no data rows")
                return 1
            print(f"✅ Finna games file has {len(lines)-1} data rows")
    except Exception as e:
        print(f"❌ SMOKE TEST FAILED: Could not read finna games file: {e}")
        return 1
    
    print("\n" + "=" * 60)
    print("🎉 SMOKE TEST PASSED!")
    print("All pipeline steps completed successfully with minimal data.")
    print("=" * 60)
    
    # Clean up smoke test data
    print(f"\n🧹 Cleaning up smoke test data: {smoke_dir}")
    if smoke_dir.exists():
        shutil.rmtree(smoke_dir)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())