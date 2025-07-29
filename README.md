# BGG + Finna Board Game Explorer

Discover the top-rated board games available in Finnish libraries by combining data from the Finna API with BoardGameGeek (BGG) ratings and metadata.

[![GitHub](https://img.shields.io/badge/GitHub-frankier%2Fbggfinna-blue)](https://github.com/frankier/bggfinna)

## Overview

This pipeline fetches board game data from Finnish libraries via the Finna API, matches games with their BoardGameGeek counterparts to obtain ratings and detailed metadata, and creates a searchable database. A Streamlit dashboard provides an interactive interface to explore the data.

## Setup

Ensure you have the required tools:
- Python with uv package manager

## Running the Pipeline

### Full Pipeline

Run the complete data processing pipeline:

```bash
uv run python run_pipeline.py
```

This executes four steps:
1. **Fetch Finna games** - Downloads board game data from Finnish libraries
2. **Match with BGG** - Links Finna games to BoardGameGeek entries  
3. **Fetch BGG details** - Gets ratings, mechanics, categories from BGG API
4. **Load to DuckDB** - Creates the final searchable database

### Individual Steps

Run pipeline steps independently:

```bash
# Step 1: Fetch library data
uv run python fetch_finna_games.py

# Step 2: Match games with BGG
uv run python match_with_bgg.py

# Step 3: Get BGG game details
uv run python fetch_bgg_games.py

# Step 4: Create database
uv run python load_to_duckdb.py
```

## Test Mode

Enable test mode to process a smaller dataset during development:

```bash
export BGGFINNA_TEST=1
uv run python run_pipeline.py
```

Test mode:
- Limits Finna fetch to 10 records for faster iteration
- Outputs to `data/test/` instead of `data/`
- Uses smaller API request batches

## Dashboard

Launch the interactive Streamlit dashboard after running the pipeline:

```bash
uv run streamlit run dashboard.py
```

The dashboard provides:
- **Filtering** - By rating, players, playtime, age, categories, mechanics
- **Statistics** - Rating distributions and category breakdowns  
- **Game browser** - Sortable list with library and BGG links
- **Search** - Find specific games by title

## Data Files

Pipeline generates these files in `data/` (or `data/test/` in test mode):

- `finna_board_games.csv` - Raw library data from Finna
- `finna_bgg_relations.csv` - Mapping between Finna and BGG games
- `bgg_games.csv` - Detailed BGG game information
- `boardgames.db` - Final DuckDB database for the dashboard

## Configuration

Test mode is controlled by the `BGGFINNA_TEST` environment variable. Set to any value to enable.