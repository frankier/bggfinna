#!/usr/bin/env python3

import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os

# Page config
st.set_page_config(
    page_title="BGG + Finna Board Game Explorer",
    page_icon="🎲",
    layout="wide"
)

@st.cache_resource
def get_db_connection():
    """Get DuckDB connection"""
    db_path = 'data/boardgames.db'
    if not os.path.exists(db_path):
        st.error(f"Database not found at {db_path}. Please run: python load_to_duckdb.py")
        st.stop()
    return duckdb.connect(db_path)

def query_db(query, params=None):
    """Execute query on DuckDB"""
    conn = get_db_connection()
    if params:
        return conn.execute(query, params).fetchdf()
    else:
        return conn.execute(query).fetchdf()

def get_categories_list():
    """Get unique categories from the database"""
    categories_df = query_db("""
        SELECT category_id, category
        FROM categories 
        ORDER BY category
    """)
    return [(row['category_id'], row['category']) for _, row in categories_df.iterrows()]

def get_mechanics_list():
    """Get unique mechanics from the database"""
    mechanics_df = query_db("""
        SELECT mechanic_id, mechanic
        FROM mechanics 
        ORDER BY mechanic
    """)
    return [(row['mechanic_id'], row['mechanic']) for _, row in mechanics_df.iterrows()]

def main():
    st.title("🎲 BGG + Finna Board Game Explorer")
    st.markdown("*Discover board games available in Finnish libraries with BoardGameGeek ratings and metadata*")
    
    # Get basic stats
    stats = query_db("""
        SELECT 
            COUNT(*) as total_games,
            SUM(CASE WHEN has_bgg_match THEN 1 ELSE 0 END) as games_with_bgg,
            ROUND(AVG(CASE WHEN bgg_average_rating IS NOT NULL THEN bgg_average_rating END), 2) as avg_rating
        FROM games
    """).iloc[0]
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Basic stats
    st.sidebar.metric("Total Games in Library", f"{stats['total_games']:,}")
    st.sidebar.metric("Games with BGG Data", f"{stats['games_with_bgg']:,}")
    st.sidebar.metric("BGG Match Rate", f"{stats['games_with_bgg']/stats['total_games']*100:.1f}%")
    if stats['avg_rating']:
        st.sidebar.metric("Average BGG Rating", f"{stats['avg_rating']:.1f}")
    
    # Filters
    min_rating = st.sidebar.slider("Minimum BGG Rating", 0.0, 10.0, 0.0, 0.1)
    max_players = st.sidebar.slider("Maximum Players", 1, 20, 20)
    max_playtime = st.sidebar.slider("Maximum Play Time (minutes)", 0, 300, 300, 15)
    min_age = st.sidebar.slider("Minimum Age", 0, 18, 0)
    
    # Category filter
    try:
        categories = get_categories_list()
        category_options = {name: cat_id for cat_id, name in categories}
        selected_category_names = st.sidebar.multiselect(
            "Categories", 
            list(category_options.keys()),
            help="Filter by BGG categories"
        )
        selected_categories = [category_options[name] for name in selected_category_names]
    except:
        selected_categories = []
    
    # Mechanics filter
    try:
        mechanics = get_mechanics_list()
        mechanic_options = {name: mech_id for mech_id, name in mechanics}
        selected_mechanic_names = st.sidebar.multiselect(
            "Mechanics", 
            list(mechanic_options.keys()),
            help="Filter by BGG mechanics"
        )
        selected_mechanics = [mechanic_options[name] for name in selected_mechanic_names]
    except:
        selected_mechanics = []
    
    # Build where clause for filters
    where_conditions = ["1=1"]  # Always true base condition
    
    if min_rating > 0:
        where_conditions.append(f"bgg_average_rating >= {min_rating}")
    
    if max_players < 20:
        where_conditions.append(f"(bgg_max_players IS NULL OR bgg_max_players <= {max_players})")
    
    if max_playtime < 300:
        where_conditions.append(f"(bgg_playing_time IS NULL OR bgg_playing_time <= {max_playtime})")
    
    if min_age > 0:
        where_conditions.append(f"(bgg_min_age IS NULL OR bgg_min_age >= {min_age})")
    
    if selected_categories:
        category_ids = ','.join([str(cat_id) for cat_id in selected_categories])
        where_conditions.append(f"id IN (SELECT game_id FROM game_categories WHERE category_id IN ({category_ids}))")
    
    if selected_mechanics:
        mechanic_ids = ','.join([str(mech_id) for mech_id in selected_mechanics])
        where_conditions.append(f"id IN (SELECT game_id FROM game_mechanics WHERE mechanic_id IN ({mechanic_ids}))")
    
    where_clause = " AND ".join(where_conditions)
    
    # Main content
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📊 Game Statistics")
        
        # Rating distribution
        rating_data = query_db(f"""
            SELECT 
                FLOOR(bgg_average_rating) as rating_floor,
                COUNT(*) as count
            FROM games 
            WHERE {where_clause} AND bgg_average_rating IS NOT NULL
            GROUP BY FLOOR(bgg_average_rating)
            ORDER BY rating_floor
        """)
        
        if not rating_data.empty:
            fig_rating = px.bar(
                rating_data, 
                x='rating_floor',
                y='count',
                title="BGG Rating Distribution",
                labels={'rating_floor': 'BGG Rating (Floor)', 'count': 'Number of Games'}
            )
            fig_rating.update_xaxes(dtick=1)
            st.plotly_chart(fig_rating, use_container_width=True)
        
        # Top categories
        try:
            category_data = query_db(f"""
                SELECT 
                    c.category,
                    COUNT(DISTINCT g.id) as count
                FROM games g
                JOIN game_categories gc ON g.bgg_id = gc.game_id
                JOIN categories c ON gc.category_id = c.category_id
                WHERE {where_clause.replace('games', 'g')}
                GROUP BY c.category
                ORDER BY count DESC
                LIMIT 10
            """)
            
            if not category_data.empty:
                fig_categories = px.bar(
                    category_data,
                    x='count',
                    y='category',
                    orientation='h',
                    title="Top 10 Categories",
                    labels={'count': 'Number of Games', 'category': 'Category'}
                )
                fig_categories.update_layout(height=400, yaxis={'categoryorder':'total ascending'})
                st.plotly_chart(fig_categories, use_container_width=True)
        except Exception as e:
            st.write(f"Category data not available: {e}")
    
    with col2:
        st.header("🎯 Quick Stats")
        
        # Top rated games
        top_rated = query_db(f"""
            SELECT 
                id,
                title, 
                bgg_id,
                bgg_primary_name, 
                bgg_average_rating, 
                bgg_rank,
                bgg_description,
                bgg_min_players,
                bgg_max_players,
                bgg_playing_time
            FROM games 
            WHERE {where_clause} AND bgg_average_rating IS NOT NULL
            ORDER BY bgg_average_rating DESC
            LIMIT 5
        """)
        
        if not top_rated.empty:
            st.subheader("🏆 Top Rated Games")
            for _, game in top_rated.iterrows():
                title = game['bgg_primary_name'] or game['title']
                if pd.notna(game['bgg_id']) and game['bgg_id'] != '':
                    bgg_link = f"[{title}](https://boardgamegeek.com/boardgame/{game['bgg_id']})"
                    display_title = f"⭐ {bgg_link} ({game['bgg_average_rating']:.1f})"
                else:
                    display_title = f"⭐ {title} ({game['bgg_average_rating']:.1f})"
                
                with st.expander(display_title):
                    if pd.notna(game['bgg_rank']):
                        st.write(f"**Rank:** #{int(game['bgg_rank'])}")
                    else:
                        st.write("**Rank:** Unranked")
                    
                    if pd.notna(game['bgg_min_players']):
                        st.write(f"**Players:** {int(game['bgg_min_players'])}-{int(game['bgg_max_players'])}")
                    
                    if pd.notna(game['bgg_playing_time']):
                        st.write(f"**Play Time:** {int(game['bgg_playing_time'])} min")
                    
                    if game['bgg_description'] and len(str(game['bgg_description'])) > 10:
                        desc = str(game['bgg_description'])[:200]
                        st.write(f"*{desc}...*")
        
        # Most owned games
        most_owned = query_db(f"""
            SELECT 
                id,
                title, 
                bgg_id,
                bgg_primary_name, 
                bgg_owned
            FROM games 
            WHERE {where_clause} AND bgg_owned IS NOT NULL
            ORDER BY bgg_owned DESC
            LIMIT 5
        """)
        
        if not most_owned.empty:
            st.subheader("👥 Most Owned Games")
            for _, game in most_owned.iterrows():
                title = game['bgg_primary_name'] or game['title']
                if pd.notna(game['bgg_id']) and game['bgg_id'] != '':
                    bgg_link = f"[{title}](https://boardgamegeek.com/boardgame/{game['bgg_id']})"
                    st.markdown(f"**{bgg_link}** - {int(game['bgg_owned']):,} owners")
                else:
                    st.write(f"**{title}** - {int(game['bgg_owned']):,} owners")
    
    # Game list
    st.header("🎮 Game Browser")
    
    # Sort options
    sort_options = {
        'Rating': 'bgg_average_rating DESC',
        'BGG Rank': 'bgg_rank ASC',
        'Number of Ratings': 'bgg_users_rated DESC',
        'Owned Count': 'bgg_owned DESC',
        'Title': 'title ASC'
    }
    
    sort_choice = st.selectbox("Sort by:", list(sort_options.keys()))
    sort_clause = sort_options[sort_choice]
    
    # Display filtered games
    games_list = query_db(f"""
        SELECT DISTINCT
            g.id,
            g.title,
            g.bgg_id,
            g.bgg_primary_name,
            g.bgg_average_rating,
            g.bgg_rank,
            g.bgg_min_players,
            g.bgg_max_players,
            g.bgg_playing_time,
            string_agg(DISTINCT c.category, '; ' ORDER BY c.category) as categories,
            string_agg(DISTINCT m.mechanic, '; ' ORDER BY m.mechanic) as mechanics
        FROM games g
        LEFT JOIN game_categories gc ON g.bgg_id = gc.game_id
        LEFT JOIN categories c ON gc.category_id = c.category_id
        LEFT JOIN game_mechanics gm ON g.bgg_id = gm.game_id
        LEFT JOIN mechanics m ON gm.mechanic_id = m.mechanic_id
        WHERE {where_clause}
        GROUP BY g.id, g.title, g.bgg_id, g.bgg_primary_name, g.bgg_average_rating, g.bgg_rank, 
                 g.bgg_min_players, g.bgg_max_players, g.bgg_playing_time
        ORDER BY {sort_clause}
        LIMIT 100
    """)
    
    # Create link columns
    def create_finna_link(row):
        if pd.notna(row['id']):
            return f"https://keski.finna.fi/Record/{row['id']}"
        return None
    
    def create_bgg_link(row):
        if pd.notna(row['bgg_id']) and row['bgg_id'] != '':
            return f"https://boardgamegeek.com/boardgame/{row['bgg_id']}"
        return None
    
    games_list['Finna Link'] = games_list.apply(create_finna_link, axis=1)
    games_list['BGG Link'] = games_list.apply(create_bgg_link, axis=1)
    
    # Clean up title columns
    games_list['Title'] = games_list['title']
    games_list['BGG Title'] = games_list['bgg_primary_name'].fillna('')
    
    # Drop helper columns and reorder
    display_cols = [
        'Title', 'BGG Title', 'bgg_average_rating', 'bgg_rank',
        'bgg_min_players', 'bgg_max_players', 'bgg_playing_time', 
        'categories', 'mechanics', 'Finna Link', 'BGG Link'
    ]
    games_list = games_list[display_cols]
    
    # Rename columns for display  
    games_list.columns = [
        'Title', 'BGG Title', 'Rating', 'Rank',
        'Min Players', 'Max Players', 'Play Time', 'Categories', 'Mechanics',
        'Finna Link', 'BGG Link'
    ]
    
    st.dataframe(
        games_list,
        use_container_width=True,
        height=600,
        column_config={
            "Finna Link": st.column_config.LinkColumn(
                "Finna",
                help="View game in Finna library catalog",
                width="small"
            ),
            "BGG Link": st.column_config.LinkColumn(
                "BGG",
                help="View game on BoardGameGeek",
                width="small"
            ),
            "Rating": st.column_config.NumberColumn(
                "Rating",
                format="%.1f",
                width="small"
            ),
            "Rank": st.column_config.NumberColumn(
                "Rank",
                format="%d",
                width="small"
            ),
            "Min Players": st.column_config.NumberColumn(
                "Min",
                format="%d",
                width="small"
            ),
            "Max Players": st.column_config.NumberColumn(
                "Max",
                format="%d",
                width="small"
            ),
            "Play Time": st.column_config.NumberColumn(
                "Time (min)",
                format="%d",
                width="small"
            )
        }
    )
    
    # Search functionality
    st.header("🔎 Search Games")
    search_term = st.text_input("Search by title:")
    
    if search_term:
        search_results = query_db(f"""
            SELECT DISTINCT
                g.id,
                g.title,
                g.bgg_id,
                g.bgg_primary_name,
                g.bgg_average_rating,
                g.bgg_rank,
                g.bgg_min_players,
                g.bgg_max_players,
                g.bgg_playing_time,
                string_agg(DISTINCT c.category, '; ' ORDER BY c.category) as bgg_categories,
                string_agg(DISTINCT m.mechanic, '; ' ORDER BY m.mechanic) as bgg_mechanics,
                g.bgg_description,
                g.publishers,
                g.summary,
                g.year,
                g.has_bgg_match
            FROM games g
            LEFT JOIN game_categories gc ON g.bgg_id = gc.game_id
            LEFT JOIN categories c ON gc.category_id = c.category_id
            LEFT JOIN game_mechanics gm ON g.bgg_id = gm.game_id
            LEFT JOIN mechanics m ON gm.mechanic_id = m.mechanic_id
            WHERE (g.title ILIKE '%{search_term}%' OR g.bgg_primary_name ILIKE '%{search_term}%')
            AND {where_clause.replace('games', 'g').replace(' id ', ' g.id ')}
            GROUP BY g.id, g.title, g.bgg_id, g.bgg_primary_name, g.bgg_average_rating, g.bgg_rank, 
                     g.bgg_min_players, g.bgg_max_players, g.bgg_playing_time, g.bgg_description,
                     g.publishers, g.summary, g.year, g.has_bgg_match
            LIMIT 20
        """)
        
        if not search_results.empty:
            st.write(f"Found {len(search_results)} games matching '{search_term}':")
            
            for _, game in search_results.iterrows():
                title = game['bgg_primary_name'] or game['title']
                finna_link = f"[{game['title']}](https://keski.finna.fi/Record/{game['id']})" if pd.notna(game['id']) else game['title']
                bgg_title = title if title != game['title'] else 'No BGG match'
                if pd.notna(game['bgg_id']) and game['bgg_id'] != '' and title != game['title']:
                    bgg_link = f"[{title}](https://boardgamegeek.com/boardgame/{game['bgg_id']})"
                    display_title = f"{finna_link} → {bgg_link}"
                else:
                    display_title = f"{finna_link} ({bgg_title})"
                
                with st.expander(display_title):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Library Info:**")
                        st.write(f"Title: {game['title']}")
                        if pd.notna(game['year']):
                            st.write(f"Year: {int(game['year'])}")
                        if game['publishers']:
                            st.write(f"Publisher: {game['publishers']}")
                        if game['summary']:
                            st.write(f"Description: {game['summary']}")
                    
                    with col2:
                        if game['has_bgg_match']:
                            st.write("**BGG Info:**")
                            if pd.notna(game['bgg_average_rating']):
                                st.write(f"Rating: ⭐ {game['bgg_average_rating']:.1f}")
                            if pd.notna(game['bgg_rank']):
                                st.write(f"Rank: #{int(game['bgg_rank'])}")
                            if pd.notna(game['bgg_min_players']):
                                st.write(f"Players: {int(game['bgg_min_players'])}-{int(game['bgg_max_players'])}")
                            if pd.notna(game['bgg_playing_time']):
                                st.write(f"Play Time: {int(game['bgg_playing_time'])} min")
                            if game['bgg_categories']:
                                st.write(f"Categories: {game['bgg_categories']}")
                            if game['bgg_mechanics']:
                                st.write(f"Mechanics: {game['bgg_mechanics']}")
                        else:
                            st.write("*No BGG data available*")
        else:
            st.write("No games found matching your search.")

if __name__ == "__main__":
    main()