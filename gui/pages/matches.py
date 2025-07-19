"""
Matches page for viewing and analyzing upcoming/past matches
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def render_matches_page(api_client):
    """Render the matches page"""
    
    st.title("🗓️ Matches")
    
    # Filters section
    render_match_filters()
    
    # Main content tabs
    tab1, tab2, tab3 = st.tabs(["📅 Upcoming", "✅ Completed", "🔍 Match Details"])
    
    with tab1:
        render_upcoming_matches(api_client)
        
    with tab2:
        render_completed_matches(api_client)
        
    with tab3:
        render_match_details_tab(api_client)

def render_match_filters():
    """Render match filtering controls"""
    
    with st.expander("🔍 Filters", expanded=True):
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            tournament_filter = st.selectbox(
                "Tournament",
                options=["All", "VCT Americas", "VCT EMEA", "VCT Pacific", "Champions", "Masters"],
                key="tournament_filter"
            )
            
        with col2:
            region_filter = st.selectbox(
                "Region", 
                options=["All", "Americas", "EMEA", "Pacific", "China"],
                key="region_filter"
            )
            
        with col3:
            time_range = st.selectbox(
                "Time Range",
                options=["Next 24h", "Next 3 days", "Next Week", "Next Month"],
                key="time_range_filter"
            )
            
        with col4:
            best_of_filter = st.selectbox(
                "Best Of",
                options=["All", "BO1", "BO3", "BO5"],
                key="best_of_filter"
            )

def render_upcoming_matches(api_client):
    """Render upcoming matches section"""
    
    st.subheader("📅 Upcoming Matches")
    
    try:
        # Get time range from filter
        time_mapping = {
            "Next 24h": 24,
            "Next 3 days": 72,
            "Next Week": 168,
            "Next Month": 720
        }
        
        hours_ahead = time_mapping.get(st.session_state.get('time_range_filter', 'Next 24h'), 24)
        
        upcoming_matches = api_client.get_upcoming_matches(hours_ahead=hours_ahead)
        matches = upcoming_matches.get('upcoming_matches', [])
        
        if not matches:
            st.info(f"No upcoming matches found in the selected time range.")
            return
            
        # Apply filters
        matches = apply_match_filters(matches)
        
        if not matches:
            st.info("No matches found with current filters.")
            return
            
        # Display matches
        for i, match in enumerate(matches):
            render_match_card(api_client, match, i, upcoming=True)
            
    except Exception as e:
        logger.error(f"Error loading upcoming matches: {e}")
        st.error("Unable to load upcoming matches.")

def render_completed_matches(api_client):
    """Render completed matches section"""
    
    st.subheader("✅ Completed Matches")
    
    try:
        # Get completed matches
        matches_data = api_client.get_matches(status="completed", days_back=7, page_size=20)
        matches = matches_data.get('matches', [])
        
        if not matches:
            st.info("No completed matches found.")
            return
            
        # Apply filters
        matches = apply_match_filters(matches, completed=True)
        
        # Display matches
        for i, match in enumerate(matches):
            render_match_card(api_client, match, i, upcoming=False)
            
    except Exception as e:
        logger.error(f"Error loading completed matches: {e}")
        st.error("Unable to load completed matches.")

def render_match_details_tab(api_client):
    """Render match details tab"""
    
    st.subheader("🔍 Match Details")
    
    # Match ID input
    match_id = st.text_input("Enter Match ID to view details:")
    
    if match_id:
        try:
            # Get match details
            match_details = api_client.get_match_details(match_id)
            
            # Match info
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"**Tournament:** {match_details.get('tournament_name', 'N/A')}")
                st.markdown(f"**Teams:** {match_details['team_a']['team_name']} vs {match_details['team_b']['team_name']}")
                st.markdown(f"**Start Time:** {match_details.get('start_time', 'N/A')}")
                
            with col2:
                st.markdown(f"**Best Of:** {match_details.get('best_of', 'N/A')}")
                st.markdown(f"**Status:** {match_details.get('status', 'N/A')}")
                st.markdown(f"**LAN:** {'Yes' if match_details.get('is_lan') else 'No'}")
                
            # Teams and rosters
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown(f"### {match_details['team_a']['team_name']}")
                team_a_players = match_details.get('team_a_players', [])
                if team_a_players:
                    for player in team_a_players:
                        st.markdown(f"- {player['player_name']} ({player.get('role', 'N/A')})")
                else:
                    st.info("No roster information available")
                    
            with col2:
                st.markdown(f"### {match_details['team_b']['team_name']}")
                team_b_players = match_details.get('team_b_players', [])
                if team_b_players:
                    for player in team_b_players:
                        st.markdown(f"- {player['player_name']} ({player.get('role', 'N/A')})")
                else:
                    st.info("No roster information available")
                    
            # Maps
            if match_details.get('maps'):
                st.markdown("### 🗺️ Maps")
                maps_df = pd.DataFrame([{'Map': map_name} for map_name in match_details['maps']])
                st.dataframe(maps_df, use_container_width=True, hide_index=True)
                
            # Get match stats if available
            try:
                match_stats = api_client.get_match_stats(match_id)
                if match_stats and 'maps' in match_stats:
                    render_match_statistics(match_stats)
            except:
                pass  # Stats not available
                
        except Exception as e:
            st.error(f"Unable to load match details: {e}")

def render_match_card(api_client, match, index, upcoming=True):
    """Render individual match card"""
    
    with st.container():
        # Match header
        col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
        
        with col1:
            if upcoming:
                team_a = match.get('team_a_name', 'TBD')
                team_b = match.get('team_b_name', 'TBD')
            else:
                team_a = match['team_a']['team_name']
                team_b = match['team_b']['team_name']
                
            st.markdown(f"**{team_a} vs {team_b}**")
            
        with col2:
            tournament = match.get('tournament_name', 'Unknown Tournament')
            st.markdown(f"📍 {tournament}")
            
        with col3:
            if upcoming:
                hours_until = match.get('hours_until_start', 0)
                if hours_until < 1:
                    st.markdown("🔴 **Starting Soon**")
                elif hours_until < 24:
                    st.markdown(f"🕐 {hours_until:.1f}h")
                else:
                    st.markdown(f"📅 {hours_until/24:.1f}d")
            else:
                start_time = match.get('start_time', '')
                if start_time:
                    dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                    st.markdown(f"📅 {dt.strftime('%m/%d %H:%M')}")
                    
        with col4:
            best_of = match.get('best_of', 3)
            st.markdown(f"**BO{best_of}**")
            
        # Action buttons
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("🔮 Predictions", key=f"pred_{index}"):
                view_match_predictions(api_client, match)
                
        with col2:
            if st.button("📊 Odds", key=f"odds_{index}"):
                view_match_odds(api_client, match)
                
        with col3:
            if st.button("📈 Stats", key=f"stats_{index}"):
                view_match_stats(api_client, match)
                
        with col4:
            if st.button("🎲 Bet", key=f"bet_{index}"):
                add_to_bet_builder(match)
                
        st.markdown("---")

def render_match_statistics(match_stats):
    """Render detailed match statistics"""
    
    st.markdown("### 📊 Match Statistics")
    
    maps_data = match_stats.get('maps', [])
    
    if maps_data:
        # Maps overview
        df = pd.DataFrame(maps_data)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Display maps table
            display_df = df[['map_name', 'team_a_score', 'team_b_score', 'total_rounds']].copy()
            display_df.columns = ['Map', 'Team A', 'Team B', 'Total Rounds']
            st.dataframe(display_df, use_container_width=True, hide_index=True)
            
        with col2:
            # Rounds per map chart
            if len(df) > 1:
                fig = px.bar(
                    df,
                    x='map_name',
                    y='total_rounds',
                    title="Total Rounds per Map",
                    labels={'map_name': 'Map', 'total_rounds': 'Total Rounds'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
        # Player statistics if available
        players_data = match_stats.get('players', [])
        if players_data:
            st.markdown("#### 👥 Player Performance")
            
            players_df = pd.DataFrame(players_data)
            
            # Group by team
            team_a_players = players_df[players_df['team_id'] == match_stats.get('team_a_id', '')]
            team_b_players = players_df[players_df['team_id'] == match_stats.get('team_b_id', '')]
            
            col1, col2 = st.columns(2)
            
            with col1:
                if not team_a_players.empty:
                    st.markdown("**Team A**")
                    display_players_stats(team_a_players)
                    
            with col2:
                if not team_b_players.empty:
                    st.markdown("**Team B**")
                    display_players_stats(team_b_players)

def display_players_stats(players_df):
    """Display player statistics table"""
    
    if players_df.empty:
        return
        
    # Select relevant columns
    display_cols = ['player_name', 'agent', 'kills', 'deaths', 'assists', 'acs']
    available_cols = [col for col in display_cols if col in players_df.columns]
    
    if available_cols:
        display_df = players_df[available_cols].copy()
        display_df.columns = [col.replace('_', ' ').title() for col in display_df.columns]
        st.dataframe(display_df, use_container_width=True, hide_index=True)

def apply_match_filters(matches, completed=False):
    """Apply filters to matches list"""
    
    filtered_matches = matches.copy()
    
    # Tournament filter
    tournament_filter = st.session_state.get('tournament_filter', 'All')
    if tournament_filter != 'All':
        filtered_matches = [m for m in filtered_matches 
                          if tournament_filter.lower() in m.get('tournament_name', '').lower()]
    
    # Best of filter
    best_of_filter = st.session_state.get('best_of_filter', 'All')
    if best_of_filter != 'All':
        best_of_num = int(best_of_filter.replace('BO', ''))
        filtered_matches = [m for m in filtered_matches if m.get('best_of') == best_of_num]
        
    return filtered_matches

# Action functions
def view_match_predictions(api_client, match):
    """View predictions for a match"""
    match_id = match.get('match_id')
    if match_id:
        st.session_state.selected_match_for_predictions = match_id
        st.session_state.current_page = 'Predictions'
        st.rerun()

def view_match_odds(api_client, match):
    """View odds for a match"""
    st.info("Odds view will open in new section")

def view_match_stats(api_client, match):
    """View detailed match statistics"""
    st.info("Stats view will open in new section")

def add_to_bet_builder(match):
    """Add match to bet builder"""
    st.session_state.selected_match_for_betting = match
    st.session_state.current_page = 'Bet Builder'
    st.rerun()