"""
Dashboard page for the VALORANT betting platform
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def render_dashboard(api_client):
    """Render the main dashboard page"""
    
    st.title("🎯 VALORANT Betting Dashboard")
    
    # Key Metrics Row
    render_key_metrics(api_client)
    
    # Main Content Columns
    col1, col2 = st.columns([2, 1])
    
    with col1:
        render_upcoming_matches_section(api_client)
        render_recent_predictions_section(api_client)
        
    with col2:
        render_betting_recommendations_section(api_client)
        render_bankroll_section()
        
    # Bottom Row
    render_performance_section(api_client)

def render_key_metrics(api_client):
    """Render key performance metrics"""
    
    st.subheader("📊 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    try:
        # Get model performance
        model_perf = api_client.get_model_performance()
        
        # Get recent predictions
        recent_preds = api_client.get_recent_predictions(hours_back=24)
        
        # Get betting recommendations
        recommendations = api_client.get_betting_recommendations()
        
        with col1:
            st.metric(
                label="🎯 Today's Predictions",
                value=len(recent_preds),
                delta=f"+{len([p for p in recent_preds if p.get('confidence', 0) > 0.7])} high confidence"
            )
            
        with col2:
            profitable_bets = recommendations.get('profitable_bets', 0)
            total_recs = recommendations.get('total_recommendations', 0)
            st.metric(
                label="💰 Profitable Opportunities", 
                value=profitable_bets,
                delta=f"{total_recs} total recommendations"
            )
            
        with col3:
            bankroll = st.session_state.get('bankroll', 1000)
            # Calculate mock ROI
            mock_roi = 8.5  # This would come from actual betting history
            st.metric(
                label="💳 Current Bankroll",
                value=f"${bankroll:,.2f}",
                delta=f"{mock_roi:+.1f}% ROI"
            )
            
        with col4:
            models_loaded = model_perf.get('health_status', {}).get('models_loaded', 0)
            models_expected = model_perf.get('health_status', {}).get('models_expected', 5)
            st.metric(
                label="🤖 Models Active",
                value=f"{models_loaded}/{models_expected}",
                delta="All systems operational" if models_loaded == models_expected else "Some models offline"
            )
            
    except Exception as e:
        logger.error(f"Error loading key metrics: {e}")
        st.error("Unable to load key metrics. Please check API connection.")

def render_upcoming_matches_section(api_client):
    """Render upcoming matches section"""
    
    st.subheader("🗓️ Upcoming Matches")
    
    try:
        upcoming_matches = api_client.get_upcoming_matches(hours_ahead=48)
        matches = upcoming_matches.get('upcoming_matches', [])
        
        if not matches:
            st.info("No upcoming matches found in the next 48 hours.")
            return
            
        # Create DataFrame for display
        df = pd.DataFrame(matches)
        
        # Format columns
        df['Start Time'] = pd.to_datetime(df['start_time']).dt.strftime('%m/%d %H:%M')
        df['Match'] = df['team_a_name'] + ' vs ' + df['team_b_name']
        df['Tournament'] = df['tournament_name']
        df['Best Of'] = df['best_of']
        df['Hours Until'] = df['hours_until_start'].round(1)
        
        # Display table
        display_df = df[['Start Time', 'Match', 'Tournament', 'Best Of', 'Hours Until']].head(10)
        
        # Add selection for generating predictions
        selected_matches = st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="multi-row"
        )
        
        # Quick action buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔮 Generate Predictions", help="Generate predictions for selected matches"):
                if selected_matches.selection.rows:
                    generate_predictions_for_matches(api_client, df.iloc[selected_matches.selection.rows])
                else:
                    st.warning("Please select matches first")
                    
        with col2:
            if st.button("📊 View Odds", help="View current odds for selected matches"):
                if selected_matches.selection.rows:
                    view_odds_for_matches(api_client, df.iloc[selected_matches.selection.rows])
                else:
                    st.warning("Please select matches first")
                    
        with col3:
            if st.button("🔄 Refresh", help="Refresh upcoming matches"):
                st.rerun()
                
    except Exception as e:
        logger.error(f"Error loading upcoming matches: {e}")
        st.error("Unable to load upcoming matches.")

def render_recent_predictions_section(api_client):
    """Render recent predictions section"""
    
    st.subheader("🔮 Recent Predictions")
    
    try:
        # Get recent predictions
        predictions = api_client.get_recent_predictions(hours_back=24, min_confidence=0.5)
        
        if not predictions:
            st.info("No recent predictions found.")
            return
            
        # Create DataFrame
        df = pd.DataFrame(predictions)
        
        # Format columns
        df['Created'] = pd.to_datetime(df['created_at']).dt.strftime('%H:%M')
        df['Market'] = df['market_type'].str.replace('_', ' ').str.title()
        df['Probability'] = (df['probability'] * 100).round(1).astype(str) + '%'
        df['Confidence'] = (df['confidence'] * 100).round(1).astype(str) + '%'
        df['Fair Odds'] = df['fair_odds'].round(2)
        
        # Display recent predictions
        display_df = df[['Created', 'match_id', 'Market', 'Probability', 'Confidence', 'Fair Odds']].head(10)
        display_df.columns = ['Time', 'Match ID', 'Market', 'Probability', 'Confidence', 'Fair Odds']
        
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        # Show confidence distribution
        if len(df) > 0:
            confidence_dist = df['confidence'].value_counts(bins=5).sort_index()
            
            with st.expander("📈 Confidence Distribution"):
                fig = px.histogram(
                    df, 
                    x='confidence', 
                    nbins=10,
                    title="Prediction Confidence Distribution",
                    labels={'confidence': 'Confidence Score', 'count': 'Number of Predictions'}
                )
                st.plotly_chart(fig, use_container_width=True)
                
    except Exception as e:
        logger.error(f"Error loading recent predictions: {e}")
        st.error("Unable to load recent predictions.")

def render_betting_recommendations_section(api_client):
    """Render betting recommendations section"""
    
    st.subheader("💡 Betting Recommendations")
    
    try:
        recommendations = api_client.get_betting_recommendations(min_edge=0.02, max_recommendations=10)
        recs = recommendations.get('recommendations', [])
        
        if not recs:
            st.info("No profitable betting opportunities found.")
            return
            
        # Display top recommendations
        for i, rec in enumerate(recs[:5]):
            with st.expander(f"🎯 {rec['selection']} (+{rec['edge_percent']:.1f}% edge)"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Market:** {rec['market_type'].replace('_', ' ').title()}")
                    st.write(f"**Selection:** {rec['selection']}")
                    st.write(f"**Bookmaker:** {rec['bookmaker'].title()}")
                    
                with col2:
                    st.write(f"**Odds:** {rec['odds_decimal']:.2f}")
                    st.write(f"**Edge:** +{rec['edge_percent']:.1f}%")
                    st.write(f"**Kelly Stake:** {rec['kelly_stake']*100:.1f}%")
                    
                # Quick bet button
                if st.button(f"🎲 Quick Bet", key=f"quick_bet_{i}"):
                    add_to_bet_slip(rec)
                    
        # Summary stats
        total_edge = recommendations.get('total_edge', 0)
        profitable_bets = recommendations.get('profitable_bets', 0)
        
        st.info(f"💰 {profitable_bets} profitable opportunities with {total_edge:.1f}% total edge")
        
    except Exception as e:
        logger.error(f"Error loading betting recommendations: {e}")
        st.error("Unable to load betting recommendations.")

def render_bankroll_section():
    """Render bankroll management section"""
    
    st.subheader("💳 Bankroll Management")
    
    # Current bankroll
    bankroll = st.session_state.get('bankroll', 1000.0)
    st.metric("Current Bankroll", f"${bankroll:,.2f}")
    
    # Quick bankroll actions
    col1, col2 = st.columns(2)
    
    with col1:
        deposit = st.number_input("Deposit Amount", min_value=0.0, value=0.0, step=10.0)
        if st.button("💵 Deposit", disabled=deposit <= 0):
            st.session_state.bankroll = bankroll + deposit
            st.success(f"Deposited ${deposit:,.2f}")
            st.rerun()
            
    with col2:
        withdraw = st.number_input("Withdraw Amount", min_value=0.0, value=0.0, step=10.0)
        if st.button("💸 Withdraw", disabled=withdraw <= 0 or withdraw > bankroll):
            st.session_state.bankroll = bankroll - withdraw
            st.success(f"Withdrew ${withdraw:,.2f}")
            st.rerun()
            
    # Bankroll allocation
    with st.expander("📊 Recommended Allocation"):
        st.write("**Conservative Strategy:**")
        st.write(f"- Max bet per wager: ${bankroll * 0.02:,.2f} (2%)")
        st.write(f"- Max daily risk: ${bankroll * 0.10:,.2f} (10%)")
        st.write(f"- Emergency fund: ${bankroll * 0.20:,.2f} (20%)")

def render_performance_section(api_client):
    """Render performance analytics section"""
    
    st.subheader("📈 Performance Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Mock ROI chart
        dates = pd.date_range(start=datetime.now() - timedelta(days=30), end=datetime.now(), freq='D')
        roi_data = [1000]  # Starting bankroll
        
        # Generate mock ROI progression
        for i in range(1, len(dates)):
            change = np.random.normal(0.005, 0.02)  # Small daily changes
            roi_data.append(roi_data[-1] * (1 + change))
            
        roi_df = pd.DataFrame({'Date': dates, 'Bankroll': roi_data})
        
        fig = px.line(
            roi_df, 
            x='Date', 
            y='Bankroll',
            title="30-Day Bankroll Performance",
            labels={'Bankroll': 'Bankroll ($)'}
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    with col2:
        # Market performance by type
        market_performance = {
            'Match Winner': {'accuracy': 0.68, 'roi': 0.12},
            'Total Rounds': {'accuracy': 0.61, 'roi': 0.08},
            'Map Winner': {'accuracy': 0.65, 'roi': 0.15},
            'Player Props': {'accuracy': 0.58, 'roi': 0.05}
        }
        
        perf_df = pd.DataFrame(market_performance).T
        perf_df['Market'] = perf_df.index
        
        fig = px.scatter(
            perf_df,
            x='accuracy',
            y='roi',
            size=[100]*len(perf_df),
            text='Market',
            title="Market Performance: Accuracy vs ROI",
            labels={'accuracy': 'Accuracy Rate', 'roi': 'ROI'}
        )
        
        fig.update_traces(textposition="top center")
        st.plotly_chart(fig, use_container_width=True)

# Helper functions
def generate_predictions_for_matches(api_client, selected_matches):
    """Generate predictions for selected matches"""
    with st.spinner("Generating predictions..."):
        for _, match in selected_matches.iterrows():
            try:
                predictions = api_client.regenerate_predictions(match['match_id'])
                st.success(f"Generated predictions for {match['Match']}")
            except Exception as e:
                st.error(f"Failed to generate predictions for {match['Match']}: {e}")

def view_odds_for_matches(api_client, selected_matches):
    """View odds for selected matches"""
    st.info("Odds viewing feature coming soon!")

def add_to_bet_slip(recommendation):
    """Add recommendation to bet slip"""
    if 'bet_slip' not in st.session_state:
        st.session_state.bet_slip = []
        
    st.session_state.bet_slip.append(recommendation)
    st.success("Added to bet slip!")

# Import numpy for mock data generation
import numpy as np