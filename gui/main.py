"""
Main Streamlit application for VALORANT betting platform
"""

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from gui.pages import dashboard, matches, predictions, betting, analytics, settings
from gui.components import sidebar, header
from gui.utils.api_client import APIClient

# Configure page
st.set_page_config(
    page_title="VALORANT Betting Platform",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ValorantBettingApp:
    """Main Streamlit application class"""
    
    def __init__(self):
        self.api_client = APIClient()
        self.setup_session_state()
        
    def setup_session_state(self):
        """Initialize session state variables"""
        if 'current_page' not in st.session_state:
            st.session_state.current_page = 'Dashboard'
            
        if 'api_connected' not in st.session_state:
            st.session_state.api_connected = False
            
        if 'bankroll' not in st.session_state:
            st.session_state.bankroll = 1000.0  # Default bankroll
            
        if 'bet_history' not in st.session_state:
            st.session_state.bet_history = []
            
        if 'selected_matches' not in st.session_state:
            st.session_state.selected_matches = []
            
    def check_api_connection(self):
        """Check if API is connected and healthy"""
        try:
            health = self.api_client.get_health()
            st.session_state.api_connected = health.get('status') == 'healthy'
            return st.session_state.api_connected
        except Exception as e:
            logger.error(f"API connection failed: {e}")
            st.session_state.api_connected = False
            return False
            
    def render_header(self):
        """Render application header"""
        header.render_header(st.session_state.api_connected)
        
    def render_sidebar(self):
        """Render sidebar navigation"""
        pages = [
            'Dashboard',
            'Upcoming Matches', 
            'Predictions',
            'Bet Builder',
            'Analytics',
            'Settings'
        ]
        
        selected_page = sidebar.render_sidebar(pages, st.session_state.current_page)
        
        if selected_page != st.session_state.current_page:
            st.session_state.current_page = selected_page
            st.rerun()
            
    def render_main_content(self):
        """Render main content based on selected page"""
        page = st.session_state.current_page
        
        try:
            if page == 'Dashboard':
                dashboard.render_dashboard(self.api_client)
                
            elif page == 'Upcoming Matches':
                matches.render_matches_page(self.api_client)
                
            elif page == 'Predictions':
                predictions.render_predictions_page(self.api_client)
                
            elif page == 'Bet Builder':
                betting.render_betting_page(self.api_client)
                
            elif page == 'Analytics':
                analytics.render_analytics_page(self.api_client)
                
            elif page == 'Settings':
                settings.render_settings_page(self.api_client)
                
            else:
                st.error(f"Unknown page: {page}")
                
        except Exception as e:
            logger.error(f"Error rendering {page}: {e}")
            st.error(f"Error loading {page}: {str(e)}")
            
            # Show fallback content
            st.markdown("### Something went wrong")
            st.markdown("Please try refreshing the page or contact support.")
            
            with st.expander("Error Details"):
                st.exception(e)
                
    def render_footer(self):
        """Render application footer"""
        st.markdown("---")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.caption("🎯 VALORANT Betting Platform v0.1.0")
            
        with col2:
            if st.session_state.api_connected:
                st.caption("🟢 API Connected")
            else:
                st.caption("🔴 API Disconnected")
                
        with col3:
            st.caption(f"💰 Bankroll: ${st.session_state.bankroll:,.2f}")
            
    def run(self):
        """Main application runner"""
        # Check API connection
        api_connected = self.check_api_connection()
        
        # Render UI components
        self.render_header()
        self.render_sidebar()
        
        # Show connection warning if API is down
        if not api_connected:
            st.warning(
                "⚠️ API connection failed. Some features may not work properly. "
                "Please check the API service is running."
            )
            
        # Render main content
        self.render_main_content()
        
        # Render footer
        self.render_footer()

def main():
    """Main entry point"""
    app = ValorantBettingApp()
    app.run()

if __name__ == "__main__":
    main()