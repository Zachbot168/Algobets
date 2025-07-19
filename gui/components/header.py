"""
Header component for the application
"""

import streamlit as st
from datetime import datetime
import pytz

def render_header(api_connected=False):
    """Render application header"""
    
    # Main header
    col1, col2, col3 = st.columns([3, 2, 1])
    
    with col1:
        st.markdown("# 🎯 VALORANT Betting Platform")
        st.caption("AI-Powered Esports Betting Analytics")
        
    with col2:
        # Current time
        timezone = pytz.timezone('US/Eastern')
        current_time = datetime.now(timezone)
        st.markdown(f"**📅 {current_time.strftime('%B %d, %Y')}**")
        st.markdown(f"**🕐 {current_time.strftime('%I:%M %p ET')}**")
        
    with col3:
        # Status indicators
        if api_connected:
            st.success("🟢 LIVE")
        else:
            st.error("🔴 OFFLINE")
            
        # User info (mock)
        st.markdown("👤 **Demo User**")
        
    # Alert banner for important notifications
    render_alert_banner()
    
    st.markdown("---")

def render_alert_banner():
    """Render alert banner for important notifications"""
    
    # Check for important alerts
    alerts = get_current_alerts()
    
    if alerts:
        for alert in alerts:
            if alert['type'] == 'warning':
                st.warning(f"⚠️ {alert['message']}")
            elif alert['type'] == 'info':
                st.info(f"ℹ️ {alert['message']}")
            elif alert['type'] == 'success':
                st.success(f"✅ {alert['message']}")
            elif alert['type'] == 'error':
                st.error(f"❌ {alert['message']}")

def get_current_alerts():
    """Get current system alerts"""
    alerts = []
    
    # Check API connection
    if not st.session_state.get('api_connected', False):
        alerts.append({
            'type': 'error',
            'message': 'API connection failed. Some features may not work properly.'
        })
    
    # Check for maintenance mode
    if st.session_state.get('maintenance_mode', False):
        alerts.append({
            'type': 'warning', 
            'message': 'System maintenance in progress. Data may be temporarily unavailable.'
        })
        
    # Check for model updates
    if st.session_state.get('models_updating', False):
        alerts.append({
            'type': 'info',
            'message': 'ML models are being updated. New predictions will be available shortly.'
        })
        
    # Check for high-value opportunities
    high_value_bets = st.session_state.get('high_value_opportunities', 0)
    if high_value_bets > 0:
        alerts.append({
            'type': 'success',
            'message': f'{high_value_bets} high-value betting opportunities detected!'
        })
        
    return alerts