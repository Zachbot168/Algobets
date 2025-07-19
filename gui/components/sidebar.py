"""
Sidebar component for navigation
"""

import streamlit as st

def render_sidebar(pages, current_page):
    """Render sidebar with navigation"""
    
    with st.sidebar:
        # Logo and title
        st.image("https://via.placeholder.com/150x50/ff4b4b/ffffff?text=VALORANT", width=150)
        st.title("Betting Platform")
        
        # Navigation
        st.markdown("### 🧭 Navigation")
        
        # Page selection
        page_icons = {
            'Dashboard': '🏠',
            'Upcoming Matches': '🗓️', 
            'Predictions': '🔮',
            'Bet Builder': '🎲',
            'Analytics': '📊',
            'Settings': '⚙️'
        }
        
        selected_page = None
        for page in pages:
            icon = page_icons.get(page, '📄')
            
            if st.button(
                f"{icon} {page}",
                key=f"nav_{page}",
                use_container_width=True,
                type="primary" if page == current_page else "secondary"
            ):
                selected_page = page
                
        st.markdown("---")
        
        # Quick stats
        st.markdown("### 📊 Quick Stats")
        
        # Bankroll
        bankroll = st.session_state.get('bankroll', 1000.0)
        st.metric("💰 Bankroll", f"${bankroll:,.2f}")
        
        # Active bets (mock)
        active_bets = len(st.session_state.get('bet_slip', []))
        st.metric("🎯 Active Bets", active_bets)
        
        # Today's profit (mock)
        daily_profit = 45.20  # This would come from actual data
        st.metric("📈 Today's P&L", f"${daily_profit:+,.2f}", delta=f"{(daily_profit/bankroll)*100:+.1f}%")
        
        st.markdown("---")
        
        # Quick actions
        st.markdown("### ⚡ Quick Actions")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🔮 Generate Predictions", use_container_width=True):
                st.session_state.quick_action = 'generate_predictions'
                
        with col2:
            if st.button("💰 View Opportunities", use_container_width=True):
                st.session_state.quick_action = 'view_opportunities'
                
        if st.button("📊 Refresh Data", use_container_width=True):
            st.session_state.quick_action = 'refresh_data'
            
        st.markdown("---")
        
        # System status
        st.markdown("### 🔌 System Status")
        
        api_connected = st.session_state.get('api_connected', False)
        
        if api_connected:
            st.success("🟢 API Connected")
        else:
            st.error("🔴 API Disconnected")
            
        # Model status (mock)
        models_active = 4
        total_models = 5
        
        if models_active == total_models:
            st.success(f"🤖 All Models Active ({models_active}/{total_models})")
        else:
            st.warning(f"⚠️ Some Models Offline ({models_active}/{total_models})")
            
        # Data freshness (mock)
        last_update = "2 minutes ago"
        st.info(f"🔄 Last Update: {last_update}")
        
        st.markdown("---")
        
        # Footer
        st.markdown("### ℹ️ About")
        st.caption("VALORANT Betting Platform v0.1.0")
        st.caption("Powered by ML & Analytics")
        
        if st.button("📚 Documentation", use_container_width=True):
            st.session_state.show_docs = True
            
        if st.button("🐛 Report Issue", use_container_width=True):
            st.session_state.show_issue_form = True
            
    return selected_page