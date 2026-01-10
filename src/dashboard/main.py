import streamlit as st
import time
from dashboard import config
from dashboard import services
from dashboard import ui_components

# 1. Setup Page
st.set_page_config(page_title=config.PAGE_TITLE, page_icon=config.PAGE_ICON, layout=config.LAYOUT)

# 2. Sidebar Configuration (The "Controller")
st.sidebar.header("📡 Data Feed")

# Fetch symbols
symbols = services.get_available_symbols()

if not symbols:
    st.warning("⏳ Waiting for data stream...")
    time.sleep(3)
    st.rerun()

# --- UX FIX: Session State ---
if 'selected_symbol' not in st.session_state:
    st.session_state.selected_symbol = symbols[0]

# The Selectbox writes directly to session state
selected_symbol = st.sidebar.selectbox(
    "Select Asset", 
    options=symbols,
    index=symbols.index(st.session_state.selected_symbol) if st.session_state.selected_symbol in symbols else 0,
    key='symbol_selector' # This automatically updates st.session_state.symbol_selector
)

# Update the main state variable if changed
if st.session_state.symbol_selector != st.session_state.selected_symbol:
    st.session_state.selected_symbol = st.session_state.symbol_selector
    st.rerun() # Immediate update on click

auto_refresh = st.sidebar.checkbox("Live Updates", value=True)

# 3. Main Content
st.title(f"{config.PAGE_ICON} {config.PAGE_TITLE}")

# Create a placeholder for the live content
dashboard_placeholder = st.empty()

# 4. Rendering Logic
def update_dashboard():
    with dashboard_placeholder.container():
        # Fetch Data
        df = services.get_market_data(st.session_state.selected_symbol)
        prediction = services.get_latest_prediction(st.session_state.selected_symbol)
        
        if df is not None:
            ui_components.render_kpis(df, prediction)
            st.divider()
            ui_components.render_advanced_chart(df, st.session_state.selected_symbol)
        else:
            st.info(f"Waiting for data for {st.session_state.selected_symbol}...")

# 5. Execution Loop
if auto_refresh:
    update_dashboard()
    time.sleep(config.REFRESH_SECONDS)
    st.rerun()
else:
    update_dashboard()