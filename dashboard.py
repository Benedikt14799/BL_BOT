import streamlit as st
import pandas as pd
import asyncio
import os
import time
import aiohttp
import subprocess
from dotenv import load_dotenv, set_key

# Import your existing logic
import sys
sys.path.append(os.path.dirname(__file__))

from database import DatabaseManager
import ebay_analytics
import ebay_upload
import scrape

# --- 1. SETUP & DESIGN ---
st.set_page_config(
    page_title="BL_BOT Dashboard",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for a premium look
st.markdown("""
<style>
    /* Metric Cards */
    div[data-testid="stMetric"] {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 12px;
        border-top: 4px solid #0073e6;
        box-shadow: 0 4px 10px rgba(0,0,0,0.05);
        color: #1f1f1f !important;
    }
    
    /* Force high contrast for labels and values */
    [data-testid="stMetricLabel"] {
        color: #555 !important;
        font-size: 1rem !important;
        font-weight: 600 !important;
    }
    
    [data-testid="stMetricValue"] {
        color: #0073e6 !important;
        font-size: 1.8rem !important;
        font-weight: 800 !important;
    }
    
    /* Buttons */
    .stButton>button {
        border-radius: 8px;
        transition: transform 0.2s;
    }
    .stButton>button:hover {
        transform: translateY(-2px);
    }
    
    /* Headers */
    h1, h2, h3 {
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
</style>
""", unsafe_allow_html=True)

load_dotenv(".env")

# --- 2. AUTHENTICATION ---
def check_password():
    """Returns `True` if the user had a correct password."""
    def password_entered():
        if st.session_state["password"] == os.environ.get("DASHBOARD_PASSWORD", "admin123"):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.markdown("<h1 style='text-align: center; color: #0073e6;'>BL_BOT Login</h1>", unsafe_allow_html=True)
        st.text_input("Passwort", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.markdown("<h1 style='text-align: center; color: #0073e6;'>BL_BOT Login</h1>", unsafe_allow_html=True)
        st.text_input("Passwort", type="password", on_change=password_entered, key="password")
        st.error("😕 Falsches Passwort")
        return False
    else:
        return True

if not check_password():
    st.stop()  # Stop execution until logged in

# --- 3. HELPER FUNCTIONS ---
@st.cache_resource
def get_async_loop():
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop

def run_async(coro):
    loop = get_async_loop()
    return loop.run_until_complete(coro)

async def get_db_pool():
    if "db_pool" not in st.session_state:
        db_url = os.environ.get("DATABASE_URL")
        st.session_state.db_pool = await DatabaseManager.create_pool(db_url)
        # Tabellen initialisieren
        await DatabaseManager.create_table(st.session_state.db_pool)
    return st.session_state.db_pool

# --- 4. DATA FETCHING ---
async def fetch_dashboard_stats():
    pool = await get_db_pool()
    if not pool: return {}
    async with pool.acquire() as conn:
        total_books = await conn.fetchval("SELECT COUNT(*) FROM library")
        missing_books = await conn.fetchval("SELECT COUNT(*) FROM library WHERE status_id = 2")
        pending_books = await conn.fetchval("SELECT COUNT(*) FROM library WHERE status_id = 7")
        vacation_books = await conn.fetchval("SELECT COUNT(*) FROM library WHERE ebay_status = 'VACATION_PAUSED'")
    return {
        "total": total_books,
        "missing": missing_books,
        "pending": pending_books,
        "vacation": vacation_books
    }

async def fetch_rate_limits():
    async with aiohttp.ClientSession() as session:
        try:
            return await ebay_analytics.get_rate_limit_status(session)
        except Exception as e:
            return None

# --- 5. SIDEBAR NAVIGATION ---
st.sidebar.title("🤖 BL_BOT")
st.sidebar.markdown("---")
page = st.sidebar.radio("Navigation", ["📊 Dashboard", "📦 Upload Manager", "🔗 Link Manager", "⚙️ Einstellungen"])
st.sidebar.markdown("---")
if st.sidebar.button("Ausloggen"):
    st.session_state["password_correct"] = False
    st.rerun()

# --- 6. PAGE: DASHBOARD ---
if page == "📊 Dashboard":
    st.title("🚀 Scraper Dashboard")
    st.markdown("Hier hast du alle wichtigen Metriken und Schnellaktionen im Blick.")
    
    # KPIs
    stats = run_async(fetch_dashboard_stats())
    if stats:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Gesamt Inventar", f"{stats['total']} Bücher")
        col2.metric("Bereit für eBay", f"{stats['pending']} Bücher", delta_color="normal")
        col3.metric("Pausiert (Urlaub)", f"{stats['vacation']} Anbieter")
        col4.metric("Aussortiert (Missing)", f"{stats['missing']} Bücher")
    
    st.markdown("---")
    
    # Schnellaktionen
    st.subheader("⚡ Schnellaktionen")
    colA, colB, colC = st.columns(3)
    
    with colA:
        if st.button("🔍 Scraping Starten", use_container_width=True):
            st.info("Scraping gestartet! (Checke Logs im Hintergrund)")
            subprocess.Popen(["venv/bin/python", "run_scraping.sh"]) if os.name != 'nt' else subprocess.Popen(["python", "main.py"])
            
    with colB:
        if st.button("🏖️ Urlaubs-Reaktivierung", use_container_width=True):
            with st.spinner("Prüfe Urlaubsrückkehrer..."):
                pool = run_async(get_db_pool())
                from sync.booklooker.reactivate_vacation import reactivate_vacation
                res = run_async(reactivate_vacation(pool))
                st.success(f"Erledigt! {res['reactivated']} Bücher reaktiviert.")
                
    with colC:
        if st.button("🔄 Bestands- & Preis-Sync", use_container_width=True):
            st.info("Sync im Hintergrund gestartet.")
            subprocess.Popen(["venv/bin/python", "sync/ebay_inventory_check.py"]) if os.name != 'nt' else subprocess.Popen(["python", "sync/ebay_inventory_check.py"])

    st.markdown("---")
    
    # Rate Limits
    st.subheader("🛡️ eBay API Rate Limit")
    if st.button("Rate Limit abrufen"):
        with st.spinner("Frage eBay API ab..."):
            limits = run_async(fetch_rate_limits())
            if limits:
                col_L, col_R = st.columns(2)
                
                sell = limits.get("sell", {})
                with col_L:
                    st.markdown("#### Uploads (Sell API)")
                    st.write(f"**Limit gesamt:** {sell.get('limit', 0):,}")
                    st.write(f"**Verbraucht:** {sell.get('used', 0):,}")
                    st.write(f"**Verbleibend:** {sell.get('remaining', 0):,}")
                    st.write(f"**Reset um:** {sell.get('reset', 'Unbekannt')}")

                buy = limits.get("buy", {})
                with col_R:
                    st.markdown("#### Konkurrenzcheck (Buy API)")
                    st.write(f"**Limit gesamt:** {buy.get('limit', 0):,}")
                    st.write(f"**Verbraucht:** {buy.get('used', 0):,}")
                    st.write(f"**Verbleibend:** {buy.get('remaining', 0):,}")
                    st.write(f"**Reset um:** {buy.get('reset', 'Unbekannt')}")
            else:
                st.error("Konnte Rate Limit nicht abrufen. Bitte prüfe deine API Credentials.")



# --- 8. PAGE: UPLOAD MANAGER ---
elif page == "📦 Upload Manager":
    st.title("📦 Upload Manager")
    
    st.subheader("Batch Upload")
    if st.button("🚀 Alle bereiten Bücher zu eBay hochladen"):
        st.info("Upload-Prozess gestartet!")
        subprocess.Popen(["venv/bin/python", "ebay_upload.py"]) if os.name != 'nt' else subprocess.Popen(["python", "ebay_upload.py"])
        
    st.markdown("---")
    st.subheader("Einzel-Upload (Force)")
    force_id = st.text_input("Buch-ID (SKU) eingeben:")
    if st.button("Spezielles Buch hochladen"):
        if force_id:
            with st.spinner("Lade hoch..."):
                pool = run_async(get_db_pool())
                res = run_async(ebay_upload.run_upload_batch(pool, specific_ids=[int(force_id)] if force_id.isdigit() else None))
                st.write(res)
        else:
            st.warning("Bitte eine ID eingeben.")

# --- 9. PAGE: LINK MANAGER ---
elif page == "🔗 Link Manager":
    st.title("🔗 Link Manager (links.txt)")
    
    if os.path.exists("links.txt"):
        with open("links.txt", "r") as f:
            content = f.read()
    else:
        content = ""
        
    new_content = st.text_area("Bearbeite hier deine Booklooker-Suchlinks:", value=content, height=300)
    
    if st.button("💾 Speichern"):
        with open("links.txt", "w") as f:
            f.write(new_content)
        st.success("Erfolgreich gespeichert!")

# --- 10. PAGE: EINSTELLUNGEN ---
elif page == "⚙️ Einstellungen":
    st.title("⚙️ Konfiguration (.env)")
    
    # Felder aus .env lesen
    min_margin = os.environ.get("MINDESTMARGE", "2.50")
    fix_costs = os.environ.get("FIXKOSTEN_MONATLICH", "79.95")
    sales = os.environ.get("ERWARTETE_VERKAEUFE", "200")
    
    with st.form("config_form"):
        new_margin = st.text_input("Mindestmarge (€)", value=min_margin)
        new_fix = st.text_input("Fixkosten Monatlich (€)", value=fix_costs)
        new_sales = st.text_input("Erwartete Verkäufe pro Monat", value=sales)
        
        submitted = st.form_submit_button("💾 Speichern")
        if submitted:
            set_key(".env", "MINDESTMARGE", new_margin)
            set_key(".env", "FIXKOSTEN_MONATLICH", new_fix)
            set_key(".env", "ERWARTETE_VERKAEUFE", new_sales)
            load_dotenv(".env", override=True)
            st.success("Einstellungen wurden live aktualisiert!")

