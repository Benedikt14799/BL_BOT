import tkinter as tk
from tkinter import ttk, messagebox
import ttkbootstrap as tb
from ttkbootstrap.constants import *
from ttkbootstrap.widgets import ToolTip
from dotenv import load_dotenv, set_key
import os
import threading
import sys
import logging
import asyncio
import asyncpg
import time
import aiohttp
from decimal import Decimal
from bs4 import BeautifulSoup
from database import DatabaseManager
import ebay_upload
import scrape
# import price_monitor  <- Entfernt, da durch sync_service ersetzt
import price_processing
import ebay_analytics
from sync.booklooker import reactivate_vacation, ebay as sync_ebay

# --- Redirect logging to GUI ---
class TextHandler(logging.Handler):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        def append():
            self.text_widget.configure(state='normal')
            self.text_widget.insert(tk.END, msg + '\n')
            self.text_widget.configure(state='disabled')
            self.text_widget.yview(tk.END)
        self.text_widget.after(0, append)

class BLBotApp(tb.Window):
    def __init__(self):
        super().__init__(themename="darkly", title="BL_BOT Control Panel", size=(900, 650))
        self.env_path = ".env"
        self.links_path = "links.txt"
        
        # UI Setup
        # Custom Navigation Bar
        self.nav_frame = tb.Frame(self, padding=(10, 10, 10, 0))
        self.nav_frame.pack(fill=X)
        
        # Content Area
        self.container = tb.Frame(self, padding=10)
        self.container.pack(fill=BOTH, expand=True)
        
        # Define Tab Content Frames
        self.tab_dashboard = tb.Frame(self.container)
        self.tab_upload = tb.Frame(self.container)
        self.tab_links = tb.Frame(self.container)
        self.tab_settings = tb.Frame(self.container)
        self.tabs = [self.tab_dashboard, self.tab_upload, self.tab_links, self.tab_settings]
        
        # Nav Buttons with physical Space (padx)
        self.nav_btns = []
        btn_data = [
            ("🚀 Scraper Dashboard", self.tab_dashboard),
            ("📦 Upload Manager", self.tab_upload),
            ("🔗 Links", self.tab_links),
            ("⚙️ Settings", self.tab_settings)
        ]
        
        for i, (text, frame) in enumerate(btn_data):
            btn = tb.Button(
                self.nav_frame, 
                text=text, 
                bootstyle=(SECONDARY, OUTLINE), 
                command=lambda f=frame, idx=i: self._switch_tab(idx)
            )
            btn.pack(side=LEFT, padx=8, pady=5)
            self.nav_btns.append(btn)
        
        # Style tweak: Custom styles are no longer needed for Notebook
        
        self._build_dashboard()
        self._build_upload_manager()
        self._build_links_tab()
        self._build_settings_tab()
        
        # Default tab
        self._switch_tab(0)
        
        # Load initials
        self._load_settings()
        self._load_links()

        # Custom Logger format — nur EINMAL einrichten
        self.log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%H:%M:%S')
        self.handler = TextHandler(self.log_text)
        self.handler.setFormatter(self.log_format)
        logging.getLogger().addHandler(self.handler)
        logging.getLogger().setLevel(logging.INFO)

        # Bot State — nur EINMAL initialisieren
        self.db_pool = None
        self.scrape_task = None
        self.auto_sync_active = False
        self.sync_loop_task = None
        
        # Async Event Loop — nur EINMAL starten
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self._run_async_loop, daemon=True).start()
        
        # Handle clean exit
        self.protocol("WM_DELETE_WINDOW", self.on_closing)

    def _switch_tab(self, index):
        """Switches the visible tab content frame."""
        for i, frame in enumerate(self.tabs):
            if i == index:
                frame.pack(fill=BOTH, expand=True)
                self.nav_btns[i].configure(bootstyle=PRIMARY)
            else:
                frame.pack_forget()
                self.nav_btns[i].configure(bootstyle=(SECONDARY, OUTLINE))

    def _run_async_loop(self):
        asyncio.set_event_loop(self.loop)
        # Create lock inside the running loop
        self._db_init_lock = asyncio.Lock()
        self.loop.run_forever()

    async def _get_db_pool(self):
        """Asynchronous way to get or create the DB pool."""
        if not self.db_pool:
            db_url = os.environ.get("DATABASE_URL")
            if not db_url:
                logging.error("DATABASE_URL missing in .env")
                return None
            
            async with self._db_init_lock: 
                if self.db_pool: return self.db_pool
                
                try:
                    logging.info("Initializing DB Pool (limited size)...")
                    # Limit pool size to stay within Supabase free tier limits
                    self.db_pool = await asyncio.wait_for(
                        asyncpg.create_pool(
                            dsn=db_url, 
                            ssl="require",
                            min_size=1,
                            max_size=3,
                            command_timeout=60
                        ),
                        timeout=30.0
                    )
                    logging.info("Pool created, verifying tables...")
                    await DatabaseManager.create_table(self.db_pool)
                    logging.info("DB Connection established and tables verified.")
                except Exception as e:
                    logging.error(f"Failed to connect to DB: {str(e)}")
                    return None
        return self.db_pool

    def on_closing(self):
        """Cleanup before closing the window."""
        logging.info("Closing application and cleaning up connections...")
        if self.db_pool:
            # Schedule pool closing in the loop
            self.loop.call_soon_threadsafe(lambda: asyncio.create_task(self.db_pool.close()))
        
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.destroy()
        sys.exit(0)

    def _build_dashboard(self):
        # Controls
        controls = tb.Frame(self.tab_dashboard, padding=10)
        controls.pack(fill=X)
        
        self.btn_start = tb.Button(controls, text="Scraping Starten", bootstyle=SUCCESS, command=self.start_scraping)
        self.btn_start.pack(side=LEFT, padx=10)

        self.btn_vacation = tb.Button(controls, text="🏖️ Urlaubs-Reaktivierung", bootstyle=INFO, command=self.start_vacation_reactivation)
        self.btn_vacation.pack(side=LEFT, padx=10)
        ToolTip(self.btn_vacation, text="Prüft pausierte Bücher und stellt sie wieder ein, wenn der Anbieter zurück ist.", bootstyle=INFO, delay=100)

        self.btn_sync = tb.Button(controls, text="🔄 Bestands- & Preis-Sync", bootstyle=PRIMARY, command=self.start_price_sync)
        self.btn_sync.pack(side=LEFT, padx=10)
        ToolTip(self.btn_sync, text="Gleicht alle eBay-Angebote mit BookLooker ab (Preise, Verkäufe, Urlaub).", bootstyle=INFO, delay=100)



        # Rate Limit Section
        rl_frame = tb.Labelframe(self.tab_dashboard, text="eBay API Rate Limit", padding=15)
        rl_frame.pack(fill=X, padx=20, pady=10)

        rl_controls = tb.Frame(rl_frame)
        rl_controls.pack(fill=X)

        self.btn_rate_limit = tb.Button(rl_controls, text="🔄 Rate Limit abrufen", bootstyle=SECONDARY, command=self.refresh_rate_limit)
        self.btn_rate_limit.pack(side=LEFT, padx=5)

        self.lbl_rl_status = tb.Label(rl_controls, text="Status: Bereit", font=("Helvetica", 9, "italic"))
        self.lbl_rl_status.pack(side=LEFT, padx=20)

        rl_data = tb.Frame(rl_frame)
        rl_data.pack(fill=X, pady=(10, 0))

        # Grid for values - Uploads (Sell API)
        tb.Label(rl_data, text="Uploads (Sell API):", font=("Helvetica", 10, "bold")).grid(row=0, column=0, columnspan=2, sticky=W, padx=5, pady=(0,5))
        
        tb.Label(rl_data, text="Limit gesamt:", font=("Helvetica", 10)).grid(row=1, column=0, sticky=W, padx=5)
        self.lbl_sell_limit_total = tb.Label(rl_data, text="---", font=("Helvetica", 10, "bold"))
        self.lbl_sell_limit_total.grid(row=1, column=1, sticky=W, padx=10)

        tb.Label(rl_data, text="Verbraucht:", font=("Helvetica", 10)).grid(row=1, column=2, sticky=W, padx=20)
        self.lbl_sell_limit_used = tb.Label(rl_data, text="---", font=("Helvetica", 10, "bold"))
        self.lbl_sell_limit_used.grid(row=1, column=3, sticky=W, padx=10)

        tb.Label(rl_data, text="Verbleibend:", font=("Helvetica", 10)).grid(row=2, column=0, sticky=W, padx=5)
        self.lbl_sell_limit_remaining = tb.Label(rl_data, text="---", font=("Helvetica", 10, "bold"))
        self.lbl_sell_limit_remaining.grid(row=2, column=1, sticky=W, padx=10)

        tb.Label(rl_data, text="Reset um:", font=("Helvetica", 10)).grid(row=2, column=2, sticky=W, padx=20)
        self.lbl_sell_limit_reset = tb.Label(rl_data, text="---", font=("Helvetica", 10, "bold"))
        self.lbl_sell_limit_reset.grid(row=2, column=3, sticky=W, padx=10)

        # Grid for values - Konkurrenzcheck (Buy API)
        tb.Label(rl_data, text="Konkurrenzcheck (Buy API):", font=("Helvetica", 10, "bold")).grid(row=3, column=0, columnspan=2, sticky=W, padx=5, pady=(15,5))
        
        tb.Label(rl_data, text="Limit gesamt:", font=("Helvetica", 10)).grid(row=4, column=0, sticky=W, padx=5)
        self.lbl_buy_limit_total = tb.Label(rl_data, text="---", font=("Helvetica", 10, "bold"))
        self.lbl_buy_limit_total.grid(row=4, column=1, sticky=W, padx=10)

        tb.Label(rl_data, text="Verbraucht:", font=("Helvetica", 10)).grid(row=4, column=2, sticky=W, padx=20)
        self.lbl_buy_limit_used = tb.Label(rl_data, text="---", font=("Helvetica", 10, "bold"))
        self.lbl_buy_limit_used.grid(row=4, column=3, sticky=W, padx=10)

        tb.Label(rl_data, text="Verbleibend:", font=("Helvetica", 10)).grid(row=5, column=0, sticky=W, padx=5)
        self.lbl_buy_limit_remaining = tb.Label(rl_data, text="---", font=("Helvetica", 10, "bold"))
        self.lbl_buy_limit_remaining.grid(row=5, column=1, sticky=W, padx=10)

        tb.Label(rl_data, text="Reset um:", font=("Helvetica", 10)).grid(row=5, column=2, sticky=W, padx=20)
        self.lbl_buy_limit_reset = tb.Label(rl_data, text="---", font=("Helvetica", 10, "bold"))
        self.lbl_buy_limit_reset.grid(row=5, column=3, sticky=W, padx=10)

        # Log Window
        lbl = tb.Label(self.tab_dashboard, text="Live Logs:", font=("Helvetica", 12, "bold"))
        lbl.pack(anchor=W, padx=20)
        
        container = tb.Frame(self.tab_dashboard, padding=10)
        container.pack(fill=BOTH, expand=True)
        
        self.log_text = tb.Text(container, state='disabled', wrap='word', font=("Consolas", 10))
        self.log_text.pack(fill=BOTH, expand=True)
        
        # Scrollbar for logs
        sb = tb.Scrollbar(self.log_text, orient=VERTICAL, command=self.log_text.yview)
        sb.pack(side=RIGHT, fill=Y)
        self.log_text.configure(yscrollcommand=sb.set)
        
    def _build_upload_manager(self):
        controls = tb.Frame(self.tab_upload, padding=10)
        controls.pack(fill=X)
        
        self.btn_refresh = tb.Button(controls, text="↻ Liste Aktualisieren", bootstyle=INFO, command=self.refresh_upload_table)
        self.btn_refresh.pack(side=LEFT, padx=5)
        
        tb.Separator(controls, orient=VERTICAL).pack(side=LEFT, fill=Y, padx=10)
        
        self.btn_select_all = tb.Button(controls, text="Alle Auswählen", bootstyle=(SECONDARY, OUTLINE), command=self.select_all)
        self.btn_select_all.pack(side=LEFT, padx=5)
        
        self.btn_deselect_all = tb.Button(controls, text="Auswahl Aufheben", bootstyle=(SECONDARY, OUTLINE), command=self.deselect_all)
        self.btn_deselect_all.pack(side=LEFT, padx=5)
        
        tb.Separator(controls, orient=VERTICAL).pack(side=LEFT, fill=Y, padx=10)

        self.btn_upload = tb.Button(controls, text="Ausgewählte Hochladen", bootstyle=PRIMARY, command=self.upload_selected)
        self.btn_upload.pack(side=LEFT, padx=5)

        self.btn_delete = tb.Button(controls, text="🗑️ Ausgewählte Löschen", bootstyle=DANGER, command=self.delete_selected)
        self.btn_delete.pack(side=LEFT, padx=5)
        
        columns = ("id", "title", "author", "price", "isbn")
        self.tree = tb.Treeview(self.tab_upload, columns=columns, show="headings", bootstyle=INFO, selectmode='extended')
        self.tree.heading("id", text="ID")
        self.tree.column("id", width=50, stretch=False)
        self.tree.heading("title", text="Titel")
        self.tree.column("title", width=350)
        self.tree.heading("author", text="Autor")
        self.tree.heading("price", text="Preis (€)")
        self.tree.column("price", width=100, anchor=E)
        self.tree.heading("isbn", text="ISBN")
        
        self.tree.pack(fill=BOTH, expand=True, padx=10, pady=5)
        
    def _build_links_tab(self):
        # Top: New Links Input
        new_links_frame = tb.Labelframe(self.tab_links, text="Neue Booklooker-URLs hinzufügen", padding=15)
        new_links_frame.pack(fill=X, padx=10, pady=10)
        
        lbl_hint = tb.Label(new_links_frame, text="Eine URL pro Zeile. Suffix wird automatisch ergänzt (siehe Settings).", font=("Helvetica", 8, "italic"))
        lbl_hint.pack(anchor=W, pady=(0, 5))

        self.links_text = tb.Text(new_links_frame, wrap='none', height=8)
        self.links_text.pack(fill=X, expand=True)
        
        btn_add = tb.Button(new_links_frame, text="➕ Links zur Warteschlange hinzufügen", bootstyle=SUCCESS, command=self.add_new_links)
        btn_add.pack(pady=10)
        
        # Bottom: History (Collapsible)
        self.history_visible = tk.BooleanVar(value=False)
        history_frame = tb.Frame(self.tab_links, padding=10)
        history_frame.pack(fill=BOTH, expand=True)
        
        # Collapse Header
        header_frame = tb.Frame(history_frame)
        header_frame.pack(fill=X)
        
        self.btn_toggle_history = tb.Button(header_frame, text="▶ Link-Verlauf anzeigen (Datenbank)", bootstyle=(SECONDARY, OUTLINE), command=self.toggle_history)
        self.btn_toggle_history.pack(side=LEFT)
        
        self.btn_refresh_history = tb.Button(header_frame, text="🔄", bootstyle=INFO, command=self.refresh_history_table)
        # Initially hidden until history is toggled
        
        self.history_container = tb.Frame(history_frame, padding=(0, 10))
        # Container will be packed/unpacked by toggle_history
        
        # Treeview for sitetoscrape
        columns = ("id", "link", "pages", "books", "status")
        self.tree_history = tb.Treeview(self.history_container, columns=columns, show="headings", bootstyle=INFO, height=10)
        self.tree_history.heading("id", text="ID")
        self.tree_history.column("id", width=50, stretch=False)
        self.tree_history.heading("link", text="Basis-Link")
        self.tree_history.column("link", width=400)
        self.tree_history.heading("pages", text="Seiten")
        self.tree_history.column("pages", width=60, anchor=CENTER)
        self.tree_history.heading("books", text="Bücher")
        self.tree_history.column("books", width=80, anchor=CENTER)
        self.tree_history.heading("status", text="Status")
        self.tree_history.column("status", width=100, anchor=CENTER)
        
        self.tree_history.pack(fill=BOTH, expand=True)
        
        # Scrollbar for history
        sb_h = tb.Scrollbar(self.tree_history, orient=VERTICAL, command=self.tree_history.yview)
        sb_h.pack(side=RIGHT, fill=Y)
        self.tree_history.configure(yscrollcommand=sb_h.set)
        
    def _build_settings_tab(self):
        self.settings_vars = {
            "DATABASE_URL": tk.StringVar(),
            "EBAY_APP_ID": tk.StringVar(),
            "EBAY_DEV_ID": tk.StringVar(),
            "EBAY_CERT_ID": tk.StringVar(),
            "EBAY_CLIENT_ID": tk.StringVar(),
            "EBAY_CLIENT_SECRET": tk.StringVar(),
            "EBAY_REFRESH_TOKEN": tk.StringVar(),
            "EBAY_ENV": tk.StringVar(value="SANDBOX"),
            "FIXKOSTEN_MONATLICH": tk.StringVar(value="79.95"),
            "ERWARTETE_VERKAEUFE": tk.StringVar(value="200"),
            "MINDESTMARGE": tk.StringVar(value="2.50"),
            "STEUERSATZ": tk.StringVar(value="7.0"),
            "ZUSATZKOSTEN_LOW_MID": tk.StringVar(value="0.50"),
            "ZUSATZKOSTEN_HIGH": tk.StringVar(value="1.75"),
            "SHIPPING_DESCRIPTION_EBAY": tk.StringVar(value="Standardversand"),
            "DELIVERY_TIME_EBAY": tk.StringVar(value="1-3 Werktage"),
            "BL_URL_SUFFIX": tk.StringVar()
        }
        
        container = tb.Frame(self.tab_settings, padding=20)
        container.pack(fill=BOTH, expand=True)
        
        # Left Column: API & Fixkosten
        left_frame = tb.Frame(container)
        left_frame.pack(side=LEFT, fill=BOTH, expand=True, padx=5)
        
        # Right Column: Margen & eBay-Einstellungen
        right_frame = tb.Frame(container)
        right_frame.pack(side=RIGHT, fill=BOTH, expand=True, padx=5)

        # === LEFT_FRAME ===
        self._add_setting_row(left_frame, "DATABASE_URL:", "DATABASE_URL", row=0, is_secret=True, is_required=True, tooltip_text="Verbindungsstring zur Supabase-Datenbank (PostgreSQL).")
        self._add_setting_row(left_frame, "EBAY_APP_ID:", "EBAY_APP_ID", row=1, is_secret=True, is_required=True, tooltip_text="Deine eBay Developer App-ID (Client ID).")
        self._add_setting_row(left_frame, "EBAY_DEV_ID:", "EBAY_DEV_ID", row=2, is_secret=True, is_required=True, tooltip_text="Deine eBay Developer-ID.")
        self._add_setting_row(left_frame, "EBAY_CERT_ID:", "EBAY_CERT_ID", row=3, is_secret=True, is_required=True, tooltip_text="Deine eBay Developer Cert-ID (Client Secret).")
        self._add_setting_row(left_frame, "EBAY_CLIENT_ID:", "EBAY_CLIENT_ID", row=4, is_secret=True, is_required=True, tooltip_text="Deine eBay OAuth Client-ID (identisch mit App-ID, wird für den Token-Refresh benötigt).")
        self._add_setting_row(left_frame, "EBAY_CLIENT_SECRET:", "EBAY_CLIENT_SECRET", row=5, is_secret=True, is_required=True, tooltip_text="Dein eBay OAuth Client Secret (identisch mit Cert-ID, wird für den Token-Refresh benötigt).")
        self._add_setting_row(left_frame, "EBAY_REFRESH_TOKEN:", "EBAY_REFRESH_TOKEN", row=6, is_secret=True, is_required=True, tooltip_text="Einmalig generierter Refresh Token (18 Monate gültig). Der Access Token wird automatisch erneuert.")
        self._add_setting_row(left_frame, "EBAY_ENV:", "EBAY_ENV", row=7, is_combobox=True, tooltip_text="SANDBOX für Tests, PRODUCTION für echte eBay-Aufschaltungen.")
        self._add_setting_row(left_frame, "Fixkosten monatlich (€):", "FIXKOSTEN_MONATLICH", row=8, is_required=True, tooltip_text="Gesamte monatliche Kosten des eBay-Shops (z.B. 79.95), die anteilig auf Verkäufe umgelegt werden.")
        self._add_setting_row(left_frame, "Erwartete Verkäufe:", "ERWARTETE_VERKAEUFE", row=9, is_required=True, tooltip_text="Wie viele Artikel du ca. im Monat verkaufst (zur Umlage der Fixkosten).")

        # Buttons for left frame
        btn_save = tb.Button(left_frame, text="Einstellungen Speichern", bootstyle=SUCCESS, command=self.save_settings)
        btn_save.grid(row=10, column=0, pady=20, padx=5)

        btn_test = tb.Button(left_frame, text="Verbindung Testen", bootstyle=INFO, command=self.test_connection)
        btn_test.grid(row=10, column=1, pady=20, padx=5)

        self.lbl_fixkosten_hint = tb.Label(left_frame, text="= 0.400€ pro Verkauf", font=("Helvetica", 8, "italic"))
        self.lbl_fixkosten_hint.grid(row=11, column=1, sticky=W, pady=(0, 10))
        
        # === RIGHT_FRAME ===
        self._add_setting_row(right_frame, "Steuersatz MwSt (%):", "STEUERSATZ", row=0, is_required=True, tooltip_text="Dein Umsatzsteuersatz für Bücher. Es wird intern mit 7% Vorsteuer auf den Einkauf gerechnet. Kleinunternehmer = 0.")
        self._add_setting_row(right_frame, "Mindestmarge netto (€):", "MINDESTMARGE", row=1, is_required=True, tooltip_text="Absoluter Mindestgewinn nach Abzug ALLER Gebühren, Steuern, Portos und Verpackung, der am Ende übrig bleiben muss.")
        self._add_setting_row(right_frame, "Zusatzkosten (Buch <30€):", "ZUSATZKOSTEN_LOW_MID", row=2, tooltip_text="Pauschalbetrag für Verpackung/Polsterumschläge bei günstigen und mittleren Büchern.")
        self._add_setting_row(right_frame, "Zusatzkosten (Buch >30€):", "ZUSATZKOSTEN_HIGH", row=3, tooltip_text="Pauschalbetrag für hochwertigere Pakete/Polster für wertvolle Sammlerstücke / Lexikons.")
        self._add_setting_row(right_frame, "eBay Versandinfo:", "SHIPPING_DESCRIPTION_EBAY", row=4, tooltip_text="Standard Versandprofil-Text für eBay (z.B. 'Standardversand' oder 'Büchersendung').")
        self._add_setting_row(right_frame, "eBay Lieferzeit:", "DELIVERY_TIME_EBAY", row=5, tooltip_text="Information zur Lieferzeit, die bei eBay als Textbaustein mitübergeben werden soll.")
        self._add_setting_row(right_frame, "BL Link Suffix:", "BL_URL_SUFFIX", row=6, tooltip_text="Anhängsel für Booklooker Links (z.B. &searchUserTyp=2&hasPic=on...), wird automatisch an jeden Link im Scraper angehängt.")
        
        lbl_required = tb.Label(right_frame, text="* Pflichtfelder", font=("Helvetica", 8), bootstyle="danger")
        lbl_required.grid(row=6, column=1, sticky=E, pady=(10, 0))

        # Configure column weights for frames
        left_frame.columnconfigure(1, weight=1)
        right_frame.columnconfigure(1, weight=1)

        self._update_fixkosten_hint()

        # Update hint when values change
        self.settings_vars["FIXKOSTEN_MONATLICH"].trace_add("write", lambda *a: self._update_fixkosten_hint())
        self.settings_vars["ERWARTETE_VERKAEUFE"].trace_add("write", lambda *a: self._update_fixkosten_hint())

    def _add_setting_row(self, parent_frame, label_text, var_key, row, is_secret=False, is_combobox=False, tooltip_text="", is_required=False):
        lbl_frame = tb.Frame(parent_frame)
        lbl_frame.grid(row=row, column=0, pady=10, sticky=W)
        
        label_full = label_text
        if is_required:
            label_full += " *"
            
        lbl = tb.Label(lbl_frame, text=label_full)
        lbl.pack(side=LEFT)
        
        if is_required:
            lbl_star = tb.Label(lbl_frame, text="", bootstyle="danger") # We could put the star in a separate label for color
            # but appending it to the main label is cleaner for layout. 
            # Let's just append it to the text in the main label above.
            pass
        if tooltip_text:
            info_lbl = tb.Label(lbl_frame, text=" ℹ️", font=("Helvetica", 9), cursor="hand2")
            info_lbl.pack(side=LEFT, padx=(0, 5))
            ToolTip(info_lbl, text=tooltip_text, bootstyle=INFO, delay=100)
            
        if is_combobox:
            cb = tb.Combobox(parent_frame, textvariable=self.settings_vars[var_key], values=["SANDBOX", "PRODUCTION"])
            cb.grid(row=row, column=1, sticky=EW, padx=10)
        else:
            show_char = "*" if is_secret else ""
            tb.Entry(parent_frame, textvariable=self.settings_vars[var_key], show=show_char).grid(row=row, column=1, sticky=EW, padx=10)

    # --- Actions ---
    def refresh_rate_limit(self):
        self.btn_rate_limit.configure(state='disabled', text="⌛ Lade...")
        self.lbl_rl_status.configure(text="Status: Frage API ab...")
        asyncio.run_coroutine_threadsafe(self._refresh_rate_limit_task(), self.loop)

    async def _refresh_rate_limit_task(self):
        try:
            async with aiohttp.ClientSession() as session:
                data = await ebay_analytics.get_rate_limit_status(session)
                sell_d = data.get("sell", {})
                buy_d = data.get("buy", {})
                
                def update_ui():
                    # Update Sell
                    self.lbl_sell_limit_total.configure(text=f"{sell_d.get('limit', 0):,}".replace(",", "."))
                    self.lbl_sell_limit_used.configure(text=f"{sell_d.get('used', 0):,}".replace(",", "."))
                    s_rem = sell_d.get('remaining', 0)
                    self.lbl_sell_limit_remaining.configure(text=f"{s_rem:,}".replace(",", "."))
                    if s_rem < 500: self.lbl_sell_limit_remaining.configure(foreground='red')
                    elif s_rem < 1000: self.lbl_sell_limit_remaining.configure(foreground='orange')
                    else: self.lbl_sell_limit_remaining.configure(foreground='#28a745')
                    self.lbl_sell_limit_reset.configure(text=sell_d.get('reset', 'Unbekannt'))
                    
                    # Update Buy
                    self.lbl_buy_limit_total.configure(text=f"{buy_d.get('limit', 0):,}".replace(",", "."))
                    self.lbl_buy_limit_used.configure(text=f"{buy_d.get('used', 0):,}".replace(",", "."))
                    b_rem = buy_d.get('remaining', 0)
                    self.lbl_buy_limit_remaining.configure(text=f"{b_rem:,}".replace(",", "."))
                    if b_rem < 500: self.lbl_buy_limit_remaining.configure(foreground='red')
                    elif b_rem < 1000: self.lbl_buy_limit_remaining.configure(foreground='orange')
                    else: self.lbl_buy_limit_remaining.configure(foreground='#28a745')
                    self.lbl_buy_limit_reset.configure(text=buy_d.get('reset', 'Unbekannt'))

                    self.lbl_rl_status.configure(text=f"Status: Aktualisiert um {time.strftime('%H:%M:%S')}")
                    self.btn_rate_limit.configure(state='normal', text="🔄 Rate Limit abrufen")

                self.after(0, update_ui)
        except Exception as e:
            logging.error(f"Rate Limit Fehler: {e}")
            def show_err():
                self.lbl_rl_status.configure(text="Status: Fehler!")
                self.btn_rate_limit.configure(state='normal', text="🔄 Rate Limit abrufen")
                messagebox.showerror("Fehler", f"Konnte Rate Limit nicht abrufen:\n{e}")
            self.after(0, show_err)

    def test_connection(self):
        asyncio.run_coroutine_threadsafe(self._test_connection_task(), self.loop)

    async def _test_connection_task(self):
        logging.info("Teste Datenbankverbindung...")
        pool = await self._get_db_pool()
        if pool:
            self.after(0, lambda: messagebox.showinfo("Erfolg", "Verbindung zur Datenbank erfolgreich hergestellt!"))
        else:
            self.after(0, lambda: messagebox.showerror("Fehler", "Verbindung zur Datenbank fehlgeschlagen. Details in den Logs."))

    def _load_settings(self):
        load_dotenv(self.env_path)
        for key, var in self.settings_vars.items():
            val = os.environ.get(key, "")
            var.set(val)

    def _update_fixkosten_hint(self):
        try:
            fk = float(self.settings_vars["FIXKOSTEN_MONATLICH"].get().replace(',', '.'))
            n = int(self.settings_vars["ERWARTETE_VERKAEUFE"].get())
            if n > 0:
                val = fk / n
                self.lbl_fixkosten_hint.configure(text=f"= {val:.3f}€ pro Verkauf")
        except:
            self.lbl_fixkosten_hint.configure(text="Ungültige Werte")

    def save_settings(self):
        if not os.path.exists(self.env_path):
            open(self.env_path, 'w').close()
            
        for key, var in self.settings_vars.items():
            set_key(self.env_path, key, var.get())
        
        # Token Manager zurücksetzen, damit neue Credentials sofort wirken
        try:
            from ebay_token_manager import reset as reset_token_manager
            reset_token_manager()
        except Exception:
            pass
            
        messagebox.showinfo("Erfolg", "Einstellungen wurden in der .env aktualisiert.")

    def add_new_links(self):
        """Processes links from text area, adds them to DB, and clears the area."""
        content = self.links_text.get(1.0, tk.END).strip()
        links = [l.strip() for l in content.split('\n') if l.strip()]
        
        if not links:
            messagebox.showwarning("Warnung", "Keine Links zum Hinzufügen gefunden.")
            return
            
        def run():
            self.loop.call_soon_threadsafe(lambda: asyncio.create_task(self._add_links_task(links)))
        
        threading.Thread(target=run, daemon=True).start()

    async def _add_links_task(self, links):
        pool = await self._get_db_pool()
        if not pool: return
        
        try:
            await scrape.insert_links_into_sitetoscrape(links, pool)
            self.after(0, lambda: self.links_text.delete(1.0, tk.END))
            self.after(0, lambda: messagebox.showinfo("Erfolg", f"{len(links)} Links wurden zur Warteschlange hinzugefügt."))
            self.after(0, self.refresh_history_table)
        except Exception as e:
            logging.error(f"Fehler beim Hinzufügen der Links: {e}")
            self.after(0, lambda: messagebox.showerror("Fehler", f"Konnte Links nicht hinzufügen: {e}"))

    def toggle_history(self):
        if self.history_visible.get():
            self.history_visible.set(False)
            self.history_container.pack_forget()
            self.btn_refresh_history.pack_forget()
            self.btn_toggle_history.configure(text="▶ Link-Verlauf anzeigen (Datenbank)")
        else:
            self.history_visible.set(True)
            self.history_container.pack(fill=BOTH, expand=True)
            self.btn_refresh_history.pack(side=LEFT, padx=10)
            self.btn_toggle_history.configure(text="▼ Link-Verlauf ausblenden")
            self.refresh_history_table()

    def refresh_history_table(self):
        if not self.history_visible.get(): return
        asyncio.run_coroutine_threadsafe(self._refresh_history_task(), self.loop)

    async def _refresh_history_task(self):
        pool = await self._get_db_pool()
        if not pool: return
        
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT id, link, anzahlSeiten, numbersOfBooks, 
                           CASE WHEN is_scraped THEN 'Gescrapt' ELSE 'Wartend' END as status
                    FROM sitetoscrape 
                    ORDER BY id DESC LIMIT 50
                """)
                
                def update_ui():
                    for item in self.tree_history.get_children():
                        self.tree_history.delete(item)
                    for r in rows:
                        self.tree_history.insert("", tk.END, values=(
                            r["id"], r["link"], r["anzahlseiten"], r["numbersofbooks"], r["status"]
                        ))
                
                self.after(0, update_ui)
        except Exception as e:
            logging.error(f"Fehler beim Laden des Verlaufs: {e}")

    def _load_links(self):
        pass

    def save_links(self):
        pass

    def select_all(self):
        for item in self.tree.get_children():
            self.tree.selection_add(item)

    def deselect_all(self):
        self.tree.selection_remove(self.tree.selection())

    def refresh_upload_table(self):
        asyncio.run_coroutine_threadsafe(self._refresh_task(), self.loop)

    async def _refresh_task(self):
        pool = await self._get_db_pool()
        if not pool:
            self.after(0, lambda: messagebox.showerror("Fehler", "Keine Datenbankverbindung möglich. Bitte .env prüfen."))
            return
        
        try:
            books = await ebay_upload.get_unlisted_books(pool, limit=100)
            
            def update_ui():
                try:
                    for item in self.tree.get_children():
                        self.tree.delete(item)
                    for b in books:
                        vals = (
                            str(b.get('id', '')),
                            str(b.get('title', '')),
                            str(b.get('autor', '')),
                            str(b.get('start_price', '')),
                            str(b.get('isbn', ''))
                        )
                        self.tree.insert("", tk.END, values=vals)
                except Exception as ex:
                    logging.error(f"UI Update error: {ex}")
            
            self.after(0, update_ui)
        except Exception as e:
            logging.error(f"Error refreshing table: {e}")
            self.after(0, lambda: messagebox.showerror("Fehler", f"Fehler beim Laden der Daten: {e}"))


    def delete_selected(self):
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showwarning("Warnung", "Bitte wähle mindestens ein Buch aus der Liste aus.")
            return
        
        ids = [int(self.tree.item(item)['values'][0]) for item in selected_items]
        if messagebox.askyesno("Confirm", f"Möchtest du {len(ids)} Bücher wirklich unwiderruflich aus der Datenbank löschen?"):
            asyncio.run_coroutine_threadsafe(self._delete_task(ids), self.loop)

    async def _delete_task(self, ids):
        pool = await self._get_db_pool()
        if not pool: return
        
        try:
            await DatabaseManager.delete_library_entries(pool, ids)
            self.after(0, lambda: messagebox.showinfo("Erfolg", f"{len(ids)} Einträge wurden gelöscht."))
            await self._refresh_task()
        except Exception as e:
            self.after(0, lambda: messagebox.showerror("Fehler", f"Löschen fehlgeschlagen: {e}"))

    def upload_selected(self):
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showwarning("Warnung", "Bitte wähle mindestens ein Buch aus der Liste aus.")
            return
        
        ids = [self.tree.item(item)['values'][0] for item in selected_items]
        if messagebox.askyesno("Confirm", f"Möchtest du {len(ids)} Bücher zu eBay hochladen?"):
            asyncio.run_coroutine_threadsafe(self._upload_task(ids), self.loop)

    async def _upload_task(self, ids):
        pool = await self._get_db_pool()
        if not pool: return
        
        self.after(0, lambda: self.btn_upload.configure(state='disabled', text="Uploading..."))
        try:
            await ebay_upload.run_upload_batch(pool, specific_ids=ids)
            self.after(0, lambda: messagebox.showinfo("Erfolg", "Upload abgeschlossen. Details findest du in den Logs."))
            await self._refresh_task()
        except Exception as e:
            self.after(0, lambda: messagebox.showerror("Fehler", f"Upload fehlgeschlagen: {e}"))
        finally:
            self.after(0, lambda: self.btn_upload.configure(state='normal', text="Ausgewählte Hochladen"))

    def sync_prices(self):
        self.start_price_sync()


    def toggle_auto_sync(self):
        messagebox.showinfo("Info", "Der Auto-Sync wird nun über den systemd/background Dienst gesteuert.")

    def start_scraping(self):
        if self.scrape_task and not self.scrape_task.done():
            if messagebox.askyesno("Stop", "Möchtest du den Scraping-Prozess wirklich abbrechen?"):
                self.scrape_task.cancel()
                logging.info("Stopp-Signal gesendet...")
            return

        self.btn_start.configure(bootstyle=DANGER, text="Scraping Stoppen")
        self.scrape_task = asyncio.run_coroutine_threadsafe(self._scrape_task(), self.loop)

    async def _scrape_task(self):
        pool = await self._get_db_pool()
        if not pool:
            self.after(0, lambda: self.btn_start.configure(bootstyle=SUCCESS, text="Scraping Starten"))
            return
        
        try:
            logging.info("Bot gestartet...")
            def get_links():
                content = self.links_text.get(1.0, tk.END).strip()
                return [l.strip() for l in content.split('\n') if l.strip()]
            
            links = get_links()
            
            if links:
                await scrape.insert_links_into_sitetoscrape(links, pool)
                await scrape.scrape_and_save_pages(pool)
            
            await scrape.perform_webscrape_async(pool)
            
            logging.info("Scraping erfolgreich abgeschlossen.")
            self.after(0, lambda: messagebox.showinfo("Erfolg", "Scraping abgeschlossen."))
        except asyncio.CancelledError:
            logging.warning("Scraping wurde vom Benutzer abgebrochen.")
        except Exception as e:
            logging.error(f"Scraping Error: {e}")
            self.after(0, lambda: messagebox.showerror("Fehler", f"Scraping fehlgeschlagen: {e}"))
        finally:
            self.after(0, lambda: self.btn_start.configure(bootstyle=SUCCESS, text="Scraping Starten"))

    def start_vacation_reactivation(self):
        if hasattr(self, 'vacation_task') and self.vacation_task and not self.vacation_task.done():
            messagebox.showinfo("Info", "Die Reaktivierung läuft bereits.")
            return

        self.btn_vacation.configure(state='disabled', text="⌛ Prüfe Urlaub...")
        self.vacation_task = asyncio.run_coroutine_threadsafe(self._vacation_reactivation_task(), self.loop)

    async def _vacation_reactivation_task(self):
        try:
            logging.info("Urlaubs-Reaktivierung gestartet...")
            # Wir rufen direkt die main() aus dem reactivate_vacation Modul auf
            # Da diese main() bereits logging macht, sehen wir es in der GUI
            await reactivate_vacation.main()
            
            self.after(0, lambda: messagebox.showinfo("Erfolg", "Urlaubs-Reaktivierung abgeschlossen. Prüfe die Logs für Details."))
        except Exception as e:
            logging.error(f"Fehler bei Reaktivierung: {e}")
            self.after(0, lambda: messagebox.showerror("Fehler", f"Reaktivierung fehlgeschlagen: {e}"))
        finally:
            self.after(0, lambda: self.btn_vacation.configure(state='normal', text="🏖️ Urlaubs-Reaktivierung"))

    def start_price_sync(self):
        if hasattr(self, 'sync_task') and self.sync_task and not self.sync_task.done():
            messagebox.showinfo("Info", "Der Preis-Sync läuft bereits.")
            return

        self.btn_sync.configure(state='disabled', text="⌛ Synchronisiere...")
        self.sync_task = asyncio.run_coroutine_threadsafe(self._price_sync_task(), self.loop)

    async def _price_sync_task(self):
        try:
            logging.info("Bestands- & Preis-Sync gestartet...")
            # Wir rufen direkt die main() aus dem sync.booklooker.ebay Modul auf
            await sync_ebay.main()
            
            self.after(0, lambda: messagebox.showinfo("Erfolg", "Synchronisation abgeschlossen. Details findest du in den Logs."))
        except Exception as e:
            logging.error(f"Fehler bei Synchronisation: {e}")
            self.after(0, lambda: messagebox.showerror("Fehler", f"Sync fehlgeschlagen: {e}"))
        finally:
            self.after(0, lambda: self.btn_sync.configure(state='normal', text="🔄 Bestands- & Preis-Sync"))


if __name__ == "__main__":
    app = BLBotApp()
    app.mainloop()
