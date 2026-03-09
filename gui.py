import tkinter as tk
from tkinter import ttk, messagebox
import ttkbootstrap as tb
from ttkbootstrap.constants import *
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
import price_monitor
import price_processing

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
        self.notebook = tb.Notebook(self, bootstyle="primary")
        self.notebook.pack(fill=BOTH, expand=True, padx=10, pady=10)
        
        self.tab_dashboard = tb.Frame(self.notebook)
        self.tab_upload = tb.Frame(self.notebook)
        self.tab_links = tb.Frame(self.notebook)
        self.tab_settings = tb.Frame(self.notebook)
        
        self.notebook.add(self.tab_dashboard, text="🚀 Scraper Dashboard")
        self.notebook.add(self.tab_upload, text="📦 Upload Manager")
        self.notebook.add(self.tab_links, text="🔗 Links")
        self.notebook.add(self.tab_settings, text="⚙️ Settings")
        
        self._build_dashboard()
        self._build_upload_manager()
        self._build_links_tab()
        self._build_settings_tab()
        
        # Load initials
        self._load_settings()
        self._load_links()

        # Custom Logger format
        self.log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', '%H:%M:%S')
        
        # Setup logging redirection
        self.handler = TextHandler(self.log_text)
        self.handler.setFormatter(self.log_format)
        logging.getLogger().addHandler(self.handler)
        logging.getLogger().setLevel(logging.INFO)

        self.db_pool = None
        self.scrape_task = None
        self.auto_sync_active = False
        self.sync_loop_task = None
        
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self._run_async_loop, daemon=True).start()
        
        # Handle clean exit
        self.protocol("WM_DELETE_WINDOW", self.on_closing)

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

        self.btn_sync = tb.Button(controls, text="Preis-Sync Jetzt", bootstyle=INFO, command=self.sync_prices)
        self.btn_sync.pack(side=LEFT, padx=10)

        self.btn_auto_sync = tb.Button(controls, text="Auto-Sync: AUS", bootstyle=(SECONDARY, OUTLINE), command=self.toggle_auto_sync)
        self.btn_auto_sync.pack(side=LEFT, padx=10)

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

        self.btn_comp_check = tb.Button(controls, text="Konkurrenzcheck starten", bootstyle=WARNING, command=self.start_competitor_check)
        self.btn_comp_check.pack(side=LEFT, padx=5)
        
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
        lbl = tb.Label(self.tab_links, text="Zu scrapende Booklooker-URLs (eine pro Zeile):")
        lbl.pack(anchor=W, padx=10, pady=10)
        
        self.links_text = tb.Text(self.tab_links, wrap='none', height=20)
        self.links_text.pack(fill=BOTH, expand=True, padx=10)
        
        btn_save = tb.Button(self.tab_links, text="Links Speichern", bootstyle=SUCCESS, command=self.save_links)
        btn_save.pack(pady=10)
        
    def _build_settings_tab(self):
        self.settings_vars = {
            "DATABASE_URL": tk.StringVar(),
            "EBAY_APP_ID": tk.StringVar(),
            "EBAY_DEV_ID": tk.StringVar(),
            "EBAY_CERT_ID": tk.StringVar(),
            "EBAY_USER_TOKEN": tk.StringVar(),
            "EBAY_ENV": tk.StringVar(value="SANDBOX"),
            "FIXKOSTEN_MONATLICH": tk.StringVar(value="79.95"),
            "ANZAHL_LISTINGS": tk.StringVar(value="2500"),
            "MINDESTMARGE": tk.StringVar(value="2.50")
        }
        
        container = tb.Frame(self.tab_settings, padding=20)
        container.pack(fill=BOTH, expand=True)
        
        row = 0
        for key, var in self.settings_vars.items():
            tb.Label(container, text=key, width=20).grid(row=row, column=0, pady=10, sticky=W)
            if key == "EBAY_ENV":
                cb = tb.Combobox(container, textvariable=var, values=["SANDBOX", "PRODUCTION"])
                cb.grid(row=row, column=1, sticky=EW, padx=10)
            else:
                tb.Entry(container, textvariable=var, show="*" if "TOKEN" in key or "CERT" in key or "DATABASE" in key else "").grid(row=row, column=1, sticky=EW, padx=10)
            row += 1
            
        container.columnconfigure(1, weight=1)
        
        btn_save = tb.Button(container, text="Einstellungen Speichern", bootstyle=SUCCESS, command=self.save_settings)
        btn_save.grid(row=row, column=0, pady=20, padx=5)

        btn_test = tb.Button(container, text="Verbindung Testen", bootstyle=INFO, command=self.test_connection)
        btn_test.grid(row=row, column=1, pady=20, padx=5)

        row += 1
        self.lbl_fixkosten_hint = tb.Label(container, text="", font=("Helvetica", 8, "italic"))
        self.lbl_fixkosten_hint.grid(row=row, column=1, sticky=W, padx=10)
        self._update_fixkosten_hint()

        # Update hint when values change
        self.settings_vars["FIXKOSTEN_MONATLICH"].trace_add("write", lambda *a: self._update_fixkosten_hint())
        self.settings_vars["ANZAHL_LISTINGS"].trace_add("write", lambda *a: self._update_fixkosten_hint())

    # --- Actions ---
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
            n = int(self.settings_vars["ANZAHL_LISTINGS"].get())
            if n > 0:
                val = fk / n
                self.lbl_fixkosten_hint.configure(text=f"= {val:.3f}€ pro Listing")
        except:
            self.lbl_fixkosten_hint.configure(text="Ungültige Werte")

    def save_settings(self):
        if not os.path.exists(self.env_path):
            open(self.env_path, 'w').close()
            
        for key, var in self.settings_vars.items():
            set_key(self.env_path, key, var.get())
            
        messagebox.showinfo("Erfolg", "Einstellungen wurden in der .env aktualisiert.")

    def _load_links(self):
        if os.path.exists(self.links_path):
            with open(self.links_path, "r", encoding="utf-8") as f:
                self.links_text.insert(tk.END, f.read())

    def save_links(self):
        with open(self.links_path, "w", encoding="utf-8") as f:
            f.write(self.links_text.get(1.0, tk.END).strip())
        messagebox.showinfo("Erfolg", "Links wurden gespeichert.")

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
        """Manually trigger the price monitoring sync."""
        def run():
            self.loop.call_soon_threadsafe(lambda: asyncio.create_task(self._sync_task_async()))
        
        threading.Thread(target=run, daemon=True).start()

    async def _sync_task_async(self):
        pool = await self._get_db_pool()
        if not pool: return
        
        self.btn_sync.configure(state='disabled', text="Sync läuft...")
        try:
            logging.info("Manueller Preis-Sync gestartet...")
            await price_monitor.run_price_monitor(pool)
            logging.info("Preis-Sync abgeschlossen.")
        except Exception as e:
            logging.error(f"Fehler beim Preis-Sync: {e}")
        finally:
            self.after(0, lambda: self.btn_sync.configure(state='normal', text="Preis-Sync Jetzt"))

    def toggle_auto_sync(self):
        """Toggles the background auto-sync loop."""
        if self.auto_sync_active:
            self.auto_sync_active = False
            self.btn_auto_sync.configure(text="Auto-Sync: AUS", bootstyle=(SECONDARY, OUTLINE))
            logging.info("Auto-Sync deaktiviert.")
        else:
            self.auto_sync_active = True
            self.btn_auto_sync.configure(text="Auto-Sync: EIN", bootstyle=SUCCESS)
            logging.info("Auto-Sync aktiviert (Intervall: 4 Std.).")
            self.loop.call_soon_threadsafe(lambda: asyncio.create_task(self._auto_sync_loop()))

    async def _auto_sync_loop(self):
        """Background loop that periodically runs the price sync."""
        interval = 4 * 3600 # 4 Hours
        
        while self.auto_sync_active:
            try:
                await self._sync_task_async()
            except Exception as e:
                logging.error(f"Fehler im Auto-Sync Loop: {e}")
            
            for _ in range(interval // 10): 
                if not self.auto_sync_active:
                    break
                await asyncio.sleep(10)

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

    def start_competitor_check(self):
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showwarning("Warnung", "Bitte wähle mindestens ein Buch aus.")
            return
        
        ids = [self.tree.item(item)['values'][0] for item in selected_items]
        logging.info(f"Starte Konkurrenzcheck für {len(ids)} Bücher...")
        asyncio.run_coroutine_threadsafe(self._competitor_check_task(ids), self.loop)

    async def _competitor_check_task(self, ids):
        pool = await self._get_db_pool()
        if not pool: return
        
        token = self.settings_vars["EBAY_USER_TOKEN"].get()
        env = self.settings_vars["EBAY_ENV"].get()
        base_url = "https://api.ebay.com" if env == "PRODUCTION" else "https://api.sandbox.ebay.com"
        
        if not token:
            logging.error("Kein eBay User Token in den Settings gefunden!")
            return

        try:
            fk_monat = Decimal(self.settings_vars["FIXKOSTEN_MONATLICH"].get().replace(',', '.'))
            listings = int(self.settings_vars["ANZAHL_LISTINGS"].get())
            marge_req = Decimal(self.settings_vars["MINDESTMARGE"].get().replace(',', '.'))
        except:
            logging.error("Ungültige Kalkulations-Parameter in den Settings!")
            return

        self.after(0, lambda: self.btn_comp_check.configure(state='disabled', text="Checking..."))
        
        success_count = 0
        rentable_count = 0
        
        try:
            async with aiohttp.ClientSession() as session:
                for internal_id in ids:
                    try:
                        async with pool.acquire() as conn:
                            row = await conn.fetchrow("SELECT LinkToBL FROM library WHERE id = $1", int(internal_id))
                            if not row: continue
                            bl_url = row['linktobl']

                        async with session.get(bl_url) as resp:
                            if resp.status != 200:
                                logging.error(f"Konnte BL-URL nicht laden: {bl_url}")
                                continue
                            html = await resp.text()
                            soup = BeautifulSoup(html, "html.parser")

                        # Call get_price which handles ISBN extraction and competitor API calls
                        await price_processing.PriceProcessing.get_price(
                            session=session,
                            soup=soup,
                            num=int(internal_id),
                            db_pool=pool,
                            token=token,
                            base_url=base_url,
                            fixed_costs_monthly=fk_monat,
                            total_listings=listings,
                            min_margin_req=marge_req
                        )
                        success_count += 1
                        
                        async with pool.acquire() as conn:
                            rentabel = await conn.fetchval("SELECT rentabel FROM library WHERE id = $1", int(internal_id))
                            if rentabel: rentable_count += 1

                    except Exception as e:
                        logging.error(f"Fehler bei ID {internal_id}: {e}")

            logging.info(f"Konkurrenzcheck beendet. {success_count} geprüft, {rentable_count} rentabel.")
            self.after(0, lambda: messagebox.showinfo("Check beendet", 
                f"Konkurrenzcheck für {success_count} Bücher abgeschlossen.\n\n"
                f"✅ Rentabel: {rentable_count}\n"
                f"❌ Nicht rentabel: {success_count - rentable_count}\n\n"
                f"Details siehe Live-Logs."))
            
            await self._refresh_task()

        finally:
            self.after(0, lambda: self.btn_comp_check.configure(state='normal', text="Konkurrenzcheck starten"))

if __name__ == "__main__":
    app = BLBotApp()
    app.mainloop()
