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
from database import DatabaseManager
import ebay_upload
import scrape

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
        self.loop = asyncio.new_event_loop()
        threading.Thread(target=self._run_async_loop, daemon=True).start()

    def _run_async_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _get_db_pool(self):
        if not self.db_pool:
            db_url = os.environ.get("DATABASE_URL")
            if not db_url:
                logging.error("DATABASE_URL missing in .env")
                return None
            try:
                # Create pool synchronously in the async loop
                future = asyncio.run_coroutine_threadsafe(self._init_pool(db_url), self.loop)
                self.db_pool = future.result(timeout=10)
            except Exception as e:
                logging.error(f"Failed to connect to DB: {e}")
                return None
        return self.db_pool

    async def _init_pool(self, dsn):
        pool = await asyncpg.create_pool(dsn=dsn, ssl="require")
        await DatabaseManager.create_table(pool)
        return pool

    def _build_dashboard(self):
        # Controls
        controls = tb.Frame(self.tab_dashboard, padding=10)
        controls.pack(fill=X)
        
        self.btn_start = tb.Button(controls, text="Scraping Starten", bootstyle=SUCCESS, command=self.start_scraping)
        self.btn_start.pack(side=LEFT, padx=10)

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
            "EBAY_ENV": tk.StringVar(value="SANDBOX")
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
        btn_save.grid(row=row, columnspan=2, pady=20)

    # --- Actions ---
    def _load_settings(self):
        load_dotenv(self.env_path)
        for key, var in self.settings_vars.items():
            val = os.environ.get(key, "")
            var.set(val)

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
            self.after(0, lambda: messagebox.showerror("Fehler", "Keine DATABASE_URL gefunden."))
            return
        
        books = await ebay_upload.get_unlisted_books(pool, limit=100)
        
        def update_ui():
            for item in self.tree.get_children():
                self.tree.delete(item)
            for b in books:
                self.tree.insert("", tk.END, values=(b['id'], b['title'], b['autor'], b['start_price'], b['isbn']))
        
        self.after(0, update_ui)

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
        self.after(0, lambda: self.btn_upload.configure(state='disabled', text="Uploading..."))
        try:
            await ebay_upload.run_upload_batch(pool, specific_ids=ids)
            self.after(0, lambda: messagebox.showinfo("Erfolg", "Upload abgeschlossen. Details findest du in den Logs."))
            self.refresh_upload_table()
        except Exception as e:
            self.after(0, lambda: messagebox.showerror("Fehler", f"Upload fehlgeschlagen: {e}"))
        finally:
            self.after(0, lambda: self.btn_upload.configure(state='normal', text="Ausgewählte Hochladen"))

    def start_scraping(self):
        self.btn_start.configure(state='disabled', text="Läuft...")
        asyncio.run_coroutine_threadsafe(self._scrape_task(), self.loop)

    async def _scrape_task(self):
        pool = await self._get_db_pool()
        if not pool:
            self.after(0, lambda: self.btn_start.configure(state='normal', text="Scraping Starten"))
            return
        
        try:
            # Re-load links from text widget just in case
            links = self.links_text.get(1.0, tk.END).strip().split('\n')
            links = [l.strip() for l in links if l.strip()]
            
            if links:
                await scrape.insert_links_into_sitetoscrape(links, pool)
                await scrape.scrape_and_save_pages(pool)
            
            await scrape.perform_webscrape_async(pool)
            
            self.after(0, lambda: messagebox.showinfo("Erfolg", "Scraping abgeschlossen."))
        except Exception as e:
            logging.error(f"Scraping Error: {e}")
            self.after(0, lambda: messagebox.showerror("Fehler", f"Scraping fehlgeschlagen: {e}"))
        finally:
            self.after(0, lambda: self.btn_start.configure(state='normal', text="Scraping Starten"))


if __name__ == "__main__":
    app = BLBotApp()
    app.mainloop()
