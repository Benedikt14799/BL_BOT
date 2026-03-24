import logging
import asyncpg

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Klasse: DatabaseManager
    ------------------------
    Verwaltet die Datenbankerstellung und das Einfügen neuer Scraping-Daten.
    """

    @staticmethod
    async def create_pool(db_url):
        """Erstellt einen asyncpg-Pool für die Datenbankverbindung."""
        import asyncpg
        return await asyncpg.create_pool(
            db_url,
            min_size=2,
            max_size=10,
            command_timeout=60
        )

    @staticmethod
    async def table_exists(conn, table_name):
        """Prüft, ob eine Tabelle in der Datenbank existiert."""
        row = await conn.fetchrow("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = $1
            );
        """, table_name)
        return row['exists'] if row else False

    @staticmethod
    async def create_table(db_pool):
        """
        Erstellt die benötigten Tabellen, falls sie noch nicht existieren.
        Führt außerdem eine einfache Migration für neue Spalten durch.
        """
        async with db_pool.acquire() as conn:
            # Eigene Sequence für die Custom-SKU erstellen (Startet z. B. bei 10000)
            await conn.execute("""
                CREATE SEQUENCE IF NOT EXISTS custom_sku_seq START 10000;
            """)

            # Tabelle für Links, die noch gescraped werden sollen
            exists = await DatabaseManager.table_exists(conn, 'sitetoscrape')
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sitetoscrape (
                    id SERIAL PRIMARY KEY,
                    link TEXT UNIQUE NOT NULL,
                    anzahlSeiten INTEGER,
                    numbersOfBooks INTEGER,
                    is_scraped BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            if not exists:
                logger.info("Tabelle 'sitetoscrape' wurde neu angelegt.")
            else:
                logger.debug("Tabelle 'sitetoscrape' bereits vorhanden.")

            # Neue Tabelle für Listing-Status (Relational / Foreign Keys)
            exists_status = await DatabaseManager.table_exists(conn, 'library_statuses')
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS library_statuses (
                    id INTEGER PRIMARY KEY,
                    label VARCHAR(50) UNIQUE NOT NULL
                );
            """)
            
            # Initiales Seeding der Status-Werte
            await conn.execute("""
                INSERT INTO library_statuses (id, label) VALUES 
                (1, 'active'),       -- Bereit für eBay / In Prüfung
                (2, 'missing_isbn'), -- Ungültige oder fehlende ISBN
                (3, 'unprofitable'), -- Marge zu gering
                (4, 'listed'),       -- Aktiv auf eBay
                (5, 'sold_on_bl'),   -- Auf Booklooker verkauft
                (6, 'delisted'),     -- Manuell oder durch Fehler entfernt
                (7, 'pending')       -- Neu gescrapt, noch nicht geprüft
                ON CONFLICT (id) DO NOTHING;
            """)
            if not exists_status:
                logger.info("Tabelle 'library_statuses' wurde angelegt und initial befüllt.")

            # Haupttabelle für die gescrapten Buchdaten (Refactored für Status-System)
            exists = await DatabaseManager.table_exists(conn, 'library')
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS library (
                    id SERIAL PRIMARY KEY,
                    sku VARCHAR(50) UNIQUE DEFAULT 'BL-' || LPAD(nextval('custom_sku_seq')::text, 6, '0'),
                    status_id INTEGER DEFAULT 7 REFERENCES library_statuses(id),
                    ebay_status VARCHAR(20) DEFAULT 'pending',
                    ebay_listing_id VARCHAR(255),
                    ebay_item_id BIGINT,
                    last_checked TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW(),
                    
                    isbn VARCHAR(255),
                    title TEXT,
                    autor VARCHAR(255),
                    condition_id INTEGER,
                    bl_condition VARCHAR(100),
                    photo TEXT,
                    description TEXT,
                    
                    start_price NUMERIC,
                    margin NUMERIC,
                    rentabel BOOLEAN,
                    purchase_price NUMERIC,
                    purchase_shipping NUMERIC,
                    gewinn_real NUMERIC,
                    fehlende_marge NUMERIC,
                    
                    linktobl TEXT UNIQUE,
                    is_private BOOLEAN DEFAULT FALSE,
                    backup1_url TEXT,
                    backup1_price NUMERIC,
                    backup1_shipping NUMERIC,
                    backup1_is_private BOOLEAN DEFAULT FALSE,
                    backup2_url TEXT,
                    backup2_price NUMERIC,
                    backup2_shipping NUMERIC,
                    backup2_is_private BOOLEAN DEFAULT FALSE,
                    
                    categoryname VARCHAR(255),
                    location VARCHAR(255),
                    quantity INTEGER DEFAULT 1,
                    best_offer_enabled INTEGER DEFAULT 1,
                    best_offer_auto_accept_price NUMERIC,
                    minimum_best_offer_price NUMERIC,
                    immediate_pay_required BOOLEAN DEFAULT FALSE,
                    format VARCHAR(255),
                    
                    -- eBay Metadata
                    ebay_action VARCHAR(50),
                    category_id BIGINT,
                    duration VARCHAR(25) DEFAULT 'GTC',
                    
                    -- Metadata Fields 
                    sprache VARCHAR(255),
                    seitenanzahl VARCHAR(255),
                    thematik TEXT,
                    buchreihe TEXT,
                    genre TEXT,
                    verlag TEXT,
                    erscheinungsjahr VARCHAR(255),
                    cformat VARCHAR(255),
                    originalsprache VARCHAR(255),
                    herstellungsland_und_region VARCHAR(255),
                    produktart TEXT,
                    literarische_gattung TEXT,
                    zielgruppe TEXT,
                    signiert_von VARCHAR(255),
                    literarische_bewegung TEXT,
                    ausgabe TEXT,
                    
                    -- Interne Logik Felder
                    competitor_min_preis NUMERIC,
                    competitor_median_preis NUMERIC,
                    empfohlener_ebay_preis NUMERIC,
                    anzahl_konkurrenzangebote INTEGER,
                    last_competitor_check TIMESTAMP,
                    ebay_condition_filter VARCHAR(50),
                    competitor_filter_level VARCHAR(20),
                    outlier_removed_count INTEGER DEFAULT 0,
                    days_not_profitable INTEGER DEFAULT 0,
                    next_recheck_date DATE,
                    ebay_error TEXT,
                    ebay_delisted_reason TEXT,

                    -- Verknüpfung
                    sitetoscrape_id INTEGER REFERENCES sitetoscrape(id)
                );
            """)
            
            # Migration: status_id hinzufügen, falls die Tabelle schon existierte (für Option A transition)
            try:
                await conn.execute("ALTER TABLE library ADD COLUMN IF NOT EXISTS status_id INTEGER DEFAULT 7 REFERENCES library_statuses(id);")
            except: pass

            if not exists:
                logger.info("Tabelle 'library' wurde neu angelegt (Relational v1).")
            else:
                logger.debug("Tabelle 'library' bereits vorhanden.")

            # Trigger um ebay_status (String) synchron zu status_id (FK) zu halten (für Abwärtskompatibilität)
            try:
                await conn.execute("""
                    CREATE OR REPLACE FUNCTION sync_status_label()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        SELECT label INTO NEW.ebay_status FROM library_statuses WHERE id = NEW.status_id;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                    
                    DROP TRIGGER IF EXISTS trigger_sync_status ON library;
                    CREATE TRIGGER trigger_sync_status
                    BEFORE INSERT OR UPDATE OF status_id ON library
                    FOR EACH ROW
                    EXECUTE FUNCTION sync_status_label();
                """)
                logger.info("Datenbank-Trigger für Status-Synchronisation initialisiert.")
            except Exception as e:
                logger.error(f"Fehler beim Erstellen der Trigger: {e}")

            # Tabellen wurden erfolgreich initialisiert
            logger.info("Datenbank-Initialisierung (Relational v1) abgeschlossen.")


    @staticmethod
    async def mark_as_active(db_pool, library_id: int):
        """
        Markiert ein erfolgreich verarbeitetes Angebot als 'active' (Status 1).
        """
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("UPDATE library SET status_id = 1, last_checked = NOW() WHERE id = $1", library_id)
            logger.info(f"Status 'active' für library_id {library_id} gesetzt.")
        except Exception as e:
            logger.error(f"Fehler beim Setzen von Status active für {library_id}: {e}")

    @staticmethod
    async def insert_library_entry(db_pool, properties: dict):
        try:
            async with db_pool.acquire() as conn:
                # Einfügen und ID zurückbekommen
                result = await conn.fetchrow("""
                                             INSERT INTO library
                                             (autor, title, sprache, thematik, verlag, erscheinungsjahr,
                                              cformat, produktart, ausgabe, description, bl_condition)
                                              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) RETURNING id, sku
                                             """,
                                             properties.get("Autor", ""),
                                             properties.get("Buchtitel", ""),
                                             properties.get("Sprache", ""),
                                             properties.get("Thematik", ""),
                                             properties.get("Verlag", ""),
                                             properties.get("Erscheinungsjahr", ""),
                                             properties.get("CFormat", ""),
                                             properties.get("Produktart", ""),
                                             properties.get("Ausgabe", ""),
                                             properties.get("Description", ""),
                                             properties.get("Erhaltungszustand", "")
                                             )

                sku = result['sku']

                logger.info(f"Neu hinzugefügt mit SKU {sku}: {properties.get('Buchtitel')} (Zustand: {properties.get('Erhaltungszustand', 'Unbekannt')})")

        except Exception as e:
            logger.error(f"Fehler beim Einfügen: {e}")

    @staticmethod
    async def record_missing_listing(db_pool, library_id: int, link: str, reason: str):
        """
        Markiert einen Datensatz in library als 'missing_isbn' (Status 2), anstatt ihn zu löschen.
        """
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE library 
                    SET status_id = 2, 
                        ebay_error = $2,
                        last_checked = NOW()
                    WHERE id = $1
                """, library_id, reason)
            logger.info(f"Status 'missing_isbn' für library_id {library_id} gesetzt. Grund={reason}")
        except Exception as e:
            logger.error(f"Fehler beim Setzen von Status missing_isbn für {library_id}: {e}")

    @staticmethod
    async def record_unprofitable_listing(db_pool, library_id: int, link: str, reason: str, price: float = None, margin: float = None):
        """
        Markiert ein unrentables Angebot in library als 'unprofitable' (Status 3).
        """
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE library 
                    SET status_id = 3,
                        rentabel = FALSE,
                        start_price = $2,
                        margin = $3,
                        ebay_error = $4,
                        last_checked = NOW()
                    WHERE id = $1
                """, library_id, price, margin, reason)
            logger.info(f"Status 'unprofitable' für library_id {library_id} gesetzt. Preis={price}€, Marge={margin}€")
        except Exception as e:
            logger.error(f"Fehler beim Setzen von Status unprofitable für {library_id}: {e}")

    @staticmethod
    async def record_sold_listing(db_pool, library_id: int, link: str, sku: str, title: str, marker: str):
        """
        Markiert ein verkauftes Angebot in library als 'sold_on_bl' (Status 5).
        """
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    UPDATE library 
                    SET status_id = 5,
                        ebay_delisted_reason = $2,
                        last_checked = NOW()
                    WHERE id = $1
                """, library_id, marker)
            logger.info(f"Status 'sold_on_bl' für library_id {library_id} gesetzt (Marker={marker})")
        except Exception as e:
            logger.error(f"Fehler beim Setzen von Status sold_on_bl für {library_id}: {e}")

    @staticmethod
    async def set_foreignkey(db_pool):
        """
        Setzt den Fremdschlüssel sitetoscrape_id in der library-Tabelle.
        """
        try:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT id FROM sitetoscrape;")
                for row in rows:
                    sitetoscrape_id = row["id"]
                    await conn.execute("""
                        UPDATE library 
                        SET sitetoscrape_id = $1
                        WHERE sitetoscrape_id IS NULL
                    """, sitetoscrape_id)
            logger.info("Fremdschlüssel-Zuordnung abgeschlossen.")
        except Exception as e:
            logger.error(f"Fehler in set_foreignkey: {e}")

    @staticmethod
    async def delete_library_entries(db_pool, ids: list):
        """
        Löscht mehrere Einträge unwiderruflich aus der library Tabelle.
        """
        if not ids:
            return
        
        async with db_pool.acquire() as conn:
            try:
                await conn.execute("DELETE FROM library WHERE id = ANY($1)", ids)
                logger.info(f"{len(ids)} Einträge erfolgreich aus library gelöscht.")
            except Exception as e:
                logger.error(f"Fehler beim Löschen der Einträge: {e}")
                raise e

    @staticmethod
    async def prefill_db_with_static_data(db_pool, category_name: str):
        """
        Füllt die `library`-Tabelle mit Standardwerten für bestimmte Spalten.
        :param db_pool: asyncpg-Pool
        :param category_name: Name der Kategorie, z.B. "/Bücher & Zeitschriften/Bücher"
        """
        if not category_name:
            logger.debug("Kein Category Name übergeben – verwende Default '/Bücher & Zeitschriften/Bücher'")
            category_name = "/Bücher & Zeitschriften/Bücher"

        sql = """
            UPDATE library
            SET
                ebay_action               = 'Add',
                category_id               = 261186,
                categoryname              = $1,
                duration                  = 'GTC',
                format                    = 'FixedPrice',
                location                  = '78567',
                shipping_profile_name     = 'Standardversand Bücher Deutschland',
                return_profile_name       = 'Rückgabe für Bücher',
                payment_profile_name      = 'Zahlung für Bücher',
                quantity                  = 1,
                best_offer_enabled        = 1
            WHERE ebay_action IS NULL OR ebay_action = ''
        """
        logger.debug("prefill_db SQL:\n%s", sql.strip())
        logger.debug("prefill_db Parameter: category_name=%s", category_name)

        try:
            async with db_pool.acquire() as conn:
                await conn.execute(sql, category_name)
            logger.info("Statische Standardwerte wurden erfolgreich eingefügt. Category Name: %s", category_name)
        except Exception as e:
            logger.error("Fehler in prefill_db_with_static_data: %s", e)

