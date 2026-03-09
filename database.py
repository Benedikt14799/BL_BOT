import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Klasse: DatabaseManager
    ------------------------
    Verwaltet die Datenbankerstellung und das Einfügen neuer Scraping-Daten.
    """

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
            logger.info("Tabelle 'sitetoscrape' existiert nun oder wurde neu angelegt.")

            # Haupttabelle für die gescrapten Buchdaten
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS library (
                    id SERIAL PRIMARY KEY,
                    sitetoscrape_id INTEGER REFERENCES sitetoscrape(id),
                    Action VARCHAR(255),
                    Custom_label_SKU VARCHAR(255),
                    CategoryID INTEGER,
                    CategoryName VARCHAR(255),
                    Title VARCHAR(255),
                    Relationship VARCHAR(255),
                    RelationshipDetails VARCHAR(255),
                    ISBN VARCHAR(255),
                    EPID VARCHAR(255),
                    Start_price NUMERIC,
                    Margin NUMERIC,
                    Quantity INTEGER DEFAULT 1,
                    photo TEXT,
                    VideoID VARCHAR(255),
                    Condition_ID VARCHAR(255),
                    Description TEXT,
                    Format VARCHAR(255),
                    Duration VARCHAR(255),
                    Buy_It_Now_price NUMERIC,
                    Best_Offer_Enabled INTEGER DEFAULT 1,
                    Best_Offer_Auto_Accept_Price NUMERIC,
                    Minimum_Best_Offer_Price NUMERIC,
                    VAT_percent NUMERIC,
                    Immediate_pay_required BOOLEAN DEFAULT FALSE,
                    Location VARCHAR(255),
                    Shipping_service_1_option VARCHAR(255),
                    Shipping_service_1_cost NUMERIC,
                    Shipping_service_1_priority INTEGER,
                    Shipping_service_2_option VARCHAR(255),
                    Shipping_service_2_cost NUMERIC,
                    Shipping_service_2_priority INTEGER,
                    Max_dispatch_time VARCHAR(255),
                    Returns_accepted_option VARCHAR(255),
                    Returns_within_option VARCHAR(255),
                    Refund_option VARCHAR(255),
                    Return_shipping_cost_paid_by VARCHAR(255),
                    Shipping_profile_name VARCHAR(255),
                    Return_profile_name VARCHAR(255),
                    Payment_profile_name VARCHAR(255),
                    ProductCompliancePolicyID VARCHAR(255),
                    Regional_ProductCompliancePolicies VARCHAR(255),
                    EconomicOperator_CompanyName VARCHAR(255),
                    EconomicOperator_AddressLine1 VARCHAR(255),
                    EconomicOperator_AddressLine2 VARCHAR(255),
                    EconomicOperator_City VARCHAR(255),
                    EconomicOperator_Country VARCHAR(255),
                    EconomicOperator_PostalCode VARCHAR(255),
                    EconomicOperator_StateOrProvince VARCHAR(255),
                    EconomicOperator_Phone VARCHAR(255),
                    EconomicOperator_Email VARCHAR(255),
                    Autor VARCHAR(255),
                    Buchtitel TEXT,
                    Sprache VARCHAR(255),
                    Thematik TEXT,
                    Buchreihe TEXT,
                    Genre TEXT,
                    Verlag TEXT,
                    Erscheinungsjahr VARCHAR(255),
                    CFormat VARCHAR(255),
                    Originalsprache VARCHAR(255),
                    Herstellungsland_und_region VARCHAR(255),
                    Produktart TEXT,
                    Literarische_Gattung TEXT,
                    Zielgruppe TEXT,
                    Signiert_von VARCHAR(255),
                    Literarische_Bewegung TEXT,
                    Ausgabe TEXT,
                    LinkToBL TEXT UNIQUE,
                    SKU VARCHAR(50) UNIQUE DEFAULT 'BL-' || LPAD(nextval('custom_sku_seq')::text, 6, '0'),  -- Eindeutige SKU für eBay
                    created_at TIMESTAMP DEFAULT NOW(),
                    ebay_listed BOOLEAN DEFAULT FALSE,
                    ebay_listing_id VARCHAR(255),
                    ebay_error TEXT
                );
            """)
            logger.info("Tabelle 'library' existiert nun oder wurde neu angelegt.")

            # Trigger um Custom_label_SKU synchron zu SKU zu halten
            try:
                await conn.execute("""
                    CREATE OR REPLACE FUNCTION sync_custom_sku()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        IF NEW.sku IS NOT NULL THEN
                            NEW.custom_label_sku := NEW.sku;
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                """)
                
                await conn.execute("""
                    DROP TRIGGER IF EXISTS trigger_sync_sku ON library;
                    CREATE TRIGGER trigger_sync_sku
                    BEFORE INSERT ON library
                    FOR EACH ROW
                    EXECUTE FUNCTION sync_custom_sku();
                """)
                logger.info("Trigger 'trigger_sync_sku' für Custom-SKU Synchronisation erstellt.")
            except Exception as e:
                logger.error(f"Fehler beim Erstellen des Triggers: {e}")

            # Migration: Neue Spalten hinzufügen, falls nicht vorhanden
            # Purchase_price (Einkaufspreis) und Purchase_shipping (BL-Versandkosten) sowie created_at
            try:
                await conn.execute("""
                    ALTER TABLE library
                    ADD COLUMN IF NOT EXISTS Purchase_price NUMERIC,
                    ADD COLUMN IF NOT EXISTS Purchase_shipping NUMERIC,
                    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW(),
                    ADD COLUMN IF NOT EXISTS ebay_listed BOOLEAN DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS ebay_listing_id VARCHAR(255),
                    ADD COLUMN IF NOT EXISTS ebay_error TEXT,
                    DROP COLUMN IF EXISTS enriched;
                """)
                
                await conn.execute("""
                    ALTER TABLE sitetoscrape
                    ADD COLUMN IF NOT EXISTS is_scraped BOOLEAN DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();
                """)
                logger.info("Migration: Struktur von library und sitetoscrape aktualisiert (Masterdata & Versionierung).")
            except Exception as e:
                logger.error(f"Migration der Tabellenstrukturen fehlgeschlagen: {e}")

            # Migration für eBay-Anbindung & Konkurrenzcheck v2
            try:
                await conn.execute("""
                    ALTER TABLE library 
                    ADD COLUMN IF NOT EXISTS ebay_item_id BIGINT,
                    ADD COLUMN IF NOT EXISTS ebay_listed_at TIMESTAMP,
                    ADD COLUMN IF NOT EXISTS ebay_status VARCHAR(20) DEFAULT 'pending',
                    ADD COLUMN IF NOT EXISTS competitor_min_preis NUMERIC,
                    ADD COLUMN IF NOT EXISTS competitor_median_preis NUMERIC,
                    ADD COLUMN IF NOT EXISTS empfohlener_ebay_preis NUMERIC,
                    ADD COLUMN IF NOT EXISTS anzahl_konkurrenzangebote INTEGER,
                    ADD COLUMN IF NOT EXISTS last_competitor_check TIMESTAMP,
                    ADD COLUMN IF NOT EXISTS rentabel BOOLEAN,
                    ADD COLUMN IF NOT EXISTS fehlende_marge NUMERIC,
                    ADD COLUMN IF NOT EXISTS bl_condition VARCHAR(100),
                    ADD COLUMN IF NOT EXISTS ebay_condition_filter VARCHAR(50),
                    ADD COLUMN IF NOT EXISTS competitor_filter_level VARCHAR(20),
                    ADD COLUMN IF NOT EXISTS outlier_removed_count INTEGER DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS gewinn_real NUMERIC,
                    ADD COLUMN IF NOT EXISTS days_not_profitable INTEGER DEFAULT 0,
                    ADD COLUMN IF NOT EXISTS next_recheck_date DATE;
                """)
                logger.info("Migration: Struktur von library für eBay-Upload und Konkurrenzcheck v2 aktualisiert.")
            except Exception as e:
                logger.error(f"Migration für eBay-Upload/Konkurrenzcheck fehlgeschlagen: {e}")

            # Neue Tabelle für Listings ohne gültige ISBN Neue Tabelle für Listings ohne gültige ISBN
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS missing_listings (
                    library_id   INTEGER PRIMARY KEY,
                    link         TEXT NOT NULL,
                    reason       TEXT NOT NULL,
                    recorded_at  TIMESTAMP DEFAULT NOW()
                );
            """)
            logger.info("Tabelle 'missing_listings' existiert nun oder wurde neu angelegt.")

    @staticmethod
    async def insert_library_entry(db_pool, properties: dict):
        try:
            async with db_pool.acquire() as conn:
                # Einfügen und ID zurückbekommen
                result = await conn.fetchrow("""
                                             INSERT INTO library
                                             (Autor, Buchtitel, Sprache, Thematik, Verlag, Erscheinungsjahr,
                                              CFormat, Produktart, Ausgabe, Description, bl_condition)
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
        Speichert einen Datensatz, der keine oder eine ungültige ISBN hatte, und löscht ihn gleichzeitig aus library.
        """
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    WITH deleted AS (
                        DELETE FROM library WHERE id = $1
                    )
                    INSERT INTO missing_listings (library_id, link, reason)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (library_id) DO NOTHING
                """, library_id, link, reason)
            logger.info(f"Missing listing aufgezeichnet und library_id {library_id} gelöscht, Grund={reason}")
        except Exception as e:
            logger.error(f"Fehler beim Aufzeichnen von missing_listing {library_id}: {e}")

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
                Action                    = 'Add',
                CategoryID                = 261186,
                CategoryName              = $1,
                Duration                  = 'GTC',
                Format                    = 'FixedPrice',
                Location                  = 78567,
                Shipping_profile_name     = 'Standardversand Bücher Deutschland',
                Return_profile_name       = 'Rückgabe für Bücher',
                Payment_profile_name      = 'Zahlung für Bücher',
                Quantity                  = 1,
                Best_Offer_Enabled        = 1
            WHERE Action IS NULL
        """
        logger.debug("prefill_db SQL:\n%s", sql.strip())
        logger.debug("prefill_db Parameter: category_name=%s", category_name)

        try:
            async with db_pool.acquire() as conn:
                await conn.execute(sql, category_name)
            logger.info("Statische Standardwerte wurden erfolgreich eingefügt. Category Name: %s", category_name)
        except Exception as e:
            logger.error("Fehler in prefill_db_with_static_data: %s", e)

