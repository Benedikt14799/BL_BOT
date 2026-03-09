import asyncio
import os
import asyncpg
from dotenv import load_dotenv

async def drop():
    load_dotenv(".env")
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL unset")
        return
    
    conn = await asyncpg.connect(db_url, ssl="require")
    await conn.execute("DROP TABLE IF EXISTS missing_listings CASCADE;")
    await conn.execute("DROP TABLE IF EXISTS library CASCADE;")
    await conn.execute("DROP TABLE IF EXISTS sitetoscrape CASCADE;")
    await conn.execute("DROP SEQUENCE IF EXISTS custom_sku_seq CASCADE;")
    print("Tables dropped.")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(drop())
