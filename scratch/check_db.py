import asyncio
import asyncpg
import os
from dotenv import load_dotenv

async def main():
    load_dotenv()
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("DATABASE_URL not found")
        return
    conn = await asyncpg.connect(db_url)
    count = await conn.fetchval("SELECT count(id) FROM library WHERE ebay_listed = TRUE")
    print(f"Total listed: {count}")
    
    # Check for recent activity
    recent = await conn.fetchval("SELECT count(id) FROM library WHERE last_checked > NOW() - INTERVAL '1 hour'")
    print(f"Recently checked (last hour): {recent}")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
