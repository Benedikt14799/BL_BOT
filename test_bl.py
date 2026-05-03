import asyncio, asyncpg, os, aiohttp
from bs4 import BeautifulSoup
from dotenv import load_dotenv

async def main():
    load_dotenv()
    db_url = os.environ.get('DATABASE_URL')
    conn = await asyncpg.connect(db_url)
    rows = await conn.fetch('SELECT id, linktobl FROM library WHERE status_id IN (2, 7) LIMIT 5')
    await conn.close()
    
    async with aiohttp.ClientSession() as session:
        for row in rows:
            print(f"Testing {row['id']}: {row['linktobl']}")
            async with session.get(row['linktobl']) as resp:
                html = await resp.text()
                soup = BeautifulSoup(html, 'lxml')
                price_elem = soup.find(class_='priceValue')
                if price_elem:
                    print(f"  Found price: {price_elem.text}")
                else:
                    if 'Keine Treffer' in html:
                        print("  DEAD LINK (Keine Treffer)")
                    elif 'Captcha' in html or 'captcha' in html:
                        print("  CAPTCHA")
                    else:
                        print("  UNKNOWN NO PRICE")

asyncio.run(main())
