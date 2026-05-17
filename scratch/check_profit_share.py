import asyncio
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from database import DatabaseManager
from dotenv import load_dotenv
from decimal import Decimal

async def simulate_profit_shares():
    load_dotenv()
    db_url = os.getenv('DATABASE_URL')
    pool = await DatabaseManager.create_pool(db_url)
    
    test_shares = [10, 20, 30, 35, 40, 50]
    ebay_min_discount = 5
    
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, start_price, gewinn_real 
            FROM library 
            WHERE status_id = 1 AND start_price > 0 AND gewinn_real > 0
        """)
        
        if not rows:
            print("Keine passenden Artikel gefunden.")
            return

        total_items = len(rows)
        print(f"Simulation fuer {total_items} Artikel...")
        print("-" * 50)
        print(f"{'Gewinn-Anteil':<15} | {'Erfolgsquote (eBay OK?)'}")
        print("-" * 50)
        
        for share in test_shares:
            success_count = 0
            for r in rows:
                price = Decimal(str(r['start_price']))
                profit = Decimal(str(r['gewinn_real']))
                
                discount_amount = profit * (Decimal(str(share)) / 100)
                discount_percent = (discount_amount / price) * 100
                
                if discount_percent >= ebay_min_discount:
                    success_count += 1
            
            quota = (success_count / total_items) * 100
            print(f"{share:>12}%      | {quota:>6.1f}% ({success_count}/{total_items})")

    await pool.close()

if __name__ == "__main__":
    asyncio.run(simulate_profit_shares())
