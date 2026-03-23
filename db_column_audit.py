import asyncio
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

async def audit_columns():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        # 1. Alle Spaltennamen holen
        cols = await conn.fetch("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'library'
        """)
        column_names = [c['column_name'] for c in cols]
        
        print(f"--- DB Audit: Tabelle 'library' ---")
        print(f"Gefundene Spalten: {len(column_names)}")
        
        results = []
        for col in column_names:
            # Prüfen, wie viele Zeilen in dieser Spalte NICHT NULL sind
            count = await conn.fetchval(f'SELECT count(*) FROM library WHERE "{col}" IS NOT NULL')
            results.append((col, count))
            
        # Sortieren nach Füllgrad
        results.sort(key=lambda x: x[1], reverse=True)
        
        print("\nSpalten-Nutzung (Befüllte Zeilen):")
        for col, count in results:
            status = "✅" if count > 0 else "❌ (Nie befüllt)"
            print(f"{status} {col:25} : {count}")
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(audit_columns())
