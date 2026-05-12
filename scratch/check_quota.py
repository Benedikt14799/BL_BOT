import asyncio
import aiohttp
import os
import sys
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from ebay_analytics import has_sufficient_quota

async def run():
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env"))
    async with aiohttp.ClientSession() as s:
        res = await has_sufficient_quota(s)
        print("Quota Check Result:", res)

if __name__ == "__main__":
    asyncio.run(run())
