import asyncio
import aiohttp
import os
from dotenv import load_dotenv
load_dotenv()
from ebay_token_manager import get_token

async def main():
    token = get_token()
    print('Token:', token[:10]+'...')
    base_url = os.environ.get('EBAY_BASE_URL', 'https://api.sandbox.ebay.com')
    print('Base URL:', base_url)
    async with aiohttp.ClientSession() as s:
        async with s.get(base_url+'/sell/inventory/v1/inventory_item?limit=1', headers={'Authorization': 'Bearer '+token}) as r:
            print(r.status, await r.text())

asyncio.run(main())
