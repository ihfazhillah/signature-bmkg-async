import asyncio
import datetime

import aiohttp
import backoff as backoff

BASE_URL = "https://signature.bmkg.go.id/api/signature/impact/public/"


@backoff.on_exception(backoff.expo, aiohttp.ClientConnectionError, max_tries=60)
async def aiohttp_get(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            return await resp.json()



async def get_list_response(date: datetime.datetime):
    date_fmt = "%Y-%m-%dT00:00:00.000Z"
    date_str = date.replace(hour=0, second=0, microsecond=0).strftime(date_fmt)
    print(f"get signature data for {date_str}")
    url_list = f"{BASE_URL}list/{date_str}"
    data = await aiohttp_get(url_list)
    print(f"finished signature data for {date_str}")
    return data


async def get_detail_data(_id: str):
    print(f"start get data for id: {_id}")
    url = f"{BASE_URL}one/{_id}"
    data = await aiohttp_get(url)
    print(f"finish get data for id: {_id}")
    return data


async def main():
    now = datetime.datetime.now()

    tasks = []
    for sub in [-3, -2, -1, 0, 1, 2, 3]:
        day = now + datetime.timedelta(days=sub)

        signature_day = await get_list_response(day)
        for item in signature_day["data"]:
            tasks.append(get_detail_data(item["_id"]))

    await asyncio.gather(*tasks)


asyncio.run(main(), debug=True)

