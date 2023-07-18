import asyncio
import datetime

import aiohttp
from models import People, engine, Base, Session

URL = 'https://swapi.dev/api/people/'

CHUNK_SIZE = 10

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def get_people(session, people_id):
    result = await session.get(f'{URL}{people_id}')
    return await result.json()

async def get_details(title):
    result_list = []
    for url in title:
        async with aiohttp.ClientSession() as session:
            response = await session.get(url)
        result = await response.json()
        result_list.append(result)

    return result_list

async def get_homeworld(url, session):
    async with session.get(url) as response:
        data = await response.json()

        return data['name']

async def incert_people(people):
    async with aiohttp.ClientSession() as http_session:
        people_list = [People(
            name=item['name'],
            birth_year=item['birth_year'],
            eye_color=item['eye_color'],
            films=', '.join([i['title'] for i in await get_details(item['films'])]),
            gender=item['gender'],
            hair_color=item['hair_color'],
            height=item['height'],
            homeworld=await get_homeworld(item['homeworld'], http_session),
            mass=item['mass'],
            skin_color=item['skin_color'],
            species=', '.join([i['name'] for i in await get_details(item['species'])]),
            starships=', '.join([i['name'] for i in await get_details(item['starships'])]),
            vehicles=', '.join([i['name'] for i in await get_details(item['vehicles'])])
        ) for item in people]

    async with Session() as session:
        session.add_all(people_list)
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    async with aiohttp.ClientSession() as web_session:
        n = 1
        while True:
            coros = [get_people(web_session, i) for i in range(n, n + CHUNK_SIZE)]
            people = await asyncio.gather(*coros)
            people = [item for item in people if not item.get('detail')]
            if not people:
                break
            in_p = incert_people(people)
            task = asyncio.create_task(in_p)
            await task
            n += CHUNK_SIZE

if __name__ == '__main__':
    start = datetime.datetime.now()
    asyncio.run(main())
    print(f'Время выполнения: {datetime.datetime.now() - start}')
