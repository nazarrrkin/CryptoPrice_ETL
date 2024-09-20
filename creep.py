import aiohttp
import asyncio
from fake_useragent import UserAgent
from time import sleep, time
from datetime import datetime
import re
from confluent_kafka import Producer
import json
import logging

async def fetch(session, url):
    try:
        ua = UserAgent()
        headers = {
            'User-Agent': str(ua.random),
            ':path': '/',
            ':scheme': 'https',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'ru,en;q=0.9',
            'Origin': 'https://www.wildberries.ru',
            'Priority': 'u=1, i',
            'Referer': 'https://www.wildberries.ru/',
            'Sec-Ch-Ua': '"Not/A)Brand";v="8", "Chromium";v="126", "YaBrowser";v="24.7", "Yowser";v="2.5"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site',
            'Upgrade-Insecure-Requests': '1'
        }
        cookies = {
            'suid': '6b8013d1a9c23556f2583aa2c64b0528.50b25f9f6eea9b14be11a3823a9a6f52',
            '_csrf_token': '8a740618a469323fcdd992c6e5634f291d1221a520efb7ed',
            'autoruuid': 'g66a9f65b2vghjq9g4q9gb6qrpq4j6pr.d1c1aac5b9ea681e790962f2b94d295d',
            'yandex_login': 'nazarkindaniil',
            'i': 'jSqcP9xdoY8HTBBDtKsGFHyzaJjb0RV757pV9rbqBV1lC60J3MJcrUqWCwZVnAlzC3/BFs9IFJwqbbPGY7ppJvN0ty8=',
            'yandexuid': '7057773591612117260',
            'my': 'YwA=',
            'crookie': 'rwGmwcEZXAHcmWQuadRZSHdOtPWgi5yW1S4ZOKsonki1Zcu8JAPd2DUMt9IXwwj3aWf3spCWMuyjvqd5bKQc1rUb3y0=',
            'cmtchd': 'MTcyMjQxNDY4NjAzNQ==',
            'yaPassportTryAutologin': '1',
            'autoru_sid': '68115904%7C1722414692594.7776000.5B-003rzdakI6K7UsXaqDw.E-HH1P0Mqraa0jjaMkQO3Ry_0FMYaq1Qxo6bS3RrJYs',
            'listing_view_session': '{}',
            'listing_view': '%7B%22output_type%22%3Anull%2C%22version%22%3A1%7D',
            'gids': '213',
            'gradius': '200',
            'los': '1',
            'bltsr': '1',
            'coockoos': '1',
            'fp': 'db9a04abf186e22fdef5a9fc969a6159%7C1722414958338',
            'count-visits': '4',
            'layout-config': '{"screen_height":864,"screen_width":1536,"win_width":919.2000122070312,"win_height":731.2000122070312}',
            'from': 'direct',
            '_yasc': 'jZoSHtv75oQqTQc5KmITHl6K7Iucyg/9EretwzDVlwDUJRdl/G47Vx7dO2bYMLs9',
            'autoru_gdpr': '1',
            'from_lifetime': '1722439258682',
            'autoru_sso_blocked': '1',
            'Session_id': '3:1722439259.5.1.1637172610895:ZhoqXA:5.1.2:1|912879878.0.2|655629437.-1.2.2:31513048|61:10024503.573954.alN1bNSRKPZncIoRjIGIain6dnU',
            'sessar': '1.1192.CiAGMghYGigX0uTzxS6UtX4x_cbVMF_3VwpjzIMm7G0ssw.nN1FlHJy1o8e8eC-S43TRzA-EAmjureVV-iBhMPDit8',
            'ys': 'udn.cDpuYXphcmtpbmRhbmlpbA%3D%3D#c_chck.2547947796',
            'mda2_beacon': '1722439259369',
            'sso_status': 'sso.passport.yandex.ru:synchronized'
        }
        async with session.get(url, headers={'User-Agent':str(ua)}) as response:
            if response.status == 200:
                return await response.json()

    except aiohttp.ClientError as e:
        print(f"Client error: {e} for URL: {url}")
    except asyncio.TimeoutError:
        print(f"Request timed out for URL: {url}")
    except Exception as e:
        print(f"Unexpected error: {e} for URL: {url}")


async def get_info(session, url, producer):
    page = await fetch(session, url)
    if page:
        cryptos = page['data']['cryptoCurrencyList']
        date = datetime(*map(int, list(i for i in re.split(r'[-T:]', page['status']['timestamp'][:-5]))))
        for val in cryptos:
            data = {}
            data['name'] = val['name']
            data['symbol'] = val['symbol']
            data['circulatingSupply'] = val['circulatingSupply']
            data['price'] = val['quotes'][0]['price']
            data['volume24h'] = val['quotes'][0]['volume24h']
            data['marketCap'] = val['quotes'][0]['marketCap']
            data['percentChange24h'] = val['quotes'][0]['percentChange24h']
            try:
                producer.produce('creept', json.dumps(data).encode('utf-8'))
            except Exception as e:
                logging.error(f'An error occured: {e}')
        producer.flush()

async def main():
    producer = Producer({'bootstrap.servers' : 'broker:29092'})
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(get_info(session, f'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?start={start}&limit=200&sortBy=market_cap&sortType=desc&convert=USD&cryptoType=all&tagType=all&audited=false', producer)) for start in range(1,805, 200)]
        await asyncio.gather(*tasks)


if __name__ == '__main__':
    start_time = time()
    asyncio.run(main())
    print(f"--- {time() - start_time} seconds ---")
