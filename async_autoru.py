import aiohttp
import asyncio
from asyncio import Semaphore
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import json
import random
import brotli
from time import sleep, time


async def fetch(session, url, sem, retries=3):
    for attempt in range(retries):
        async with sem:
            try:
                #await asyncio.sleep(random.uniform(2, 5))
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
                async with session.get(url, headers=headers, cookies=cookies, timeout=aiohttp.ClientTimeout(total=20)) as response:
                    if response.status == 200:
                        return await response.text()

            except aiohttp.ClientError as e:
                print(f"Client error: {e} for URL: {url}")
            except asyncio.TimeoutError:
                print(f"Request timed out for URL: {url}")
            except Exception as e:
                print(f"Unexpected error: {e} for URL: {url}")
            print(f"Retrying ({attempt + 1}/{retries}) for URL: {url}")
    return None

async def get_urls(session, url, hrefs, page_num, sem):
    html = await fetch(session, url, sem)
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        page_a = soup.find_all('a', class_='Link ListingItemTitle__link')
        for i in page_a:
            hrefs.append(i.get('href'))
        print(f"Found {len(page_a)} links on page {page_num}")
    else:
        print(f"No content found for page {page_num}")

async def get_info(session, href, sem):
    html = await fetch(session, href, sem)
    if html:
        soup = BeautifulSoup(html, 'html.parser')
        name = soup.find('h1', class_= 'CardHead__title')
        if name:
            name = name.text
            with open('all.json', 'a') as info:
                json.dump(name, info)
                json.dump('     ', info)
        else:
            pass
        #price = soup.find('div', class_ = 'OfferPriceCaption__price').text()
        #all = soup.find('div', class_ = 'CardInfo__list-MZpc1').text()





async def main():
    async with aiohttp.ClientSession() as session:
        hrefs = []
        tasks = []
        with open('all.json', 'w') as info:
            pass
        sem = Semaphore(5)
        await get_urls(session, f'https://auto.ru/cars/all/?sort=cr_date-desc&page=1', hrefs, '1', sem)
        for href in hrefs:
            tasks.append(asyncio.create_task(get_info(session, href, sem)))
        await asyncio.gather(*tasks)


        print(f"Found {len(hrefs)} links")
        print(set(hrefs))
        print(len(set(hrefs)))


if __name__ == '__main__':
    start_time = time()
    asyncio.run(main())
    print(f"--- {time() - start_time} seconds ---")
