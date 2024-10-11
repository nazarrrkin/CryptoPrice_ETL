from fake_useragent import UserAgent
from time import sleep, time
from datetime import datetime
import re
from confluent_kafka import Producer
import json
import logging
import requests

def get_page(session, url):
    try:
        ua = UserAgent()
        headers = {
    'User-Agent': str(ua.random),
    'authority': 'api.coinmarketcap.com',
    'method': 'GET',
    'path': url[url.find('.com')+len('.com'):],
    'scheme': 'https',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'ru,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Priority': 'u=0, i',
    'Sec-Ch-Ua': '"Not/A)Brand";v="8", "Chromium";v="126", "YaBrowser";v="24.7", "Yowser";v="2.5"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1'
    }
        with session.get(url, headers=headers) as response:
            return response.json()
    except Exception as e:
        logging.error(f"Unexpected error: {e} for URL: {url}")


def get_info(session, url, producer):
    page = get_page(session, url)
    if page:
        try:
            cryptos = page['data']['cryptoCurrencyList']
            date = datetime(*map(int, list(i for i in re.split(r'[-T:]', page['status']['timestamp'][:-5])))).isoformat()
            for val in cryptos:
                data = {}
                data['name'] = val['name']
                data['symbol'] = val['symbol']
                data['circulatingSupply'] = val['circulatingSupply']
                data['price'] = val['quotes'][0]['price']
                data['volume24h'] = val['quotes'][0]['volume24h']
                data['marketCap'] = val['quotes'][0]['marketCap']
                data['percentChange24h'] = val['quotes'][0]['percentChange24h']
                data['date'] = date
                try:
                    producer.produce('creept', json.dumps(data).encode('utf-8'))
                except Exception as e:
                    logging.error(f'An error occured: {e}')
                    continue
        except Exception as e:
            logging.error(f'Data formating error occured: {e}')


def collect_data():
    producer = Producer({'bootstrap.servers': '127.22.224.1:9092',
                         'linger.ms': 50,
                         'batch.size': 100000,
                         'queue.buffering.max.messages': 100000})
    with requests.Session() as session:
        for start in range (1, 9648, 200):
            get_info(session, f'https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?start={start}&limit=200&sortBy=market_cap&sortType=desc&convert=USD&cryptoType=all&tagType=all&audited=false', producer)
    producer.flush()
