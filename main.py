import asyncio
import logging
from math import ceil
import re
import transliterate
from pathlib import Path

from aiohttp import ClientSession
from bs4 import BeautifulSoup
import pandas as pd

from my_logging import get_logger


DOMAIN = 'https://kazan.markertoys.ru'
OUTPUT = Path('markettoys.xlsx')
FILEPATH_TXT = Path('categories.txt')

cookies = {
    'index': '67vr82efnuu2tk8tsnc2r9irc5',
    'MTdivRozn': 'null',
    'mrt_cpg_cat': '90',
}
headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive',
    # 'Cookie': 'MTdivRozn=null; MTdivRozn=null; MTdivRozn=null; index=67vr82efnuu2tk8tsnc2r9irc5; MTdivRozn=null; mrt_cpg_cat=90',
    'Referer': 'https://kazan.markertoys.ru/catalog/1rr156/2rr418/',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Linux"',
}


async def session_request(s: ClientSession,
                          url: str,
                          params: dict = None
                          ) -> BeautifulSoup | bool:
    async with s.get(url, params=params) as r:
        match r.status:
            case 404:
                url = '/'.join(url.split('/')[:-2])
                logging.info(f'{r.status=}! Page was not found. {url}.\n'
                             f'Cut url and trying again... {url}')
                return await session_request(s, url, params=params)
            case 200:
                html = await r.content.read()
            case _:
                logging.error(f'Status code=={r.status}')
                raise ConnectionError

        # if url == 'https://kazan.markertoys.ru/catalog/1rr156/2rr77/':
        #     with open('test.html', 'wb') as f:
        #         f.write(html)
        try:
            return BeautifulSoup(html, 'lxml')
        except Exception as ex:
            logging.error(f'URL=\n\n{ex}\n\n{html}', exc_info=True)
            raise


async def login(s: ClientSession) -> None:
    data = {
        'login': 'asd@dsa.ru',
        'pass': 'asd',
        'city': 'kazan.markertoys.ru'
    }
    async with s.post(DOMAIN + '/authcheckerdiscount', json=data) as r:
        if r.status != 200:
            raise ConnectionError
        logging.info('Logged into account')


async def get_categories(s: ClientSession) -> list[str]:
    soup = await session_request(s, DOMAIN + '/catalog')
    return [DOMAIN + li.a.get('href')
            for ul in soup.find_all('ul', class_='column')
            for li in ul.find_all('li')
            ]


async def get_category_products(s: ClientSession, url: str) -> list[str] | bool:
    products_urls = []
    last_page = None
    page = 1
    params = {
        'limit': '1',
        'cpg': '90',
        'sort': 'newitems'
    }

    while True:
        if last_page:
            params['limit'] = (page-1) * 90 + 1

        if not (soup := await session_request(s, url, params=params)):
            return False

        products_urls += [DOMAIN + table.a.get('href')
                          for table in soup.find_all('table', class_='table_product')
                          ]
        div_pagination = soup.find('div', class_='pagination')

        if not div_pagination or page == last_page:
            return products_urls
        elif not last_page:
            last_page = int(div_pagination.find_all('a', class_='navp')[-1].text)

        logging.info(f'{page=} collected {len(products_urls)}')
        page += 1


async def get_page_data(s: ClientSession, url: str) -> dict:
    soup = await session_request(s, url)

    try:
        data = {
            'url': url,
            'Код': soup.find('b', {'itemprop': 'sku'}).text,
            'Количество': tuple(
                eval(
                    re.search(r'(?<=putGoodToBasket).+(?=; return false)',
                              soup.find('div', text=re.compile('в г. Казань'))
                              .find_previous_sibling('a', class_='plus_ico').get('onclick')
                              ).group(0)
                )
            )[-3]
            if soup.find('div', text=re.compile('в г. Казань')) else 0,
            'Артикул для ОЗОН': ...,
            'Цена поставщика(Оптовая цена)': ceil(
                float(
                    soup.find('div', {'itemprop': 'price'})
                    .find('span', class_='go-big').text.replace(',', '.').replace(' ', '')
                )
            ),
            'Цена расчитанная для озон': ...,
            'Категория': soup.find('meta', {'content': '2'})
                         .find_previous_sibling('span', {'itemprop': 'name'}).text,
            'Тип продукта': soup.find('meta', {'content': '3'})
                         .find_previous_sibling('span', {'itemprop': 'name'}).text,
            'Название': soup.find('ul', class_='breadcrumbs').li.find_next_sibling('b').text,
            'Торговая марка': ...,
            'Страна производитель': ...,
            'Размеры упаковки': ...,
            'Длина мм': ...,
            'Ширина мм': ...,
            'Высота мм': ...,
            'Размер изделия': ...,
            'Функционал': ...,
            'Вес': ...,
            'Упаковка': ...,
            'Материал': ...,
            'Комплектация': ...,
            'Возраст ребенка': ...,
            'Описание': soup.find('div', class_='descr').get_text('\n').replace(
                soup.find('div', class_='descr').find('div', class_='pdTB20').text,
                ''
            ) if soup.find('div', class_='descr') and
                 soup.find('div', class_='descr').find('div', class_='pdTB20')
            else '-',
            'Фото главное': DOMAIN + soup.find('img', {'itemprop': 'image'}).get('src')
            if soup.find('img', {'itemprop': 'image'}) else '-'
        }

        def get_ozon_price(price: int):
            if 0 < price <= 80:
                k = 15
            elif 81 <= price <= 100:
                k = 10
            elif 101 <= price <= 150:
                k = 8
            elif 151 <= price <= 250:
                k = 7
            elif 251 <= price <= 500:
                k = 5
            elif 501 <= price <= 900:
                k = 4
            elif price > 900:
                k = 3
            else:
                raise Exception(f'{price=}, k???')
            return price * k
        data['Цена расчитанная для озон'] = get_ozon_price(data['Цена поставщика(Оптовая цена)'])

        def get_ozon_article(code: str, type_: str, price: int, market: str = 'mark'):
            type_translited = '_'.join(
                transliterate.translit(type_, 'ru', reversed=True).split()
            ).replace('-', '_')
            return f'{code}-{type_translited}-c{price}-{market}'
        data['Артикул для ОЗОН'] = get_ozon_article(data['Код'], data['Тип продукта'],
                                                    data['Цена поставщика(Оптовая цена)']
                                                    )

        for column in ('Торговая марка', 'Страна производитель', 'Размеры упаковки'):
            pattern = f'{column}: '
            for div in soup.find('span', {'itemprop': 'offers'}).find_all('div'):
                if pattern in div.text:
                    data[column] = div.text.replace(pattern, '')
                    break
            else:
                data[column] = '-'

        def size_to_mm(size: str):
            metric = re.sub(r'[\dx]', '', re.search(r'[а-я]+$', size).group(0))
            match metric:
                case 'см':
                    k = 10
                case 'м':
                    k = 1000
                case 'мм':
                    k = 1
                case _:
                    raise Exception(f'Unexpected metric, {size=}')
            return [float(x)*k for x in size.replace(metric, '').split('x')]
        if data['Размеры упаковки'] != '-':
            data['Длина мм'], data['Ширина мм'], data['Высота мм'] = size_to_mm(data['Размеры упаковки'])

        for column in ('Размер изделия', 'Функционал', 'Вес', 'Упаковка',
                       'Материал', 'Комплектация', 'Возраст ребенка'):
            pattern = f'{column}: '
            for line in data['Описание'].splitlines():
                if pattern in line:
                    data[column] = line.replace(pattern, '')
                    break
            else:
                data[column] = '-'

        for k, v in data.items():
            if v is ...:
                data[k] = '-'

        return data
    except Exception:
        logging.error(f'URL={url}\n')
        raise


async def collect_data():
    already_collected_products = list(pd.read_excel(str(OUTPUT))['url']) if OUTPUT.exists() else []
    if FILEPATH_TXT.exists():
        with open(FILEPATH_TXT, 'r') as f:
            already_appended_categories = [x.replace('\n', '') for x in f.readlines()]
    else:
        already_appended_categories = []
    logging.info(f'{already_appended_categories=}')

    async with ClientSession(cookies=cookies, headers=headers) as s:
        await login(s)
        categories_urls = await get_categories(s)

        for category_url in already_appended_categories:
            if category_url in categories_urls:
                categories_urls.remove(category_url)
        logging.info(f'Categories appended: {len(already_appended_categories)}. Need to append: {len(categories_urls)}')
        logging.info(categories_urls)

        for category_url in categories_urls:
            logging.info(f'{categories_urls.index(category_url)+1}/{len(categories_urls)} category: {category_url}')
            products_urls = await get_category_products(s, category_url)
            if products_urls is False:
                continue
            logging.info(f'{len(products_urls)} product urls for category collected: {products_urls}')

            tasks = []
            for product_url in products_urls:
                if product_url in already_collected_products:
                    continue

                tasks.append(
                    asyncio.create_task(
                        get_page_data(s, product_url)
                    )
                )
                if len(tasks) == 15 or product_url == products_urls[-1]:
                    res = [d for d in await asyncio.gather(*tasks)]
                    append_to_xlsx(OUTPUT, res)
                    logging.info(f'Appended {len(tasks)} to xlsx')
                    tasks.clear()

            append_to_txt(FILEPATH_TXT, category_url)


def append_to_xlsx(filepath: Path, data2append: list[dict]) -> None:
    if not filepath.exists():
        pd.DataFrame().to_excel(str(filepath), index=False)

    df = pd.read_excel(str(filepath))
    df = pd.concat([df, pd.DataFrame(data2append)])
    df.to_excel(str(filepath), index=False)


def append_to_txt(filepath: Path, row: str) -> None:
    with open(filepath, 'a') as f:
        f.write(row + '\n')


def main():
    get_logger('main.log')
    asyncio.run(collect_data())


if __name__ == '__main__':
    main()
