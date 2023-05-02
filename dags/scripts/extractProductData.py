import psycopg2
from sqlalchemy import create_engine
import re
import pandas as pd
import requests
import time
import random
import json

url = 'https://tiki.vn/api/v2/products/{}'

cookies = {
    '_trackity': 'b2862418-501a-1a34-03cd-2d1899ac0dc5',
    '_ga': 'GA1.2.391285347.1673445776',
    'TKSESSID': 'fdc677cc1ff0f3efdc4c2337c103cb56',
    'TOKENS': '{%22access_token%22:%22VwDfQXmkAF7EJZsjrai9YlvUIWM5LRSG%22}',
    'OTZ': '7006623_28_28__28_',
    'delivery_zone': 'Vk4wMzQwMjQwMTM=',
    '_gid': 'GA1.2.1529061673.1682689832',
    'tiki_client_id': '391285347.1673445776',
    'TIKI_RECOMMENDATION': '173fbb0e6f8bd6f3bbe8ffd3c49235a9',
    '_gat': '1'
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi,en-US;q=0.9,en;q=0.8',
    'Referer': 'https://tiki.vn/dan-ong-sao-hoa-dan-ba-sao-kim-p10005245.html?itm_campaign=CTP_YPD_TKA_PLA_UNK_ALL_UNK_UNK_UNK_UNK_X.233537_Y.1815857_Z.3681714_CN.dan-ong-sao-02%2F04%2F2023&itm_medium=CPC&itm_source=tiki-ads&spid=160051550',
    'x-guest-token': 'VwDfQXmkAF7EJZsjrai9YlvUIWM5LRSG',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}

params = (
    ('platform', 'web'),
    ('spid', '160051550')
)

insert_staging_table = '''
    INSERT INTO staging.book_product_data VALUES (
        DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) ON CONFLICT ON CONSTRAINT product_id_unique DO NOTHING;
'''


def get_attr_value(attributes, field):
    ls = [attr.get('value') for attr in attributes if attr.get('code') == field]
    return ls[0] if len(ls) > 0 else None


def parser_product(json):
    product_id = json.get('id')
    sku = json.get('sku')
    name = json.get('name')

    price = json.get('price')
    original_price = json.get('original_price')
    discount = json.get('discount')
    discount_rate = json.get('discount_rate')

    image_url = json.get('images')[0].get('base_url')

    author = json.get('authors')
    author = author[0].get('name') if author != None else None

    quantity_sold = json.get('quantity_sold').get('value') if json.get('quantity_sold') != None else 0

    attributes = json.get('specifications')[0].get('attributes')

    publisher = get_attr_value(attributes, 'publisher_vn')
    manufacturer = get_attr_value(attributes, 'manufacturer')

    pages = get_attr_value(attributes, 'number_of_page')
    number_of_pages = int(pages) if pages else 0
    translator = get_attr_value(attributes, 'dich_gia')
    publication_date = get_attr_value(attributes, 'publication_date')
    book_cover = get_attr_value(attributes, 'book_cover')

    tmp_dim = get_attr_value(attributes, 'dimensions')
    width, height = 0, 0
    if tmp_dim:
        dimensions = re.findall(r'\d+\.\d+|\d+', tmp_dim)
        if len(dimensions) == 2:
            width = float(dimensions[0])
            height = float(dimensions[1])

    category = json.get('breadcrumbs')[2].get('name')
    category_id = json.get('breadcrumbs')[2].get('category_id')

    values = (product_id, name, sku, price, original_price, discount, discount_rate, image_url, author, quantity_sold,
              publisher, manufacturer, number_of_pages, translator, publication_date, book_cover, width, height,
              category, category_id)
    return values


def main():
    alchemyEngine = create_engine('postgresql+psycopg2://kiennguyen:kiennguyen@localhost:5431/mydb', pool_recycle=3600);
    dbConnection = alchemyEngine.connect();

    conn = psycopg2.connect(database="mydb", user="kiennguyen", password="kiennguyen", host="localhost", port="5431")
    cur = conn.cursor()

    df_book_product_id = pd.read_sql("SELECT * FROM staging.book_product_id", dbConnection)
    product_id_list = df_book_product_id.product_id.to_list()

    cnt = 0
    count_product = 0
    for pid in product_id_list:
        cnt = cnt + 1
        print(f"\n{cnt} / {len(product_id_list)}: ")
        response = requests.get(url=url.format(pid), headers=headers, params=params, cookies=cookies)
        print('Crawl data for product {}'.format(pid))
        if response.status_code == 200:
            try:
                values = parser_product(response.json())
                cur.execute(insert_staging_table, values)
                count_product = count_product + 1
                print('Success!!!')
            except:
                print("Errors occur!!!")

        time.sleep(random.randrange(1, 3))

    print(f"Crawled: {count_product} products' data")

    conn.commit()

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()