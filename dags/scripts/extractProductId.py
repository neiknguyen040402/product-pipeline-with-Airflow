import requests
import time
import random
import json
import psycopg2

url = 'https://tiki.vn/api/personalish/v1/blocks/listings'

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
    'Referer': 'https://tiki.vn/nha-sach-tiki/c8322',
    'x-guest-token': 'VwDfQXmkAF7EJZsjrai9YlvUIWM5LRSG',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}

params = {
    'limit': '40',
    'include': 'advertisement',
    'aggregations': '2',
    'trackity_id': 'b2862418-501a-1a34-03cd-2d1899ac0dc5',
    'category': '8322',
    'page': '1',
    'src': 'c8322',
    'urlKey':  'nha-sach-tiki',
}

insert_staging_product_id_table ='INSERT INTO staging.book_product_id VALUES (%s);'


def main():
    response_temp = requests.get(url=url, headers=headers, params=params)
    res = response_temp.json()

    last_page = res['paging']['last_page']

    conn = psycopg2.connect(database="mydb", user="kiennguyen", password="kiennguyen", host="localhost", port="5431")
    cur = conn.cursor()

    count_product = 0
    list_id = []
    for i in range(1, last_page + 1):
        params['page'] = i
        response = requests.get(url=url, headers=headers, params=params)
        if response.status_code == 200:
            try:
                for record in response.json().get('data'):
                    name = record.get('name')
                    if "combo" in name.lower(): continue  # exclude the combo product
                    id = str(record.get('id'))
                    list_id.append(id)
                    count_product = count_product + 1
                print(f'\n\t{i} requests success!')
            except:
                print("Errors occur!!!")
        # time.sleep(random.randrange(1, 2))

    print(f"Last page: {last_page}")
    print(f"Crawled: {count_product} products' id")

    set_id = set(list_id)

    for id in set_id:
        cur.execute(insert_staging_product_id_table, (id,))
        conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()