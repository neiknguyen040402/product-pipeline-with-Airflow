import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import requests
import time
import random

url = 'https://tiki.vn/api/v2/reviews'

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

params = {
    'product_id': '10005245',
    'sort': 'score|desc,id|desc,stars|all',
    'page': '1',
    'limit': '10',
    'include': 'comments,contribute_info,attribute_vote_summary'
}


def parse_reviews(json, pid):
    product_id = pid

    rating_average = json.get('rating_average')
    reviews_count = json.get('reviews_count')

    count_1_star = json.get('stars').get('1').get('count')
    percent_1_star = json.get('stars').get('1').get('percent')

    count_2_star = json.get('stars').get('2').get('count')
    percent_2_star = json.get('stars').get('2').get('percent')

    count_3_star = json.get('stars').get('3').get('count')
    percent_3_star = json.get('stars').get('3').get('percent')

    count_4_star = json.get('stars').get('4').get('count')
    percent_4_star = json.get('stars').get('4').get('percent')

    count_5_star = json.get('stars').get('5').get('count')
    percent_5_star = json.get('stars').get('5').get('percent')

    values = (product_id, rating_average, reviews_count, count_1_star, percent_1_star, count_2_star, percent_2_star,
              count_3_star, percent_3_star, count_4_star, percent_4_star, count_5_star, percent_5_star)

    return values


insert_staging_table = '''
    INSERT INTO staging.book_product_review VALUES (
        DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    );
'''


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
        params['product_id'] = pid
        print(f"\n{cnt} / {len(product_id_list)}: ")
        print('Crawl reviews for product {}'.format(pid))
        response = requests.get(url=url, headers=headers, params=params)
        if response.status_code == 200:
            try:
                values = parse_reviews(response.json(), pid)
                cur.execute(insert_staging_table, values)
                count_product = count_product + 1
                print("Success!")
            except:
                print("Errors occur!!!", response)
        time.sleep(random.randrange(1, 2))
    print(f"Crawled: {count_product} products' data")

    conn.commit()

    cur.close()
    conn.close()


if __name__ == '__main__':
    main()