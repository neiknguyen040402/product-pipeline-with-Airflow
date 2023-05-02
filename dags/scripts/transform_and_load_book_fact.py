import pandas as pd
from sqlalchemy import create_engine
import pyodbc


insert_fact_book_product_table = '''
    INSERT INTO FactBookProduct(product_id, category_id, sku, image_url, quantity_sold, price, original_price, discount, discount_rate) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
'''


def transform(df_book_products):
    df_book_products['publication_date'] = pd.to_datetime(df_book_products.publication_date)
    df_book_products['publication_date'] = df_book_products['publication_date'].dt.strftime("%m-%d-%Y")
    df_book_products['publication_date'] = df_book_products['publication_date'].map(lambda x: None if x != x else x)
    df_fact_book_product = df_book_products[
        ['product_id', 'category_id', 'sku', 'image_url', 'quantity_sold', 'price', 'original_price', 'discount',
         'discount_rate']]
    return df_fact_book_product


def main():
    sql_server_conn = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        "Server=DESKTOP-71J6RR7\SQLEXPRESS;"
        "Database=Datawarehouse;"
        "Trusted_Connection=yes;"
    )
    cursor = sql_server_conn.cursor()

    alchemyEngine = create_engine('postgresql+psycopg2://kiennguyen:kiennguyen@localhost:5431/mydb', pool_recycle=3600);
    dbConnection = alchemyEngine.connect();

    df_fact_book_product = transform(
        pd.read_sql("SELECT * FROM staging.book_product_data ORDER BY id ASC", dbConnection, index_col="id"))
    book_product_list = list(df_fact_book_product.itertuples(index=False, name=None))

    cnt = 0
    for book_product in book_product_list:
        # print(book_product)
        cnt = cnt + 1
        print(f"\n{cnt} / {len(book_product_list)}:", book_product[0], book_product[1])
        try:
            cursor.execute(insert_fact_book_product_table, book_product)
            sql_server_conn.commit()
        except:
            print("Error occurred with ", book_product[0])

    print("Done loading fact book product table!")
    cursor.close()
    sql_server_conn.close()


if __name__ == '__main__':
    main()