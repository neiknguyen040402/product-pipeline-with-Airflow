import pandas as pd
from sqlalchemy import create_engine
import pyodbc



insert_dim_book_table = '''
    INSERT INTO DimBook(product_id, name, author, publisher, manufacturer, number_of_pages, translator, publication_date, book_cover, width, height) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''


def transform(df_book_products):
    df_book_products['number_of_pages'] = df_book_products['number_of_pages'].map(lambda x: int(str(x or '0')))
    df_book_products['discount_rate'] = df_book_products['discount_rate'].map(lambda x: float(x) / 100)
    df_book_products['author'] = df_book_products['author'].map(lambda x: str(x or 'Unknown').strip().title())
    df_book_products['publisher'] = df_book_products['publisher'].map(lambda x: str(x or 'Unknown').strip().title())
    df_book_products['manufacturer'] = df_book_products['manufacturer'].map(
        lambda x: str(x or 'Unknown').strip().title())
    df_book_products['translator'] = df_book_products['translator'].map(lambda x: str(x or 'Unknown').strip().title())
    df_book_products['book_cover'] = df_book_products['book_cover'].map(
        lambda x: str(x or 'Unknown').strip().capitalize())

    df_book_products['publication_date'] = pd.to_datetime(df_book_products.publication_date)
    df_book_products['publication_date'] = df_book_products['publication_date'].dt.strftime("%m-%d-%Y")
    df_book_products['publication_date'] = df_book_products['publication_date'].map(lambda x: None if x != x else x)

    df_book = df_book_products[
        ['product_id', 'name', 'author', 'publisher', 'manufacturer', 'number_of_pages', 'translator',
         'publication_date', 'book_cover', 'width', 'height']]
    return df_book


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

    df_book = transform(
        pd.read_sql("SELECT * FROM staging.book_product_data ORDER BY id ASC", dbConnection, index_col="id"))
    book_list = list(df_book.itertuples(index=False, name=None))

    cnt = 0
    for book in book_list:
        cnt = cnt + 1
        print(f"\n{cnt} / {len(book_list)}:", book[0], book[1])
        try:
            cursor.execute(insert_dim_book_table, book)
            sql_server_conn.commit()
        except:
            print("Error occurred with ", book[0])


    cursor.close()
    sql_server_conn.close()


if __name__ == '__main__':
    main()