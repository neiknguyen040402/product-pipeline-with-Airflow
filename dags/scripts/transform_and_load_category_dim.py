import pandas as pd
from sqlalchemy import create_engine
import pyodbc


insert_dim_category_table = '''
    INSERT INTO DimCategory(category_id, category) VALUES(?, ?)

'''


def transform(df_book_products):
    df_book_products['category'] = df_book_products['category'].map(lambda x: str(x or '').strip().capitalize())
    df_category = df_book_products[['category_id', 'category']]
    df_category = df_category.drop_duplicates()
    return df_category


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

    df_category = transform(
        pd.read_sql("SELECT * FROM staging.book_product_data ORDER BY id ASC", dbConnection, index_col="id"))
    category_list = list(df_category.itertuples(index=False, name=None))

    cnt = 0
    for category in category_list:
        cnt = cnt + 1
        print(f"\n{cnt} / {len(category_list)}:", category[0])
        try:
            cursor.execute(insert_dim_category_table, category)
            sql_server_conn.commit()
        except:
            print("Error occurred with ", category[0])

    cursor.close()
    sql_server_conn.close()


if __name__ == '__main__':
    main()