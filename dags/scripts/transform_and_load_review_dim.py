import pandas as pd
from sqlalchemy import create_engine
import pyodbc

insert_dim_review_table = '''
    INSERT INTO DimReview(product_id, rating_average, reviews_count, count_1_star, percent_1_star, count_2_star, percent_2_star, count_3_star, percent_3_star, count_4_star, percent_4_star, count_5_star, percent_5_star) 
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''


def transform(df_book_reviews):
    df_book_reviews['percent_1_star'] = df_book_reviews['percent_1_star'].map(lambda x: float(x) / 100)
    df_book_reviews['percent_2_star'] = df_book_reviews['percent_2_star'].map(lambda x: float(x) / 100)
    df_book_reviews['percent_3_star'] = df_book_reviews['percent_3_star'].map(lambda x: float(x) / 100)
    df_book_reviews['percent_4_star'] = df_book_reviews['percent_4_star'].map(lambda x: float(x) / 100)
    df_book_reviews['percent_5_star'] = df_book_reviews['percent_5_star'].map(lambda x: float(x) / 100)

    df_review = df_book_reviews[
        ['product_id', 'rating_average', 'reviews_count', 'count_1_star', 'percent_1_star', 'count_2_star',
         'percent_2_star', 'count_3_star', 'percent_3_star', 'count_4_star', 'percent_4_star', 'count_5_star',
         'percent_5_star']]

    return df_review


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

    df_review = transform(
        pd.read_sql("SELECT * FROM staging.book_product_review ORDER BY id ASC", dbConnection, index_col="id"))
    review_list = list(df_review.itertuples(index=False, name=None))

    cnt = 0
    for review in review_list:
        cnt = cnt + 1
        print(f"\n{cnt} / {len(review_list)}:", review[0], review[1])
        try:
            cursor.execute(insert_dim_review_table, review)
            sql_server_conn.commit()
        except:
            print("Error occurred with ", review[0])

    cursor.close()
    sql_server_conn.close()


if __name__ == '__main__':
    main()