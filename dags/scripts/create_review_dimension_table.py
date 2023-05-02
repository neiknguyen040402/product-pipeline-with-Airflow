import pyodbc

create_dim_review_table = '''
    CREATE TABLE DimReview (
        id INT IDENTITY(1,1) PRIMARY KEY,
        product_id NVARCHAR(20),
        rating_average REAL,
        reviews_count INT,
        count_1_star INT,
        percent_1_star REAL,
        count_2_star INT,
        percent_2_star REAL,
        count_3_star INT,
        percent_3_star REAL,
        count_4_star INT,
        percent_4_star REAL,
        count_5_star INT,
        percent_5_star REAL
    );

'''


def main():
    sql_server_conn = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        "Server=DESKTOP-71J6RR7\SQLEXPRESS;"
        "Database=Datawarehouse;"
        "Trusted_Connection=yes;"
    )

    cursor = sql_server_conn.cursor()

    cursor.execute(create_dim_review_table)

    sql_server_conn.commit()

    cursor.close()
    sql_server_conn.close()

if __name__ == '__main__':
    main()