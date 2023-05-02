import pyodbc

create_dim_book_table = '''
    CREATE TABLE DimBook (
        product_id NVARCHAR(20) NOT NULL PRIMARY KEY,
        name NVARCHAR(1000),
        author NVARCHAR(1000),
        publisher NVARCHAR(1000),
        manufacturer NVARCHAR(1000),
        number_of_pages INT,
        translator NVARCHAR(1000),
        publication_date DATE,
        book_cover NVARCHAR(20),
        width FLOAT,
        height FLOAT
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

    cursor.execute(create_dim_book_table)

    sql_server_conn.commit()

    cursor.close()
    sql_server_conn.close()

if __name__ == '__main__':
    main()



