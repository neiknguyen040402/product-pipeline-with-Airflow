import pyodbc

create_fact_book_product_table = '''
    CREATE TABLE FactBookProduct (
        id INT IDENTITY(1,1) PRIMARY KEY,
        product_id NVARCHAR(20), --REFERENCES DIMBOOK(product_id),
        category_id INT, --REFERENCES DIMCATEGORY(category_id),
        sku NVARCHAR(50),
        image_url NVARCHAR(1000),
        quantity_sold INT,
        price FLOAT,
        original_price FLOAT,
        discount FLOAT,
        discount_rate FLOAT
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

    cursor.execute(create_fact_book_product_table)

    sql_server_conn.commit()

    cursor.close()
    sql_server_conn.close()

if __name__ == '__main__':
    main()
