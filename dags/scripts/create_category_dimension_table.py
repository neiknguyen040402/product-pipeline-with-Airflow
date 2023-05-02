import pyodbc

create_dim_category_table = '''
    CREATE TABLE DimCategory (
        category_id INT NOT NULL, 
        category NVARCHAR(1000),
        PRIMARY KEY(category_id)
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

    cursor.execute(create_dim_category_table)

    sql_server_conn.commit()

    cursor.close()
    sql_server_conn.close()

if __name__ == '__main__':
    main()