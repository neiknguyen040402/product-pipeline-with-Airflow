from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import scripts.sql_statements_create_staging_tables
import scripts.extractProductId
import scripts.extractProductData
import scripts.extractProductReview
import scripts.create_book_fact_table
import scripts.create_book_dimension_table
import scripts.create_category_dimension_table
import scripts.create_review_dimension_table
import scripts.transform_and_load_book_dim
import scripts.transform_and_load_category_dim
import scripts.transform_and_load_review_dim
import scripts.transform_and_load_book_fact

default_args = {
    'owner': 'Kien Nguyen',
    'start_date': days_ago(0),
    'email': ['kiennguyengtglhd@gmail.com'],
    'email_on_failure': False,
    'email_on-retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='data_pipeline',
        default_args=default_args,
        description='Data pipeline to process Tiki\'s product data',
        start_date=datetime(2023, 4, 4),
        schedule_interval='@daily',
        tags=['data-pipeline', 'etl', 'book-product']
) as dag:
    start_operator = DummyOperator(task_id='start_pipeline')

    create_staging_book_product_id_table = PostgresOperator(
        task_id='create_staging_product_id_table',
        postgres_conn_id='postgresql',
        sql=scripts.sql_statements_create_staging_tables.create_staging_book_product_id_table,
    )

    create_staging_book_product_data_table = PostgresOperator(
        task_id='create_staging_product_data_table',
        postgres_conn_id='postgresql',
        sql=scripts.sql_statements_create_staging_tables.create_staging_book_product_data_table,
    )

    create_staging_book_product_review_table = PostgresOperator(
        task_id='create_staging_product_review_table',
        postgres_conn_id='postgresql',
        sql=scripts.sql_statements_create_staging_tables.create_staging_book_product_review_table,
    )

    extract_product_id = PythonOperator(
        task_id='extract_product_id',
        python_callable=scripts.extractProductId.main
    )

    extract_product_data = PythonOperator(
        task_id='extract_product_data',
        python_callable=scripts.extractProductData.main
    )

    extract_product_review = PythonOperator(
        task_id='extract_product_review',
        python_callable=scripts.extractProductReview.main
    )

    create_fact_book_product_table = PythonOperator(
        task_id='create_fact_book_product_table',
        python_callable=scripts.create_book_fact_table.main
    )

    create_dim_book_table = PythonOperator(
        task_id='create_dim_book_table',
        python_callable=scripts.create_book_dimension_table.main
    )

    create_dim_category_table = PythonOperator(
        task_id='create_dim_category_table',
        python_callable=scripts.create_category_dimension_table.main
    )

    create_dim_review_table = PythonOperator(
        task_id='create_dim_review_table',
        python_callable=scripts.create_review_dimension_table.main
    )

    transform_load_dim_book = PythonOperator(
        task_id='transform_load_dim_book',
        python_callable=scripts.transform_and_load_book_dim.main
    )

    transform_load_dim_category = PythonOperator(
        task_id='transform_load_dim_category',
        python_callable=scripts.transform_and_load_category_dim.main
    )

    transform_load_dim_review = PythonOperator(
        task_id='transform_load_dim_review',
        python_callable=scripts.transform_and_load_review_dim.main
    )

    transform_load_fact_book_product = PythonOperator(
        task_id='transform_load_fact_book_product',
        python_callable=scripts.transform_and_load_book_fact.main
    )

    end_operator = DummyOperator(task_id='stop_pipeline')

    start_operator >> [create_staging_book_product_id_table, create_staging_book_product_data_table,
                       create_staging_book_product_review_table] >> extract_product_id
    extract_product_id >> [extract_product_data, extract_product_review] >> create_fact_book_product_table
    create_fact_book_product_table >> [create_dim_book_table, create_dim_category_table, create_dim_review_table]
    create_dim_book_table.set_downstream(transform_load_dim_book)
    create_dim_category_table.set_downstream(transform_load_dim_category)
    create_dim_review_table.set_downstream(transform_load_dim_review)
    [transform_load_dim_book, transform_load_dim_category,
     transform_load_dim_review] >> transform_load_fact_book_product >> end_operator