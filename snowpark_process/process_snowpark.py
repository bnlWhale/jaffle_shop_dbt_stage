import time

from snowflake.snowpark import Session
from datetime import date
from snowflake.snowpark.functions import col
from snowpark_process.all_tools import get_readable_curtime
from concurrent.futures.thread import ThreadPoolExecutor

conn_config = {
    "account": "KFBGAMM-MJB61345",
    "user": "ducounau",
    "password": "ilike@Bnl91",
    "role": "ACCOUNTADMIN",
    "warehouse": "compute_wh",
    "database": "RAW",
    "schema": "JAFFLE_SHOP"
}


# Invoking Snowpark Session for Establishing Connection

def create_customer_origin_table(session: Session):
    sql_script = ("create or replace table raw.jaffle_shop.customers("
                  "id integer,"
                  "first_name varchar,"
                  "last_name varchar)")
    session.sql(sql_script).collect()
    sql_script = ("copy into raw.jaffle_shop.customers (id, first_name, last_name)"
                  "from 's3://dbt-tutorial-public/jaffle_shop_customers.csv'"
                  "file_format = ("
                  "field_delimiter = ','"
                  "skip_header = 1"
                  ")")
    session.sql(sql_script).collect()
    session.table("raw.jaffle_shop.customers").show()


def insert_into_orders(session: Session):
    df = session.table("raw.jaffle_shop.orders")
    id_base = df.count()
    for step in range(3):
        id = step + id_base
        user_id = step + 1
        order_data = date.today()
        status = "complete"
        _ETL_LOADED_AT = get_readable_curtime(time.time())
        row_df = session.create_dataframe([[id, user_id, order_data, status, _ETL_LOADED_AT]], schema=df.columns)
        row_df.write.mode('append').saveAsTable('raw.jaffle_shop.orders')
    df.filter(col('id') >= id_base).show()
    time.sleep(2)

def create_orders_stream(session):
    _ = session.sql("CREATE STREAM raw.jaffle_shop.order_stream ON TABLE raw.jaffle_shop.orders").collect()


def consume_order_stream(session: Session):
    source = session.table('raw.jaffle_shop.order_stream')
    source.show()
    script = ("insert into raw.jaffle_shop.delivery_management("
              "order_id, user_id, received_at, start_delivery_at, finished_delivery_at)"
              "select id, user_id, current_timestamp(), current_timestamp(), current_timestamp()"
              "from raw.jaffle_shop.order_stream")
    session.sql(script).collect()
    time.sleep(3)


def create_delivery_table(session: Session):
    script = ("CREATE TABLE raw.jaffle_shop.delivery_management( "
              "id integer autoincrement, order_id integer, user_id integer, "
              "received_at timestamp, start_delivery_at timestamp, finished_delivery_at timestamp)")
    session.sql(script).collect()


def main(session: Session):
    # create_customer_origin_table(session)
    # insert_into_orders(session)
    # create_orders_stream(session)
    # create_delivery_table(session)
    # consume_order_stream(session)
    process_with_thread(session)
    pass


def process_with_thread(session:Session):
    tasks = [insert_into_orders, consume_order_stream]
    with ThreadPoolExecutor(max_workers=2) as executor:
        for result in tasks:
            executor.submit(result, session)



if __name__ == '__main__':
    with Session.builder.configs(conn_config).create() as session:
        import sys

        if len(sys.argv) > 1:
            main(session, *sys.argv[1:])  # type: ignore
        else:
            main(session)  # type: ignore
