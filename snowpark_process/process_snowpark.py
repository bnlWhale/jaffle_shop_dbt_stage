import time

from snowflake.snowpark import Session
from datetime import date
from snowflake.snowpark.functions import col

from snowpark_process.all_tools import get_readable_curtime

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


def main(session: Session):
    # create_customer_origin_table(session)
    insert_into_orders(session)


if __name__ == '__main__':
    with Session.builder.configs(conn_config).create() as session:
        import sys

        if len(sys.argv) > 1:
            main(session, *sys.argv[1:])  # type: ignore
        else:
            main(session)  # type: ignore
