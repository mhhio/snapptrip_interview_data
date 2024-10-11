from pyspark.sql.functions import col, lit, current_timestamp, when, to_date, date_format
import pyspark.sql.functions as F

from spark_utils import *


def main():
    spark = create_spark_session()

    # Read orders from old database
    orders_df = read_jdbc_table(spark, "orders", main_db_properties)

    # Read dimension tables from warehouse
    dim_date_df = read_jdbc_table(spark, "DIM_DATE", warehouse_properties)
    dim_product_df = read_jdbc_table(spark, "DIM_PRODUCT", warehouse_properties)
    dim_user_df = read_jdbc_table(spark, "DIM_USER", warehouse_properties)

    # Read existing fact sales to identify new orders
    existing_fact_sales = read_jdbc_table(spark, "FACT_SALES", warehouse_properties)

    # Join orders with dimension tables
    joined_df = orders_df.join(
        dim_date_df,
        date_format(to_date(orders_df.date), "yyyyMMdd").cast("int") == dim_date_df.date_key
    ).join(
        dim_product_df,
        (orders_df.product_id == dim_product_df.product_id) &
        (dim_product_df.is_current == True),
        "left"
    ).join(
        dim_user_df,
        orders_df.user_id == dim_user_df.user_id
    )

    # Identify new orders
    new_orders = joined_df.join(
        existing_fact_sales,
        (joined_df.date_key == existing_fact_sales.date_key) &
        (joined_df.product_key == existing_fact_sales.product_key) &
        (joined_df.user_key == existing_fact_sales.user_key),
        "left_anti"
    )

    print(f'total {new_orders.count()} new orders should be inserted')

    # Create fact_sales dataframe for new orders
    new_fact_sales_df = new_orders.select(
        col("date_key"),
        col("product_key"),
        col("user_key"),
        col("amount"),
        col("price"),
        col("discount"),
        col("final_price")
    )

    if new_fact_sales_df.count() > 0:
        write_jdbc_table(new_fact_sales_df,"FACT_SALES",warehouse_properties)

    spark.stop()


if __name__ == '__main__':
    main()
