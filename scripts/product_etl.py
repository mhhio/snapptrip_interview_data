from pyspark.sql.functions import col, lit, current_timestamp, when, to_date,date_format
import pyspark.sql.functions as F

from spark_utils import *

def main():
    spark = create_spark_session()

    df_source = read_jdbc_table(spark,'products',main_db_properties)

    df_current_product = read_jdbc_table(spark,'DIM_PRODUCT',warehouse_properties)

    # Identify new products
    df_new_products = df_source.join(
        df_current_product.filter(col("is_current") == True),
        df_source.id == df_current_product.product_id,
        "left_anti"
    )

    if df_new_products.count() > 0:
        df_to_insert = df_new_products.select(
            col("id").alias("product_id"),
            col("name").alias("product_name"),
            col("price").alias("current_price"),
            lit(current_timestamp()).alias("valid_from"),
            lit(None).cast("timestamp").alias("valid_to"),
            lit(True).alias("is_current")
        )

        write_jdbc_table(df_to_insert,'DIM_PRODUCT',warehouse_properties)

    # Identify existing products with changes
    df_existing_products = df_source.join(
        df_current_product.filter(col("is_current") == True),
        df_source.id == df_current_product.product_id,
        "inner"
    ).filter(
        (df_source.name != df_current_product.product_name) |
        (df_source.price != df_current_product.current_price)
    )

    if df_existing_products.count() > 0:
        df_existing_products.show(1)
        df_to_update = df_current_product.alias("target").join(
            df_existing_products,
            (col("target.product_id") == df_existing_products.id) &
            (col("target.is_current") == True)
        ).select(
            col("target.product_key"),
            col("target.product_id"),
            col("target.product_name"),
            col("target.current_price"),
            col("target.valid_from"),
            lit(current_timestamp()).alias("valid_to"),
            lit(False).alias("is_current")
        )
        manual_update_table(df_to_update,'DIM_PRODUCT','product_key',warehouse_properties)

        df_current_product = read_jdbc_table(spark, 'DIM_PRODUCT', warehouse_properties)
        # Identify new products
        df_new_products = df_source.join(
            df_current_product.filter(col("is_current") == True),
            df_source.id == df_current_product.product_id,
            "left_anti"
        )
        df_to_insert = df_new_products.select(
            col("id").alias("product_id"),
            col("name").alias("product_name"),
            col("price").alias("current_price"),
            lit(current_timestamp()).alias("valid_from"),
            lit(None).cast("timestamp").alias("valid_to"),
            lit(True).alias("is_current")
        )

        write_jdbc_table(df_to_insert, 'DIM_PRODUCT', warehouse_properties)

    spark.stop()


if __name__ == "__main__":
    main()