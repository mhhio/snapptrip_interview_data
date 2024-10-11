from pyspark.sql.functions import col, lit, current_timestamp, when, to_date,date_format
import pyspark.sql.functions as F

from spark_utils import *


def main():
    spark = create_spark_session()

    # Read users from old database
    users_df = read_jdbc_table(spark, "users", main_db_properties)

    # Read current users from warehouse
    current_users_df = read_jdbc_table(spark, "DIM_USER", warehouse_properties)

    # Transform the source data
    df_transformed = users_df.select(
        col("id").alias("user_id"),
        col("name"),
        col("phone"),
        to_date(col("date_of_birth")).alias("date_of_birth")
    )

    # Perform the upsert operation
    df_to_insert = df_transformed.join(current_users_df, df_transformed.user_id == current_users_df.user_id, "left_anti")
    if df_to_insert.count() > 0:
        write_jdbc_table(df_to_insert,'DIM_USER', warehouse_properties)

    spark.stop()


if __name__ == '__main__':
    main()