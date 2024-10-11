from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, to_date,date_format,struct, to_json
import pyspark.sql.functions as F
from psycopg2 import extras
import psycopg2


main_db_properties = {
        "url": "jdbc:postgresql://old_db:5432/old_db",
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

warehouse_properties = {
    "url": "jdbc:postgresql://warehouse_db:5432/warehouse_db",
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


def create_spark_session():
    return SparkSession.builder \
        .appName("WarehouseETL") \
        .config("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar") \
        .getOrCreate()


def read_jdbc_table(spark, table_name, connection_properties):
    return spark.read.jdbc(
        url=connection_properties['url'],
        table=table_name,
        properties=connection_properties
    )


def write_jdbc_table(df, table_name, connection_properties, mode="append"):
    print(f'Total {df.count()} records inserted in {table_name}')
    df.write.jdbc(
        url=connection_properties['url'],
        table=table_name,
        mode=mode,
        properties=connection_properties
    )


def manual_update_table(df, table_name, key_column, db_properties):
    # Convert DataFrame to a list of dictionaries
    rows = df.select(to_json(struct([col(c) for c in df.columns])).alias("json")) \
        .collect()
    data = [eval(row.json) for row in rows]

    # Construct the SQL UPDATE statement
    update_columns = [f"{col} = %({col})s" for col in df.columns if col != key_column]
    update_sql = f"""
    UPDATE {table_name}
    SET {', '.join(update_columns)}
    WHERE {key_column} = %({key_column})s
    """

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host=db_properties['host'],
        database=db_properties['database'],
        user=db_properties['user'],
        password=db_properties['password']
    )

    try:
        with conn.cursor() as cur:
            # Use execute_batch for efficient batch updates
            extras.execute_batch(cur, update_sql, data)
        conn.commit()
        print(f'Total {df.count()} records updated in {table_name}')
    except Exception as e:
        conn.rollback()
        print(f"Error updating {table_name}: {str(e)}")
    finally:
        conn.close()