import psycopg2
from psycopg2.extras import execute_batch
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# PostgreSQL connection details
DB_NAME = "old_db"
DB_USER = "postgres"
DB_PASS = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"

# Connect to PostgreSQL
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
cur = conn.cursor()


def generate_phone():
    # Generate a phone number that fits within 20 characters
    return fake.numerify(text='###-###-####')


def generate_product_name():
    # Generate a product name using words
    return ' '.join(fake.words(nb=3)).capitalize()[:255]


def generate_users(num_users):
    users = [(fake.name(), generate_phone(), fake.date_of_birth(minimum_age=18, maximum_age=90))
             for _ in range(num_users)]
    execute_batch(cur, "INSERT INTO users (name, phone, date_of_birth) VALUES (%s, %s, %s)", users)
    print(f"Generated {num_users} users")


def generate_products(num_products):
    # Get the current maximum ID from the products table
    cur.execute("SELECT MAX(id) FROM products")
    max_id = cur.fetchone()[0] or 0

    products = []
    for i in range(num_products):
        product_id = max_id + i + 1
        name = generate_product_name()
        price = round(random.uniform(10, 1000), 2)
        products.append((product_id, name, price))

    execute_batch(cur, "INSERT INTO products (id, name, price) VALUES (%s, %s, %s)", products)
    conn.commit()
    print(f"Generated {num_products} products")


def get_existing_ids(table_name, id_column):
    cur.execute(f"SELECT {id_column} FROM {table_name}")
    return [row[0] for row in cur.fetchall()]


def generate_orders(num_orders):
    user_ids = get_existing_ids("users", "id")
    product_ids = get_existing_ids("products", "id")

    if not user_ids or not product_ids:
        raise ValueError("No users or products found in the database. Please generate users and products first.")

    orders = []
    for _ in range(num_orders):
        user_id = random.choice(user_ids)
        product_id = random.choice(product_ids)
        amount = random.randint(1, 5)
        price = round(random.uniform(10, 1000), 2)
        discount = round(random.uniform(0, 20), 2)
        final_price = round(price * amount * (1 - discount / 100), 2)
        date = fake.date_time_between(start_date='-1y', end_date='now').replace(minute=0, second=0, microsecond=0)
        orders.append((user_id, product_id, amount, discount, price, final_price, date))

    execute_batch(cur, """
    INSERT INTO orders (user_id, product_id, amount, discount, price, final_price, date)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, orders)
    print(f"Generated {num_orders} orders")


if __name__ == "__main__":
    # Generate sample data
    generate_users(100_000)  # Generate 100 users
    generate_products(120_00)  # Generate 50 products
    generate_orders(900_000)  # Generate 1000 orders

    conn.commit()
    cur.close()
    conn.close()

    print("Data generation complete!")