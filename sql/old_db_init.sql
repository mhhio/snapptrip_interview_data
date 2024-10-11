-- old_db_init.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    phone VARCHAR(20) NOT NULL,
    date_of_birth TIMESTAMP NOT NULL
);

CREATE TABLE products (
    id INT UNIQUE,
    name VARCHAR(255) NOT NULL,
    price FLOAT NOT NULL
);

CREATE TABLE orders (
    user_id INT REFERENCES users(id),
    product_id INT REFERENCES products(id),
    amount INT NOT NULL,
    discount FLOAT DEFAULT 0,
    price FLOAT NOT NULL,
    final_price FLOAT NOT NULL,
    date TIMESTAMP NOT NULL,
    PRIMARY KEY (user_id, product_id, date)
);