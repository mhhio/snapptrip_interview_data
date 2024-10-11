-- warehouse_db_init.sql

-- Create the dim_date table with hourly granularity
CREATE TABLE DIM_DATE (
    date_key INT PRIMARY KEY,
    epoch BIGINT NOT NULL,
    date_actual DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE INDEX dim_date_date_actual_idx ON DIM_DATE(date_key);


-- -- Function to generate dates
CREATE OR REPLACE FUNCTION generate_dates(start_date DATE, end_date DATE)
RETURNS TABLE (
    date_key INT,
    epoch BIGINT,
    date_actual DATE,
    year INT,
    quarter INT,
    month INT,
    day INT,
    day_of_week INT,
    day_name TEXT,
    month_name TEXT,
    is_weekend BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        TO_CHAR(date_series, 'YYYYMMDD')::INT AS date_key,
        EXTRACT(EPOCH FROM date_series)::BIGINT AS epoch,
        date_series::DATE AS date_actual,
        EXTRACT(YEAR FROM date_series)::INT AS year,
        EXTRACT(QUARTER FROM date_series)::INT AS quarter,
        EXTRACT(MONTH FROM date_series)::INT AS month,
        EXTRACT(DAY FROM date_series)::INT AS day,
        EXTRACT(ISODOW FROM date_series)::INT AS day_of_week,
        TO_CHAR(date_series, 'Dy') AS day_name,
        TO_CHAR(date_series, 'Mon') AS month_name,
        CASE WHEN EXTRACT(ISODOW FROM date_series) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
    FROM generate_series(start_date, end_date, '1 day'::INTERVAL) AS date_series;
END;
$$ LANGUAGE plpgsql;

-- Insert data into DIM_DATE table
INSERT INTO DIM_DATE (
    date_key,
    epoch,
    date_actual,
    year,
    quarter,
    month,
    day,
    day_of_week,
    day_name,
    month_name,
    is_weekend
)
SELECT * FROM generate_dates('2020-01-01'::DATE, '2030-12-31'::DATE);

-- Create product dimension table
CREATE TABLE DIM_PRODUCT (
    product_key SERIAL PRIMARY KEY,
    product_id INT NOT NULL ,
    product_name VARCHAR(255) NOT NULL ,
    current_price FLOAT NOT NULL ,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL
);



CREATE TABLE DIM_USER (
    user_key SERIAL PRIMARY KEY,
    user_id INT NOT NULL ,
    name VARCHAR(255) NOT NULL ,
    phone VARCHAR(50) NOT NULL ,
    date_of_birth DATE NOT NULL
);



CREATE TABLE FACT_SALES (
    sale_key SERIAL PRIMARY KEY,
    date_key INT NOT NULL ,
    product_key INT NOT NULL ,
    user_key INT NOT NULL ,
    amount INT NOT NULL ,
    price FLOAT NOT NULL ,
    discount FLOAT NOT NULL ,
    final_price FLOAT NOT NULL ,
    FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key),
    FOREIGN KEY (product_key) REFERENCES DIM_PRODUCT(product_key),
    FOREIGN KEY (user_key) REFERENCES DIM_USER(user_key)
);

-- Index to improve query performance for products
CREATE INDEX idx_product_current ON DIM_PRODUCT (product_id, is_current);
