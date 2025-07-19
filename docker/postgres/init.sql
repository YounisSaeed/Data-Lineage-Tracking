-- Create database if not exists with error handling
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'data_catalog') THEN
        CREATE DATABASE data_catalog;
        RAISE NOTICE 'Database data_catalog created successfully';
    ELSE
        RAISE NOTICE 'Database data_catalog already exists';
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Failed to create database: %', SQLERRM;
END
$$;

-- Connect to the database with error handling
\c data_catalog


    -- Create schema change log table
    CREATE TABLE IF NOT EXISTS schema_change_log (
        log_id SERIAL PRIMARY KEY,
        table_name VARCHAR(255) NOT NULL,
        change_type VARCHAR(50) NOT NULL,
        change_details JSONB NOT NULL,
        changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        applied BOOLEAN DEFAULT FALSE,
        applied_at TIMESTAMP
    );
    
    -- Create table snapshots
    CREATE TABLE IF NOT EXISTS table_snapshots (
        snapshot_id SERIAL PRIMARY KEY,
        table_name VARCHAR(255) NOT NULL,
        snapshot_data JSONB NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create customers table
    CREATE TABLE IF NOT EXISTS customers (
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR(20) DEFAULT 'active'
    );
    
    -- Create products table
    CREATE TABLE IF NOT EXISTS products (
        product_id SERIAL PRIMARY KEY,
        product_name VARCHAR(100) NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        category VARCHAR(50),
        stock_quantity INTEGER DEFAULT 0
    );
    
    -- Create orders table
    CREATE TABLE IF NOT EXISTS orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INTEGER REFERENCES customers(customer_id),
        order_date DATE NOT NULL DEFAULT CURRENT_DATE,
        total_amount DECIMAL(10,2) NOT NULL,
        status VARCHAR(20) DEFAULT 'pending'
    );
    


        INSERT INTO customers (first_name, last_name, email) VALUES
        ('Younis', 'Zidan', 'Younis.Zidan@example.com'),
        ('ahmed', 'Moahmed', 'ahmed.Moahmed@example.com'),
        ('Yehia', 'Ibrahim', 'Yehia.Ibrahim@example.com');

        INSERT INTO products (product_name, price, category, stock_quantity) VALUES
        ('Laptop', 999.99, 'Electronics', 50),
        ('Smartphone', 699.99, 'Electronics', 100),
        ('Desk Chair', 149.99, 'Furniture', 30);

        INSERT INTO orders (customer_id, order_date, total_amount) VALUES
        (1, '2023-01-15', 999.99),
        (2, '2023-01-16', 849.98),
        (3, '2023-01-17', 149.99);
        


-- Create view
CREATE OR REPLACE VIEW customer_orders AS
SELECT c.customer_id, c.first_name, c.last_name, 
       o.order_id, o.order_date, o.total_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id;

