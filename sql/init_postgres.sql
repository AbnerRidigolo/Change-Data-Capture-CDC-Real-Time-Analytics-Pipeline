-- Create schema
CREATE SCHEMA IF NOT EXISTS ecom;

-- Create customers table
CREATE TABLE IF NOT EXISTS ecom.customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create orders table
CREATE TABLE IF NOT EXISTS ecom.orders (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES ecom.customers(id) ON DELETE CASCADE,
    product VARCHAR(255) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Set replica identity to FULL for Debezium to capture full old state on UPDATE/DELETE
ALTER TABLE ecom.customers REPLICA IDENTITY FULL;
ALTER TABLE ecom.orders REPLICA IDENTITY FULL;

-- Insert initial mock data
INSERT INTO ecom.customers (name, email) VALUES
('Abner Ridigolo', 'abner@example.com'),
('Ana Silva', 'ana.silva@example.com'),
('Carlos Souza', 'carlos.souza@example.com');

INSERT INTO ecom.orders (customer_id, product, amount, status) VALUES
(1, 'Laptop Gamer', 1200.00, 'PENDING'),
(2, 'Teclado Mecanico', 150.00, 'SHIPPED'),
(3, 'Monitor 4K', 450.00, 'DELIVERED');
