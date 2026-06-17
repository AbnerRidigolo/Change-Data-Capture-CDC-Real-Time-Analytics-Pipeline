import time
import random
import subprocess

# Force UTF-8 encoding on standard output for Windows CLI compatibility
if sys.platform.startswith('win'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass


# Auto-install dependencies if missing
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("psycopg2-binary not found. Installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2
    from psycopg2.extras import RealDictCursor

# Database connection parameters
DB_PARAMS = {
    "host": "localhost",
    "port": 5433,
    "user": "postgres",
    "password": "postgres",
    "database": "postgres"
}

PRODUCTS = [
    "Laptop Gamer", "Teclado Mecanico", "Monitor 4K", "Mouse Sem Fio", 
    "Headset RGB", "Cadeira Ergonomica", "Placa de Video RTX", "SSD NVMe 1TB"
]

STATUSES = ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"]

NAMES = [
    "Daniel Oliveira", "Mariana Costa", "Rodrigo Santos", "Beatriz Lima", 
    "Felipe Rocha", "Camila Pereira", "Gabriel Martins", "Juliana Souza"
]

def get_connection():
    while True:
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError:
            print("Waiting for PostgreSQL to be ready...")
            time.sleep(2)

def generate_customer(cursor):
    name = random.choice(NAMES)
    email = f"{name.lower().replace(' ', '.')}@{random.choice(['gmail.com', 'yahoo.com', 'outlook.com'])}"
    try:
        cursor.execute(
            "INSERT INTO ecom.customers (name, email) VALUES (%s, %s) RETURNING id, name;",
            (name, email)
        )
        customer = cursor.fetchone()
        print(f"[INSERT] Novo Cliente: {customer['name']} (ID: {customer['id']})")
        return customer['id']
    except psycopg2.errors.UniqueViolation:
        # If email collision happens, retry next time
        return None

def generate_order(cursor, customer_id):
    product = random.choice(PRODUCTS)
    amount = round(random.uniform(50.0, 3000.0), 2)
    cursor.execute(
        "INSERT INTO ecom.orders (customer_id, product, amount, status) VALUES (%s, %s, %s, 'PENDING') RETURNING id, product, amount;",
        (customer_id, product, amount)
    )
    order = cursor.fetchone()
    print(f"[INSERT] Novo Pedido: {order['product']} - R$ {order['amount']} (ID: {order['id']})")
    return order['id']

def update_random_order(cursor):
    cursor.execute("SELECT id, status FROM ecom.orders WHERE status != 'DELIVERED' AND status != 'CANCELLED';")
    orders = cursor.fetchall()
    if not orders:
        return
    
    order = random.choice(orders)
    current_status = order['status']
    
    if current_status == "PENDING":
        new_status = random.choice(["SHIPPED", "CANCELLED"])
    elif current_status == "SHIPPED":
        new_status = "DELIVERED"
    else:
        return

    cursor.execute(
        "UPDATE ecom.orders SET status = %s, updated_at = CURRENT_TIMESTAMP WHERE id = %s RETURNING id, product, status;",
        (new_status, order['id'])
    )
    updated = cursor.fetchone()
    if updated:
         print(f"[UPDATE] Pedido ID {updated['id']} alterado para {updated['status']}")

def delete_random_order(cursor):
    cursor.execute("SELECT id FROM ecom.orders WHERE status = 'CANCELLED' OR status = 'PENDING';")
    orders = cursor.fetchall()
    if not orders:
        return
    
    order_id = random.choice(orders)['id']
    cursor.execute("DELETE FROM ecom.orders WHERE id = %s;", (order_id,))
    print(f"[DELETE] Pedido ID {order_id} foi removido.")

def main():
    print("Iniciando Simulador de Transacoes para CDC...")
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        while True:
            # 1. Fetch existing customers
            cursor.execute("SELECT id FROM ecom.customers;")
            customers = cursor.fetchall()
            
            action = random.random()
            
            if not customers or action < 0.2:
                # Create a new customer first
                generate_customer(cursor)
            elif action < 0.7:
                # Create a new order for an existing customer
                cust_id = random.choice(customers)['id']
                generate_order(cursor, cust_id)
            elif action < 0.9:
                # Update an order status
                update_random_order(cursor)
            else:
                # Delete an order
                delete_random_order(cursor)
                
            time.sleep(random.uniform(1.0, 3.0))
            
    except KeyboardInterrupt:
        print("\nSimulador encerrado.")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
