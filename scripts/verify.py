import time
import sys
import subprocess
import urllib.request
import json

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

# Connection configurations
PG_PARAMS = {
    "host": "localhost",
    "port": 5433,
    "user": "postgres",
    "password": "postgres",
    "database": "postgres"
}

CH_URL = "http://localhost:8123/"

def query_clickhouse(sql):
    try:
        # We append default_format=JSON to get JSON response back from ClickHouse HTTP interface
        req = urllib.request.Request(
            f"{CH_URL}?default_format=JSON",
            data=sql.encode('utf-8'),
            headers={
                'X-ClickHouse-User': 'default',
                'X-ClickHouse-Key': 'password'
            }
        )
        with urllib.request.urlopen(req) as response:
            res = json.loads(response.read().decode('utf-8'))
            return res.get('data', [])
    except urllib.error.URLError as e:
        print(f"Error querying ClickHouse: {e}")
        return []

def query_postgres(sql, params=None):
    conn = psycopg2.connect(**PG_PARAMS)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cursor.execute(sql, params)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

def run_verification():
    print("\n" + "="*50)
    print("INICIANDO VERIFICACAO DE CONSISTENCIA DE DADOS (CDC)")
    print("="*50)

    # 1. Row counts in PostgreSQL
    pg_cust_count = query_postgres("SELECT COUNT(*) as count FROM ecom.customers;")[0]['count']
    pg_ord_count = query_postgres("SELECT COUNT(*) as count FROM ecom.orders;")[0]['count']
    pg_total_revenue = query_postgres("SELECT COALESCE(SUM(amount), 0) as total FROM ecom.orders;")[0]['total']

    # 2. Row counts in ClickHouse using FINAL to collapse ReplacingMergeTree versions
    # We filter is_deleted = 0 to exclude deleted records.
    ch_cust_data = query_clickhouse("SELECT count() as count FROM ecom.customers FINAL WHERE is_deleted = 0;")
    ch_cust_count = ch_cust_data[0]['count'] if ch_cust_data else 0

    ch_ord_data = query_clickhouse("SELECT count() as count, sum(amount) as total FROM ecom.orders FINAL WHERE is_deleted = 0;")
    ch_ord_count = ch_ord_data[0]['count'] if ch_ord_data else 0
    ch_total_revenue = ch_ord_data[0]['total'] if ch_ord_data and ch_ord_data[0]['total'] is not None else 0

    # 3. Check deletes captured
    ch_deleted_orders_data = query_clickhouse("SELECT count() as count FROM ecom.orders FINAL WHERE is_deleted = 1;")
    ch_deleted_orders = ch_deleted_orders_data[0]['count'] if ch_deleted_orders_data else 0

    print(f"\n[PostgreSQL] Clientes ativos: {pg_cust_count}")
    print(f"[ClickHouse]  Clientes ativos (FINAL): {ch_cust_count}")
    
    print(f"\n[PostgreSQL] Pedidos ativos: {pg_ord_count}")
    print(f"[ClickHouse]  Pedidos ativos (FINAL): {ch_ord_count}")
    print(f"[ClickHouse]  Pedidos deletados no histórico: {ch_deleted_orders}")

    print(f"\n[PostgreSQL] Faturamento Total: R$ {pg_total_revenue:.2f}")
    print(f"[ClickHouse]  Faturamento Total: R$ {float(ch_total_revenue):.2f}")

    # Check alignment
    cust_match = pg_cust_count == int(ch_cust_count)
    ord_match = pg_ord_count == int(ch_ord_count)
    rev_match = abs(float(pg_total_revenue) - float(ch_total_revenue)) < 0.01

    print("\n" + "-"*50)
    if cust_match and ord_match and rev_match:
        print(" STATUS: SUCESSO! Os dados estao 100% sincronizados.")
    else:
        print(" STATUS: EM SINCRONIZACAO. (Aguardando lag de rede / Debezium processar eventos)")
    print("="*50 + "\n")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--loop":
        print("Rodando verificador de consistência em loop. Pressione Ctrl+C para parar.")
        try:
            while True:
                run_verification()
                time.sleep(5)
        except KeyboardInterrupt:
            print("\nVerificacao encerrada.")
    else:
        run_verification()
