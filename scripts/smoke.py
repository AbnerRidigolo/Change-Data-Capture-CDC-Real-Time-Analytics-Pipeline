"""Exercise PostgreSQL → Debezium → Kafka → ClickHouse on an isolated demo stack."""

import argparse
from decimal import Decimal
import json
from math import ceil
from pathlib import Path
import time
from urllib.error import URLError
from urllib.request import Request, urlopen
from uuid import uuid4

MASK = "********"
OPERATIONS = {"c", "u", "d"}
ROOT = Path(__file__).resolve().parents[1]
CONNECT = "http://127.0.0.1:8083"


def order_matches(row, customer_id, product, amount, status, deleted):
    return (
        row is not None
        and row["customer_id"] == customer_id
        and row["product"] == product
        and Decimal(str(row["amount"])) == Decimal(amount)
        and row["status"] == status
        and row["is_deleted"] == deleted
    )


def assert_masked_customer_events(events, customer_id):
    observed = set()
    for event in events:
        records = [event.get(side) for side in ("before", "after") if event.get(side) is not None]
        if not any(record.get("id") == customer_id for record in records):
            continue
        if any(record.get("email") != MASK for record in records):
            raise AssertionError(f"Customer {customer_id} contains an unmasked email in Kafka.")
        observed.add(event["op"])
    missing = OPERATIONS - observed
    if missing:
        raise AssertionError(f"Customer {customer_id} is missing Kafka operations: {sorted(missing)}.")


def latency_summary(samples):
    ordered = sorted(samples)
    return {
        "samples": len(ordered),
        "p50_ms": round(ordered[ceil(0.50 * len(ordered)) - 1], 2),
        "p95_ms": round(ordered[ceil(0.95 * len(ordered)) - 1], 2),
    }


def wait_for(description, probe, accepts, timeout):
    deadline = time.monotonic() + timeout
    while True:
        try:
            value = probe()
            if accepts(value):
                return value
        except URLError:
            # Containers can accept TCP before their HTTP endpoints are ready.
            pass
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"{description} did not converge within {timeout:g}s.")
        time.sleep(min(0.1, remaining))


def connect_api(path, method="GET", data=None):
    request = Request(
        f"{CONNECT}{path}", method=method,
        data=json.dumps(data).encode() if data is not None else None,
        headers={"Content-Type": "application/json"},
    )
    with urlopen(request, timeout=4) as response:
        return json.load(response)


def start_connector(timeout):
    wait_for("Kafka Connect", lambda: connect_api("/connectors"), lambda _: True, timeout)
    registration = json.loads((ROOT / "debezium/register_connector.json").read_text())
    name = registration["name"]
    connect_api(f"/connectors/{name}/config", "PUT", registration["config"])

    def running(status):
        if status["connector"]["state"] == "FAILED" or any(
            task["state"] == "FAILED" for task in status["tasks"]
        ):
            raise RuntimeError("Debezium connector or task failed; inspect Kafka Connect logs.")
        return bool(status["tasks"]) and all(task["state"] == "RUNNING" for task in status["tasks"])

    wait_for("Debezium task", lambda: connect_api(f"/connectors/{name}/status"), running, timeout)


def measure_round(connection, query_clickhouse, timeout):
    def write(sql, values, returning=False):
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, values)
                result = cursor.fetchone()[0] if returning else None
        return result, time.monotonic()

    def verify(table, identifier, columns, accepts, started, operation):
        def read_row():
            rows = query_clickhouse(
                f"SELECT {columns} FROM ecom.{table} FINAL WHERE id = {int(identifier)}",
                strict=True,
            )
            return rows[0] if rows else None

        wait_for(f"{table}/{identifier} {operation}", read_row, accepts, timeout)
        return (time.monotonic() - started) * 1000

    def check_customer(identifier, name, deleted, since, operation):
        return verify(
            "customers", identifier, "name, email, is_deleted",
            lambda row: row == {"name": name, "email": MASK, "is_deleted": deleted},
            since, operation,
        )

    def check_order(identifier, customer, status, deleted, since, operation, amount="9.50"):
        return verify(
            "orders", identifier, "customer_id, product, toString(amount) AS amount, status, is_deleted",
            lambda row: order_matches(row, customer, "CDC sample", amount, status, deleted),
            since, operation,
        )

    samples = {"insert": [], "update": [], "delete": []}
    customer, started = write(
        "INSERT INTO ecom.customers (name, email) VALUES (%s, %s) RETURNING id",
        ("CDC original", f"smoke-{uuid4().hex}@example.invalid"), True,
    )
    samples["insert"].append(check_customer(customer, "CDC original", 0, started, "insert"))
    order, started = write(
        "INSERT INTO ecom.orders (customer_id, product, amount) VALUES (%s, %s, %s) RETURNING id",
        (customer, "CDC sample", Decimal("9.50")), True,
    )
    samples["insert"].append(check_order(order, customer, "PENDING", 0, started, "insert"))

    _, started = write("UPDATE ecom.customers SET name = %s WHERE id = %s", ("CDC updated", customer))
    samples["update"].append(check_customer(customer, "CDC updated", 0, started, "update"))
    _, started = write(
        "UPDATE ecom.orders SET amount = %s, status = %s WHERE id = %s",
        (Decimal("10.75"), "SHIPPED", order),
    )
    samples["update"].append(check_order(order, customer, "SHIPPED", 0, started, "update", "10.75"))

    _, started = write("DELETE FROM ecom.orders WHERE id = %s", (order,))
    samples["delete"].append(check_order(order, customer, "SHIPPED", 1, started, "delete", "10.75"))
    _, started = write("DELETE FROM ecom.customers WHERE id = %s", (customer,))
    samples["delete"].append(check_customer(customer, "CDC updated", 1, started, "delete"))
    return customer, samples


def check_kafka(customers, timeout):
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": "127.0.0.1:29092",
        "group.id": f"cdc-smoke-{uuid4().hex}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    groups = {identifier: [] for identifier in customers}
    deadline = time.monotonic() + timeout
    try:
        consumer.subscribe(["dbserver.ecom.customers"])
        while time.monotonic() < deadline:
            message = consumer.poll(min(0.5, max(0, deadline - time.monotonic())))
            if message is None or message.error() or message.value() is None:
                continue
            event = json.loads(message.value())
            identifier = (event.get("after") or event.get("before") or {}).get("id")
            if identifier in groups:
                groups[identifier].append(event)
            if all(OPERATIONS <= {event["op"] for event in events} for events in groups.values()):
                break
    finally:
        consumer.close()
    for identifier, events in groups.items():
        assert_masked_customer_events(events, identifier)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=45)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.rounds < 1 or args.timeout <= 0:
        parser.error("rounds and timeout must be positive")

    import psycopg2

    from scripts.verify import PG_PARAMS, query_clickhouse

    start_connector(args.timeout)
    wait_for("ClickHouse tables", lambda: query_clickhouse("SELECT count() FROM ecom.orders", strict=True),
             bool, args.timeout)
    samples = {"insert": [], "update": [], "delete": []}
    customers = []
    connection = psycopg2.connect(**PG_PARAMS, connect_timeout=5)
    try:
        warmup_customer, _ = measure_round(connection, query_clickhouse, args.timeout)
        customers.append(warmup_customer)
        for _ in range(args.rounds):
            customer, measurements = measure_round(connection, query_clickhouse, args.timeout)
            customers.append(customer)
            for operation, values in measurements.items():
                samples[operation].extend(values)
    finally:
        connection.close()
    check_kafka(customers, args.timeout)
    report = {"rounds": args.rounds, "poll_interval_ms": 100,
              "all": latency_summary([value for values in samples.values() for value in values]),
              "by_operation": {operation: latency_summary(values) for operation, values in samples.items()}}
    output = json.dumps(report, indent=2) + "\n"
    if args.output is not None:
        args.output.write_text(output, encoding="utf-8")
    print(output, end="")


if __name__ == "__main__":
    main()
