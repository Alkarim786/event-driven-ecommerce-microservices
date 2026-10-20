#!/usr/bin/env python3
"""End-to-end saga load test (stdlib only, so it runs in a bare python:alpine container).

Places N orders with C in flight. Each worker POSTs an order, then polls until the saga reaches a
terminal state. Afterwards it audits correctness across all four services:

  * every order reached CONFIRMED or CANCELLED
  * no oversell: for every SKU, available + reserved + sold == stock before the run,
    reserved == 0 and available >= 0
  * no duplicate effects: one notification per order, and every order that reached
    payment has exactly its one payment record with the matching outcome

Latency is reported two ways:
  * saga latency: server-side, order.completed_at - order.created_at (no polling bias)
  * client latency: POST until the client observed the terminal state (includes poll interval)

Env: ORDERS (1000), CONCURRENCY (50), HOT_DROP_PCT (20), DECLINE_PCT (5), POLL_MS (20), SEED (42)
"""
import json
import os
import random
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

ORDER_URL = os.environ.get("ORDER_URL", "http://localhost:8081")
INVENTORY_URL = os.environ.get("INVENTORY_URL", "http://localhost:8082")
PAYMENT_URL = os.environ.get("PAYMENT_URL", "http://localhost:8083")
NOTIFICATION_URL = os.environ.get("NOTIFICATION_URL", "http://localhost:8084")
ORDERS = int(os.environ.get("ORDERS", "1000"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "50"))
HOT_DROP_PCT = int(os.environ.get("HOT_DROP_PCT", "20"))
DECLINE_PCT = int(os.environ.get("DECLINE_PCT", "5"))
POLL_MS = int(os.environ.get("POLL_MS", "20"))
SEED = int(os.environ.get("SEED", "42"))

REGULAR_SKUS = ["KB-MECH-01", "MOUSE-02", "MON-27-4K", "USB-C-HUB", "HDPHN-ANC"]


def http(method, url, body=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"} if data else {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        return resp.status, json.loads(raw) if raw else None


def wait_healthy(deadline_s=180):
    start = time.time()
    for base in (ORDER_URL, INVENTORY_URL, PAYMENT_URL, NOTIFICATION_URL):
        while True:
            try:
                status, body = http("GET", base + "/actuator/health", timeout=3)
                if status == 200 and body.get("status") == "UP":
                    break
            except (urllib.error.URLError, OSError, ValueError):
                pass
            if time.time() - start > deadline_s:
                sys.exit(f"service {base} not healthy after {deadline_s}s")
            time.sleep(1)


def inventory_snapshot():
    _, products = http("GET", INVENTORY_URL + "/inventory")
    return {p["sku"]: p for p in products}


def percentile(values, p):
    if not values:
        return float("nan")
    s = sorted(values)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def build_orders(rng):
    plans = []
    for i in range(ORDERS):
        items = [{"sku": sku, "quantity": rng.randint(1, 3)}
                 for sku in rng.sample(REGULAR_SKUS, rng.randint(1, 3))]
        if rng.randrange(100) < HOT_DROP_PCT:
            items.append({"sku": "HOT-DROP", "quantity": 1})
        token = "tok_decline" if rng.randrange(100) < DECLINE_PCT else "tok_ok"
        plans.append({"customerId": f"load-{i % 200}", "items": items, "paymentToken": token})
    return plans


def run_one(plan):
    t0 = time.perf_counter()
    _, placed = http("POST", ORDER_URL + "/orders", plan)
    order_id = placed["orderId"]
    deadline = t0 + 120
    while time.perf_counter() < deadline:
        _, order = http("GET", f"{ORDER_URL}/orders/{order_id}")
        if order["status"] != "PENDING":
            return order, (time.perf_counter() - t0) * 1000
        time.sleep(POLL_MS / 1000)
    raise TimeoutError(f"order {order_id} still PENDING after 120s")


def main():
    wait_healthy()
    rng = random.Random(SEED)
    plans = build_orders(rng)
    before = inventory_snapshot()

    results, errors = [], []
    lock = threading.Lock()

    def task(plan):
        try:
            r = run_one(plan)
            with lock:
                results.append(r)
        except Exception as e:  # noqa: BLE001 - report every failure, don't hide it
            with lock:
                errors.append(repr(e))

    print(f"placing {ORDERS} orders, {CONCURRENCY} in flight ...", flush=True)
    wall0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        list(pool.map(task, plans))
    wall_s = time.perf_counter() - wall0

    # Let trailing events (StockCommitted / StockReleased / notifications) settle before auditing.
    time.sleep(3)
    after = inventory_snapshot()

    orders = [o for o, _ in results]
    confirmed = [o for o in orders if o["status"] == "CONFIRMED"]
    cancelled = [o for o in orders if o["status"] == "CANCELLED"]
    saga_ms = [o["sagaLatencyMs"] for o in orders if o.get("sagaLatencyMs") is not None]
    client_ms = [ms for _, ms in results]

    # Inventory audit.
    oversell, invariant_violations = 0, []
    for sku, b in before.items():
        a = after[sku]
        total_before = b["available"] + b["reserved"] + b["sold"]
        total_after = a["available"] + a["reserved"] + a["sold"]
        if total_before != total_after or a["reserved"] != 0 or a["available"] < 0:
            invariant_violations.append(sku)
        oversell += max(0, a["sold"] - (b["available"] + b["sold"]))
    hot_sold = after["HOT-DROP"]["sold"] - before["HOT-DROP"]["sold"]
    hot_confirmed = sum(1 for o in confirmed if any(i["sku"] == "HOT-DROP" for i in o["items"]))

    # Effect audit: one notification per order; every order that reached payment has its payment
    # record with the matching outcome. (payments.order_id is UNIQUE, so a second charge cannot
    # even be stored; the chaos runs show the consumers never try.)
    duplicate_notifications = missing_notifications = payment_mismatches = 0
    for o in orders:
        nots = http("GET", f"{NOTIFICATION_URL}/notifications?orderId={o['orderId']}")[1]
        duplicate_notifications += max(0, len(nots) - 1)
        missing_notifications += 1 if len(nots) == 0 else 0
        events = {e["event"] for e in o["timeline"]}
        if events & {"PaymentCompleted", "PaymentFailed"}:
            try:
                payment = http("GET", f"{PAYMENT_URL}/payments/{o['orderId']}")[1]
                expected = "COMPLETED" if o["status"] == "CONFIRMED" else "FAILED"
                payment_mismatches += 0 if payment["status"] == expected else 1
            except urllib.error.HTTPError:
                payment_mismatches += 1

    report = {
        "orders": ORDERS,
        "concurrency": CONCURRENCY,
        "completed": len(orders),
        "errors": len(errors),
        "confirmed": len(confirmed),
        "cancelled": len(cancelled),
        "wall_seconds": round(wall_s, 2),
        "throughput_orders_per_s": round(len(orders) / wall_s, 1),
        "saga_latency_ms": {
            "p50": round(percentile(saga_ms, 50), 1),
            "p95": round(percentile(saga_ms, 95), 1),
            "p99": round(percentile(saga_ms, 99), 1),
            "max": max(saga_ms) if saga_ms else None,
            "mean": round(statistics.mean(saga_ms), 1) if saga_ms else None,
        },
        "client_latency_ms": {
            "p50": round(percentile(client_ms, 50), 1),
            "p95": round(percentile(client_ms, 95), 1),
            "p99": round(percentile(client_ms, 99), 1),
        },
        "hot_drop": {"stock_before": before["HOT-DROP"]["available"], "sold": hot_sold,
                     "confirmed_orders_with_hot_drop": hot_confirmed},
        "oversell_units": oversell,
        "inventory_invariant_violations": invariant_violations,
        "duplicate_notifications": duplicate_notifications,
        "missing_notifications": missing_notifications,
        "payment_mismatches": payment_mismatches,
    }
    print(json.dumps(report, indent=2))
    if errors:
        print("first errors:", errors[:5], file=sys.stderr)
    ok = (not errors and oversell == 0 and not invariant_violations
          and duplicate_notifications == 0 and missing_notifications == 0
          and payment_mismatches == 0)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
