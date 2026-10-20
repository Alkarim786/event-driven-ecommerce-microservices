# Notes for Karim: Event-Driven E-Commerce Microservices

## Resume bullets (real measured numbers)

- Built an **event-driven e-commerce backend in Java 21 / Spring Boot with Kafka**: 4 microservices, one Postgres database each. A **choreography saga with compensation** keeps orders, stock and payments consistent without 2PC. A **transactional outbox** and **idempotent consumers** give exactly-once effects on top of at-least-once delivery.
- Load-tested **1,000 concurrent orders (50 in flight)** while **30% of events were deliberately published twice**: **0 oversold units, 0 duplicate charges or notifications, 0 errors**, at **17.5 orders/s** on a 4-core laptop. A flash-sale SKU sold exactly 50/50 units via optimistic locking.
- Wrote **50 tests**, including a Testcontainers suite that boots all 4 services against **real Kafka and Postgres with chaos duplicate delivery** and dead-letter-topic handling. The load test exposed a **connection-pool deadlock** (nested queries holding 2 pooled connections); I fixed it and added a pool-of-1 regression test.

## 5 likely interview questions

1. **"Why not a distributed transaction (2PC / XA)?"**
   - 2PC blocks every participant while the coordinator is down, and it couples all services' availability.
   - Kafka and most cloud databases don't support XA.
   - Sagas trade isolation for availability: each step commits locally, and failures are undone by **compensating actions** (`PaymentFailed` → `StockReleased`).
   - Be ready to admit the cost. Between steps the system is *temporarily* inconsistent: stock is reserved but not yet paid. That's why there's a `reserved` column and a `PENDING` state.

2. **"What is the dual-write problem and how does the outbox solve it?"**
   - "Save to DB, then send to Kafka" can crash in between, leaving a row with no event. "Send, then save" gives an event with no row.
   - The outbox writes the event as a **row in the same transaction**. `OutboxRelay` then publishes it: lock rows `FOR UPDATE SKIP LOCKED`, send, wait for all acks, mark published.
   - If it crashes after the acks but before the update, it re-sends. That's at-least-once, which is why consumers must be idempotent.
   - Outbox vs CDC (Debezium): CDC tails the WAL, so there's no polling and lower latency, but it's more infrastructure.

3. **"Kafka has exactly-once semantics. Why do you need idempotency?"**
   - Kafka EOS covers Kafka-to-Kafka (read-process-write within Kafka transactions).
   - My side effects go to **Postgres**, outside Kafka's transaction, so delivery is effectively at-least-once.
   - `IdempotentConsumer` inserts `(consumer_group, event_id)` in the same DB transaction as the effect, so a duplicate rolls back.
   - Defence in depth: `payments.order_id UNIQUE`, the reservation status guard, and the order status compare-and-set.

4. **"How do you guarantee ordering?"**
   - Every event is keyed by `orderId` → same partition → ordered for that order.
   - There is no ordering *across topics*. That's why `StockReserved` carries the amount and payment token: payment-service never waits for `OrderCreated` from another topic.
   - Caveat to volunteer: several relay instances with `SKIP LOCKED` could reorder one order's events. Run one relay per service, or partition the outbox.
   - Out-of-order or late events are made harmless by the state machine (`UPDATE … WHERE status='PENDING'`).

5. **"How does the last unit not get sold twice?"**
   - Two-phase reservation inside one transaction. Phase 1 reads every SKU's `available` and `version`, and rejects without writing if anything is short. Phase 2 runs `UPDATE … WHERE sku=? AND version=?`.
   - 0 rows means someone else changed it, so throw `OptimisticLockingFailureException`. `IdempotentConsumer` rolls back and retries with jitter.
   - `CHECK (available >= 0)` is the safety net.
   - Proven by a 24-thread unit test (3 units), the 40-buyer in-memory flash sale, the 20-buyer Testcontainers test, and the load test (HOT-DROP sold 50/50).
   - Follow-up: "why optimistic, not `SELECT FOR UPDATE`?" Low contention on most SKUs means no lock waits. Under heavy contention (a flash sale), pessimistic locking or a single-writer partition per SKU can win, because retries waste work.

Also be ready for: **poison messages** (`DefaultErrorHandler`: 4 retries, then `<topic>.DLT`; malformed JSON is not retried), and **choreography vs orchestration** (the timeline endpoint exists because choreography is hard to see).

## Understand this before interviews

### 1. `IdempotentConsumer.process` (messaging-common)
- The transaction order is: INSERT into `processed_messages` → run the effect → commit.
- A duplicate key on *that insert* is converted to a private `AlreadyProcessed` exception. That rolls back the transaction and returns `DUPLICATE`.
- Why not catch every `DuplicateKeyException`? Because a unique violation *inside the effect* (e.g. a second payment row) is a real bug and must surface. There's a test for exactly this.
- Two concurrent deliveries of the same event: the second insert blocks on the unique index until the first commits, then fails as a duplicate.
- `ConcurrencyFailureException` (optimistic lock, deadlock) triggers a retry of the whole transaction, up to N times.

### 2. `OutboxRelay.relayOnce`
- Select, send all, wait for all acks, then a batch UPDATE, all in one transaction.
- If any send fails, the whole batch rolls back, and rows that *were* acked get re-sent. The test `brokerFailureRollsBackSoTheBatchIsRetriedLater` asserts the exact duplicate.
- Wake-up: `OutboxWriter` registers an `afterCommit` hook that offers to an `ArrayBlockingQueue(1)`.
- Capacity 1 matters. A commit that happens while a pass is running leaves one signal behind instead of being lost. The first version used a `SynchronousQueue`, which drops the signal when nobody is waiting.

### 3. `InventoryHandler.reserve`: check-then-apply
- Why two phases? If you reserved item A and then found B short, you'd need a savepoint or manual undo.
- Checking everything first means the reject path writes nothing except the `StockRejected` outbox row.
- Sorted SKUs give a consistent lock order across concurrent multi-item orders, which reduces deadlocks.

### 4. Order state machine: compare-and-set in SQL
- `UPDATE orders SET status=? … WHERE id=? AND status='PENDING'`. 0 rows means already terminal: log it and emit nothing.
- That's why a late `PaymentFailed` can never cancel a confirmed order (`terminalOrderNeverFlipsAndEmitsNothingMore`).

### 5. The deadlock the load test found (great "tell me about a bug" story)
- **Symptom:** the first 1,000-order run had 100% of requests time out. Hikari logged `active=20, idle=0, waiting=155`.
- **Cause:** `OrderService.find` called `items()` and `timeline()` from *inside* the outer query's RowMapper. With no transaction, `JdbcTemplate` borrows a new connection for each nested query while the outer one is still open. 50 concurrent GETs and 20 connections meant every connection was held by a request waiting for a second one.
- **Fix:** run the nested queries after the outer result set closes.
- **Regression test:** a pool of exactly 1 connection (with a 2 s timeout), which fails fast with the old code.
- **Lesson:** "one request = one connection" is an invariant worth testing.

### 6. Why the benchmark numbers are what they are
- One laptop runs Kafka, Postgres, 4 JVMs and the load generator on 4 cores.
- Each saga is ~5 Kafka hops, each with a DB transaction.
- Slowing the generator's polling from 20 ms to 200 ms made sagas ~35% faster, so the box is CPU-bound.
- Saga latency is measured server-side (`completed_at - created_at`), so client polling doesn't bias it.
- Say this openly. It shows you understand measurement, not just the result.

### 7. System tests in one JVM
- Each service's config is `<service>.yml` (via `spring.config.name`) and its migrations live in `db/<service>/`. Otherwise four `application.yml` files and four `V2__*.sql` files on one classpath would collide.
- The services build a plain jar *and* an `-exec` fat jar (Spring Boot `classifier`), so `system-tests` can depend on them like libraries.
