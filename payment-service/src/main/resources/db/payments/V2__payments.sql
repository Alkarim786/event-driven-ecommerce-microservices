-- One payment attempt per order. The UNIQUE constraint makes "charge twice" impossible
-- even if every other idempotency layer failed.
CREATE TABLE payments (
    id              UUID         PRIMARY KEY,
    order_id        VARCHAR(64)  NOT NULL UNIQUE,
    customer_id     VARCHAR(64)  NOT NULL,
    amount_cents    BIGINT       NOT NULL,
    status          VARCHAR(16)  NOT NULL,
    failure_reason  VARCHAR(200),
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL
);
