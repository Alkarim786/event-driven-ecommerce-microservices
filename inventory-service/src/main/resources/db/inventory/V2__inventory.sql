-- Invariant per SKU: available + reserved + sold = total stock ever received.
-- CHECK constraints are the last line of defence against oversell; optimistic locking is the first.
CREATE TABLE products (
    sku        VARCHAR(64)  PRIMARY KEY,
    name       VARCHAR(200) NOT NULL,
    available  INT          NOT NULL CHECK (available >= 0),
    reserved   INT          NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    sold       INT          NOT NULL DEFAULT 0 CHECK (sold >= 0),
    version    BIGINT       NOT NULL DEFAULT 0
);

-- One row per (order, sku). RESERVED → COMMITTED (payment ok) | RELEASED (compensation).
CREATE TABLE reservations (
    order_id    VARCHAR(64) NOT NULL,
    sku         VARCHAR(64) NOT NULL REFERENCES products (sku),
    quantity    INT         NOT NULL CHECK (quantity > 0),
    status      VARCHAR(16) NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (order_id, sku)
);
