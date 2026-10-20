-- Demo stock for the SKUs in order-service's catalog. HOT-DROP is deliberately scarce
-- so the load test exercises contention and the no-oversell guarantee.
INSERT INTO products (sku, name, available) VALUES
    ('KB-MECH-01', 'Mechanical keyboard',         100000),
    ('MOUSE-02',   'Wireless mouse',              100000),
    ('MON-27-4K',  '27-inch 4K monitor',          100000),
    ('USB-C-HUB',  'USB-C hub, 7 ports',          100000),
    ('HDPHN-ANC',  'Noise-cancelling headphones', 100000),
    ('HOT-DROP',   'Limited edition console (flash sale)', 50);
