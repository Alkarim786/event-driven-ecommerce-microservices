-- Demo catalog. inventory-service seeds stock for the same SKUs.
INSERT INTO catalog (sku, name, price_cents) VALUES
    ('KB-MECH-01',  'Mechanical keyboard',        649900),
    ('MOUSE-02',    'Wireless mouse',             129900),
    ('MON-27-4K',   '27-inch 4K monitor',        2899900),
    ('USB-C-HUB',   'USB-C hub, 7 ports',         249900),
    ('HDPHN-ANC',   'Noise-cancelling headphones', 999900),
    ('HOT-DROP',    'Limited edition console (flash sale)', 4999900);
