-- One Postgres server, one database per service (database-per-service pattern).
-- Services never read each other's databases; they only exchange events.
CREATE DATABASE orders;
CREATE DATABASE inventory;
CREATE DATABASE payments;
CREATE DATABASE notifications;
