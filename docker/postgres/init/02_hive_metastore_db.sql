-- Hive Metastore database (used by Hive Metastore service).
CREATE DATABASE metastore_db;
CREATE USER hive WITH PASSWORD 'hive';
GRANT ALL PRIVILEGES ON DATABASE metastore_db TO hive;
ALTER DATABASE metastore_db OWNER TO hive;
