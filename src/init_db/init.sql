-- Базы, которых нет в образе postgres по умолчанию.
-- airflow создаётся через POSTGRES_DB, backend и metabase — здесь.
CREATE DATABASE backend;
CREATE DATABASE metabase;

\c backend;

-- 1. Справочник бортов (SCD1: позывной меняется от рейса к рейсу, история не нужна)
CREATE TABLE IF NOT EXISTS dim_flight (
    icao24 VARCHAR(6) PRIMARY KEY,
    callsign VARCHAR(16),
    origin_country VARCHAR(128),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Справочник наземных станций (SCD2: переезд станции — событие с историей)
CREATE TABLE IF NOT EXISTS dim_station (
    station_sk BIGSERIAL PRIMARY KEY,
    station_id VARCHAR(16) NOT NULL,
    name VARCHAR(255),
    station_type VARCHAR(32),
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    h DOUBLE PRECISION,
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN DEFAULT TRUE,
    UNIQUE (station_id, valid_from)
);

-- Словарь ClickHouse тянет только текущие версии — под этот фильтр нужен индекс.
CREATE INDEX IF NOT EXISTS idx_dim_station_current
    ON dim_station (station_id) WHERE is_current;
