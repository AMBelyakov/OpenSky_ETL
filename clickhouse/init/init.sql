-- 1. Таблица фактов: точки треков воздушных судов (только метрики и ключи измерений)
CREATE TABLE fact_flight_points (
    event_ts DateTime,
    icao24 String,
    station_id String,
    snapshot_time DateTime,
    time_window DateTime,
    flight_phase String,
    on_ground UInt8,
    latitude Float64,
    longitude Float64,
    height Float64,
    x_ecef Float64,
    y_ecef Float64,
    z_ecef Float64,
    velocity Float64,
    mach Float64,
    vertical_rate Float64,
    true_track Float64,
    r_to_earth Float64,
    r_to_station Float64
) ENGINE = ReplacingMergeTree(event_ts)
ORDER BY (icao24, snapshot_time)
PARTITION BY toYYYYMM(snapshot_time)
TTL snapshot_time + INTERVAL 1 YEAR DELETE;

-- 2. Словарь бортов (тянет измерение из Postgres)
CREATE DICTIONARY dim_flight_dict
(
    icao24 String,
    callsign String,
    origin_country String
)
PRIMARY KEY icao24
SOURCE(POSTGRESQL(
    host 'artemw9-postgres'
    port 5432
    user 'airflow'
    password 'airflow'
    db 'backend'
    table 'dim_flight'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 300 MAX 3600);

-- 3. Словарь станций — только актуальные версии SCD2
CREATE DICTIONARY dim_station_dict
(
    station_id String,
    name String,
    station_type String,
    lat Float64,
    lon Float64,
    h Float64
)
PRIMARY KEY station_id
SOURCE(POSTGRESQL(
    host 'artemw9-postgres'
    port 5432
    user 'airflow'
    password 'airflow'
    db 'backend'
    table 'dim_station'
    where 'is_current = true'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 300 MAX 3600);

-- 4. Витрина OBT для Metabase: факты, обогащённые измерениями из словарей.
-- FINAL схлопывает дубли ReplacingMergeTree, накопленные повторными прогонами.
CREATE VIEW flight_points_detailed AS
SELECT
    f.event_ts,
    f.icao24,
    dictGetOrDefault('dim_flight_dict', 'callsign', tuple(f.icao24), '') AS callsign,
    dictGetOrDefault('dim_flight_dict', 'origin_country', tuple(f.icao24), '') AS origin_country,
    f.snapshot_time,
    f.time_window,
    f.flight_phase,
    f.on_ground,
    f.latitude,
    f.longitude,
    f.height,
    f.x_ecef,
    f.y_ecef,
    f.z_ecef,
    f.velocity,
    f.mach,
    f.vertical_rate,
    f.true_track,
    f.r_to_earth,
    f.r_to_station,
    f.station_id,
    dictGetOrDefault('dim_station_dict', 'name', tuple(f.station_id), '') AS station_name,
    dictGetOrDefault('dim_station_dict', 'station_type', tuple(f.station_id), '') AS station_type,
    dictGetOrDefault('dim_station_dict', 'lat', tuple(f.station_id), 0.0) AS station_lat,
    dictGetOrDefault('dim_station_dict', 'lon', tuple(f.station_id), 0.0) AS station_lon,
    dictGetOrDefault('dim_station_dict', 'h', tuple(f.station_id), 0.0) AS station_h
FROM fact_flight_points f FINAL;

-- 5. Агрегат по бортам за прогон
CREATE TABLE flights_agg (
    event_ts DateTime,
    icao24 String,
    callsign String,
    origin_country String,
    total_points Int64,
    first_seen DateTime,
    last_seen DateTime,
    tracked_sec Int64,
    max_height Float64,
    max_velocity Float64,
    max_mach Float64,
    avg_velocity Float64,
    min_r_to_station Float64,
    avg_r_to_station Float64,
    distance_km Float64,
    dominant_station String
) ENGINE = ReplacingMergeTree(event_ts)
ORDER BY (icao24, first_seen)
PARTITION BY toYYYYMM(first_seen)
TTL first_seen + INTERVAL 1 YEAR DELETE;

CREATE VIEW flights_agg_latest AS
SELECT * FROM flights_agg FINAL;

-- 6. Агрегат по минутным окнам и фазам полёта
CREATE TABLE time_agg (
    event_ts DateTime,
    time_window DateTime,
    flight_phase String,
    points_in_window Int64,
    flights_in_window Int64,
    avg_height Float64,
    avg_velocity Float64,
    avg_mach Float64,
    avg_r_to_station Float64
) ENGINE = ReplacingMergeTree(event_ts)
ORDER BY (time_window, flight_phase)
PARTITION BY toYYYYMM(time_window)
TTL time_window + INTERVAL 1 YEAR DELETE;

CREATE VIEW time_agg_latest AS
SELECT * FROM time_agg FINAL;
