#!/usr/bin/env python
# coding: utf-8

import argparse
import os

import psycopg2
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

JARS_DIR = os.environ.get("SPARK_JARS_DIR", "/opt/spark/jars")

jar_files = [
    f"{JARS_DIR}/postgresql-42.7.5.jar",
    f"{JARS_DIR}/clickhouse-jdbc-0.9.7-all.jar",
    f"{JARS_DIR}/hadoop-aws-3.3.4.jar",
    f"{JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar",
]

# --- Константы WGS-84 и стандартной атмосферы (ISA) ---
WGS84_A = 6378137.0  # большая полуось эллипсоида, м
WGS84_E2 = 0.0066943799901413165  # квадрат первого эксцентриситета, f * (2 - f)

ISA_T0 = 288.15  # температура на уровне моря, K
ISA_LAPSE = 0.0065  # градиент температуры в тропосфере, K/м
ISA_T_TROPOPAUSE = 216.65  # температура выше 11 км, K
GAMMA_R = 1.4 * 287.05  # показатель адиабаты * удельная газовая постоянная

TROPOPAUSE_M = 11000.0

# Порог вертикальной скорости, отделяющий набор/снижение от горизонтального
# полёта: ADS-B шумит на пару м/с даже на эшелоне.
VERTICAL_RATE_THRESHOLD = 2.5

parser = argparse.ArgumentParser()
parser.add_argument("--date", required=True)
parser.add_argument("--data-dir", default="/opt/synthetic_data")
args = parser.parse_args()

ds = args.date
data_dir = args.data_dir
path_gen = f"{data_dir}/parquet/date={ds}"

spark = (
    SparkSession.builder.appName("flight_etl")
    .config("spark.jars", ",".join(jar_files))
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_USER"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_PASSWORD"))
    .getOrCreate()
)

states = spark.read.parquet(f"{path_gen}/states.parquet")
stations = spark.read.parquet(f"{path_gen}/stations.parquet")


# --- БЛОК 1: SOURCE DQ (входной контроль сырых данных, fail fast) ---
print("Запуск Source DQ проверок...")

if states.filter(F.col("icao24").isNull()).count() > 0:
    raise ValueError("Source DQ Failed: в states есть null в icao24!")

if states.filter(F.col("velocity") < 0).count() > 0:
    raise ValueError("Source DQ Failed: в states отрицательная путевая скорость!")

# Один борт не может дважды попасть в один и тот же снапшот.
raw_duplicates = (
    states.groupBy("icao24", "snapshot_ts").count().filter("count > 1").count()
)
if raw_duplicates > 0:
    raise ValueError(
        f"Source DQ Failed: {raw_duplicates} дублей по (icao24, snapshot_ts)!"
    )

if stations.filter(F.col("station_id").isNull()).count() > 0:
    raise ValueError("Source DQ Failed: в stations есть null в station_id!")

print("Source DQ: сырые данные валидны.")


# --- Очистка и обогащение точек ---

# ADS-B отдаёт часть полей как null (борт вне зоны приёма, старый транспондер).
# Точка без координат или скорости бесполезна для витрины.
points = (
    states.where(F.col("latitude").isNotNull())
    .where(F.col("longitude").isNotNull())
    .where(F.col("velocity").isNotNull())
    .withColumn(
        "height",
        F.coalesce(F.col("geo_altitude"), F.col("baro_altitude"), F.lit(0.0)),
    )
    .withColumn("vertical_rate", F.coalesce(F.col("vertical_rate"), F.lit(0.0)))
    .where(F.col("height") >= 0)
)

# Скорость звука падает с высотой, поэтому число Маха считаем по модели ISA,
# а не по константе 340 м/с.
isa_temp = F.when(
    F.col("height") < TROPOPAUSE_M, F.lit(ISA_T0) - F.lit(ISA_LAPSE) * F.col("height")
).otherwise(F.lit(ISA_T_TROPOPAUSE))

points = points.withColumn("speed_of_sound", F.sqrt(F.lit(GAMMA_R) * isa_temp))

lat_rad = F.radians(F.col("latitude"))
lon_rad = F.radians(F.col("longitude"))
# Радиус кривизны первого вертикала.
prime_vertical = F.lit(WGS84_A) / F.sqrt(
    F.lit(1.0) - F.lit(WGS84_E2) * F.pow(F.sin(lat_rad), 2)
)

points = (
    points.withColumn("mach", F.col("velocity") / F.col("speed_of_sound"))
    .withColumn(
        "x_ecef",
        (prime_vertical + F.col("height")) * F.cos(lat_rad) * F.cos(lon_rad),
    )
    .withColumn(
        "y_ecef",
        (prime_vertical + F.col("height")) * F.cos(lat_rad) * F.sin(lon_rad),
    )
    .withColumn(
        "z_ecef",
        (prime_vertical * F.lit(1.0 - WGS84_E2) + F.col("height")) * F.sin(lat_rad),
    )
    .withColumn(
        "r_to_earth",
        F.sqrt(
            F.pow(F.col("x_ecef"), 2)
            + F.pow(F.col("y_ecef"), 2)
            + F.pow(F.col("z_ecef"), 2)
        ),
    )
    .withColumn(
        "flight_phase",
        F.when(F.col("on_ground"), "ground")
        .when(F.col("vertical_rate") > VERTICAL_RATE_THRESHOLD, "climb")
        .when(F.col("vertical_rate") < -VERTICAL_RATE_THRESHOLD, "descent")
        .otherwise("cruise"),
    )
    # Минутные окна — сетка для агрегатов по времени.
    .withColumn("time_window", (F.col("snapshot_ts") / 60).cast("long") * 60)
    .withColumn("event_time", F.col("snapshot_ts").cast("timestamp"))
)


# --- Справочник станций: типы и ECEF для поиска ближайшей ---

station_lat_rad = F.radians(F.col("lat"))
station_lon_rad = F.radians(F.col("lon"))
station_prime_vertical = F.lit(WGS84_A) / F.sqrt(
    F.lit(1.0) - F.lit(WGS84_E2) * F.pow(F.sin(station_lat_rad), 2)
)

station_df = (
    stations.withColumn("lat", F.col("lat").cast("double"))
    .withColumn("lon", F.col("lon").cast("double"))
    .withColumn("h", F.col("h").cast("double"))
    .withColumn(
        "station_x",
        (station_prime_vertical + F.col("h"))
        * F.cos(station_lat_rad)
        * F.cos(station_lon_rad),
    )
    .withColumn(
        "station_y",
        (station_prime_vertical + F.col("h"))
        * F.cos(station_lat_rad)
        * F.sin(station_lon_rad),
    )
    .withColumn(
        "station_z",
        (station_prime_vertical * F.lit(1.0 - WGS84_E2) + F.col("h"))
        * F.sin(station_lat_rad),
    )
    .dropDuplicates(["station_id"])
)

station_df.cache()
print(f"Станций в справочнике: {station_df.count()}")


# --- Ближайшая станция к каждой точке ---

# Станций несколько десятков, поэтому broadcast-джойн каждой точки со всеми
# станциями дешевле любой пространственной индексации.
nearest = (
    points.crossJoin(F.broadcast(station_df))
    .withColumn(
        "r_to_station",
        F.sqrt(
            F.pow(F.col("x_ecef") - F.col("station_x"), 2)
            + F.pow(F.col("y_ecef") - F.col("station_y"), 2)
            + F.pow(F.col("z_ecef") - F.col("station_z"), 2)
        ),
    )
    .withColumn(
        "station_rank",
        F.row_number().over(
            Window.partitionBy("icao24", "snapshot_ts").orderBy(
                F.asc("r_to_station"), F.asc("station_id")
            )
        ),
    )
    .where(F.col("station_rank") == 1)
    .drop("station_rank", "station_x", "station_y", "station_z")
)


# --- Витрина: OBT по точкам ---

flight_points = nearest.select(
    "icao24",
    F.trim(F.col("callsign")).alias("callsign"),
    "origin_country",
    "snapshot_ts",
    "event_time",
    "time_window",
    "flight_phase",
    "on_ground",
    "latitude",
    "longitude",
    "height",
    "x_ecef",
    "y_ecef",
    "z_ecef",
    "velocity",
    "mach",
    "vertical_rate",
    "true_track",
    "r_to_earth",
    "r_to_station",
    "station_id",
    F.col("name").alias("station_name"),
    F.col("lat").alias("station_lat"),
    F.col("lon").alias("station_lon"),
    F.col("h").alias("station_h"),
).withColumn("event_ts", F.current_timestamp())

flight_points.cache()
points_count = flight_points.count()
print(f"Точек в витрине: {points_count}")


# --- БЛОК 2: TARGET DQ (проверка после трансформаций) ---
print("Запуск Target DQ проверок...")

# Поиск ближайшей станции не должен ни размножить точки, ни потерять их.
clean_count = points.count()
if points_count != clean_count:
    raise ValueError(
        f"Target DQ Failed: поиск ближайшей станции изменил число строк! "
        f"До: {clean_count}, после: {points_count} "
        f"(разница: {points_count - clean_count:+d})."
    )

if flight_points.filter(F.col("station_id").isNull()).count() > 0:
    raise ValueError("Target DQ Failed: у части точек не определилась станция!")

print("Target DQ: трансформации прошли успешно, число строк сохранено.")


# --- Агрегаты ---

flights_agg = (
    flight_points.groupBy("icao24", "callsign", "origin_country")
    .agg(
        F.count("snapshot_ts").alias("total_points"),
        F.min("snapshot_ts").alias("first_seen"),
        F.max("snapshot_ts").alias("last_seen"),
        F.max("height").alias("max_height"),
        F.max("velocity").alias("max_velocity"),
        F.max("mach").alias("max_mach"),
        F.avg("velocity").alias("avg_velocity"),
        F.min("r_to_station").alias("min_r_to_station"),
        F.avg("r_to_station").alias("avg_r_to_station"),
    )
    .withColumn("tracked_sec", F.col("last_seen") - F.col("first_seen"))
    # Оценка пройденного пути: средняя путевая скорость на время наблюдения.
    .withColumn(
        "distance_km",
        F.round(F.col("avg_velocity") * F.col("tracked_sec") / 1000, 1),
    )
    .withColumn("first_seen", F.col("first_seen").cast("timestamp"))
    .withColumn("last_seen", F.col("last_seen").cast("timestamp"))
    .withColumn("event_ts", F.current_timestamp())
)

# Какая станция вела борт дольше всех.
dominant_station = (
    flight_points.groupBy("icao24", "station_id")
    .agg(F.count("snapshot_ts").alias("points_tracked"))
    .withColumn(
        "rank",
        F.row_number().over(
            Window.partitionBy("icao24").orderBy(
                F.desc("points_tracked"), F.asc("station_id")
            )
        ),
    )
    .where(F.col("rank") == 1)
    .select("icao24", F.col("station_id").alias("dominant_station"))
)

flights_agg = flights_agg.join(dominant_station, "icao24", "left")
flights_agg.cache().count()

time_agg = (
    flight_points.groupBy("time_window", "flight_phase")
    .agg(
        F.count("snapshot_ts").alias("points_in_window"),
        F.countDistinct("icao24").alias("flights_in_window"),
        F.avg("height").alias("avg_height"),
        F.avg("velocity").alias("avg_velocity"),
        F.avg("mach").alias("avg_mach"),
        F.avg("r_to_station").alias("avg_r_to_station"),
    )
    .withColumn("time_window", F.col("time_window").cast("timestamp"))
    .withColumn("event_ts", F.current_timestamp())
    .orderBy("time_window", "flight_phase")
)

time_agg.cache().count()


# --- Разведочные метрики в лог ---

print("Топ бортов по максимальной высоте:")
(
    flights_agg.select(
        "icao24", "callsign", "origin_country", "max_height", "max_mach", "tracked_sec"
    )
    .sort(F.desc("max_height"))
    .show(10, truncate=False)
)

print("Распределение фаз полёта:")
(
    flight_points.groupBy("flight_phase")
    .agg(F.count("*").alias("points"))
    .withColumn(
        "pct",
        F.round(F.col("points") / F.sum("points").over(Window.partitionBy()) * 100, 1),
    )
    .sort(F.desc("points"))
    .show(truncate=False)
)

print("Общая статистика прогона:")
(
    flights_agg.select(
        F.count("icao24").alias("total_flights"),
        F.sum("total_points").alias("total_points"),
        F.round(F.avg("max_mach"), 3).alias("avg_max_mach"),
        F.round(F.max("max_height"), 1).alias("global_max_height_m"),
    ).show(truncate=False)
)


# --- Запись результатов ---

pg_host = "artemw9-postgres"
pg_port = "5432"
pg_db = "backend"
pg_user = os.environ.get("POSTGRES_USER")
pg_password = os.environ.get("POSTGRES_PASSWORD")

ch_host = "clickhouse"
ch_port = "8123"
ch_db = "default"
ch_user = os.environ.get("CLICKHOUSE_USER")
ch_password = os.environ.get("CLICKHOUSE_PASSWORD")


def write_to_pg_staging(df, table_name):
    (
        df.write.format("jdbc")
        .option("url", f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}")
        .option("dbtable", f"stg_{table_name}")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


def execute_pg_sql(sql_query):
    conn = psycopg2.connect(
        host=pg_host, port=pg_port, dbname=pg_db, user=pg_user, password=pg_password
    )
    cursor = conn.cursor()
    cursor.execute(sql_query)
    conn.commit()
    cursor.close()
    conn.close()


def save_to_clickhouse(df, table_name):
    (
        df.write.format("jdbc")
        .option("url", f"jdbc:clickhouse://{ch_host}:{ch_port}/{ch_db}")
        .option("dbtable", f"{ch_db}.{table_name}")
        .option("user", ch_user)
        .option("password", ch_password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .mode("append")
        .save()
    )


# 1. Борта (SCD1: позывной может смениться, история не нужна)
print("Запись dim_flight в Postgres (Upsert)...")

dim_flight_df = flight_points.select(
    "icao24", "callsign", "origin_country"
).dropDuplicates(["icao24"])

write_to_pg_staging(dim_flight_df, "dim_flight")
execute_pg_sql("""
    INSERT INTO dim_flight (icao24, callsign, origin_country, updated_at)
    SELECT icao24, callsign, origin_country, CURRENT_TIMESTAMP
    FROM stg_dim_flight
    ON CONFLICT (icao24) DO UPDATE SET
        callsign = EXCLUDED.callsign,
        origin_country = EXCLUDED.origin_country,
        updated_at = EXCLUDED.updated_at;
""")

# 2. Станции (SCD2: переезд или смена высоты станции — событие с историей)
print("Запись dim_station в Postgres (SCD2)...")

dim_station_df = station_df.select(
    "station_id", "name", "station_type", "lat", "lon", "h"
)
write_to_pg_staging(dim_station_df, "dim_station")

execute_pg_sql("""
    -- Шаг 1: закрываем текущие версии, у которых изменились атрибуты
    UPDATE dim_station ds
    SET valid_to = CURRENT_TIMESTAMP, is_current = false
    FROM stg_dim_station stg
    WHERE ds.station_id = stg.station_id
      AND ds.is_current = true
      AND (ds.lat != stg.lat OR ds.lon != stg.lon OR ds.h != stg.h
           OR ds.name != stg.name OR ds.station_type != stg.station_type);

    -- Шаг 2: вставляем новые версии (новая станция либо только что закрытая)
    INSERT INTO dim_station (station_id, name, station_type, lat, lon, h,
                             valid_from, valid_to, is_current)
    SELECT DISTINCT ON (stg.station_id)
        stg.station_id, stg.name, stg.station_type, stg.lat, stg.lon, stg.h,
        CURRENT_TIMESTAMP, '9999-12-31 23:59:59', true
    FROM stg_dim_station stg
    LEFT JOIN dim_station ds
      ON stg.station_id = ds.station_id AND ds.is_current = true
    WHERE ds.station_id IS NULL;

    -- Шаг 3: закрываем станции, пропавшие из источника.
    UPDATE dim_station ds
    SET valid_to = CURRENT_TIMESTAMP, is_current = false
    WHERE ds.is_current = true
      AND NOT EXISTS (
          SELECT 1 FROM stg_dim_station stg WHERE stg.station_id = ds.station_id
      );
""")

# 3. Факты и агрегаты — в ClickHouse
print("Запись фактов и агрегатов в ClickHouse...")

fact_flight_points = flight_points.select(
    "event_ts",
    "icao24",
    "station_id",
    F.col("event_time").alias("snapshot_time"),
    F.col("time_window").cast("timestamp").alias("time_window"),
    "flight_phase",
    F.col("on_ground").cast("int").alias("on_ground"),
    "latitude",
    "longitude",
    "height",
    "x_ecef",
    "y_ecef",
    "z_ecef",
    "velocity",
    "mach",
    "vertical_rate",
    "true_track",
    "r_to_earth",
    "r_to_station",
)

save_to_clickhouse(fact_flight_points, "fact_flight_points")
save_to_clickhouse(flights_agg, "flights_agg")
save_to_clickhouse(time_agg, "time_agg")

# 4. Curated Parquet локально — отсюда его заберёт таска выгрузки в S3
curated_path = f"{data_dir}/curated/date={ds}/flight_points"
flight_points.write.mode("overwrite").parquet(curated_path)
print(f"Curated Parquet сохранён: {curated_path}")

flight_points.unpersist()
flights_agg.unpersist()
time_agg.unpersist()
station_df.unpersist()

spark.stop()
