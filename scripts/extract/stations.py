import io
import os

import pandas as pd

from .opensky import BBOX, make_session

# Публичный открытый датасет аэропортов мира, без авторизации.
AIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"

# Малую авиацию, вертолётные площадки и закрытые полосы не берём: у них нет
# постоянного наземного оборудования, к которому осмысленно считать дистанцию.
STATION_TYPES = ("large_airport", "medium_airport")

FEET_TO_METERS = 0.3048


def generate_stations(
    output_dir: str = "/opt/synthetic_data/", bbox: dict = None
) -> pd.DataFrame:
    """
    Собирает справочник наземных станций из открытого датасета OurAirports.

    Скачивает airports.csv, оставляет крупные и средние аэропорты внутри bbox
    и приводит их к схеме измерения dim_station. Высота в источнике хранится
    в футах, в витрине — в метрах.

    Args:
        output_dir : str Директория, куда будет сохранён stations.csv.
        bbox : dict Границы зоны, по умолчанию BBOX из opensky. Должны совпадать
            с зоной опроса бортов, иначе ближайшей окажется станция за
            пределами области наблюдения.

    Returns:
        pd.DataFrame Справочник со столбцами:
        - station_id (str): код станции (ICAO ident, например UUEE)
        - name (str): название аэропорта
        - station_type (str): large_airport / medium_airport
        - lat, lon (float): координаты WGS-84, градусы
        - h (float): высота над уровнем моря, метры

    Raises:
        RuntimeError Если внутри BBOX не нашлось ни одной станции.
        requests.RequestException Если скачать датасет не удалось и после ретраев.
    """
    if bbox is None:
        bbox = BBOX

    session = make_session(retries=4, backoff=2.0)
    response = session.get(AIRPORTS_URL, timeout=60)
    response.raise_for_status()

    airports = pd.read_csv(io.StringIO(response.text))

    stations = airports[
        airports["type"].isin(STATION_TYPES)
        & airports["latitude_deg"].between(bbox["lamin"], bbox["lamax"])
        & airports["longitude_deg"].between(bbox["lomin"], bbox["lomax"])
    ].copy()

    if stations.empty:
        raise RuntimeError(f"В границах {bbox} не найдено ни одной станции!")

    stations["h"] = (stations["elevation_ft"] * FEET_TO_METERS).round(1)

    stations = (
        stations.rename(
            columns={
                "ident": "station_id",
                "type": "station_type",
                "latitude_deg": "lat",
                "longitude_deg": "lon",
            }
        )[["station_id", "name", "station_type", "lat", "lon", "h"]]
        # Без высоты станция не участвует в геометрии — такие строки бесполезны.
        .dropna(subset=["h"])
        .sort_values("station_id")
        .reset_index(drop=True)
    )

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "stations.csv")
    stations.to_csv(output_file, index=False)

    print(f"Сохранено {len(stations)} станций → {output_file}")
    return stations


if __name__ == "__main__":
    df = generate_stations(output_dir=".")
    print(df.to_string(index=False))
