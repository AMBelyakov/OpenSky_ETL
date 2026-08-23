import csv
import os
import time

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

STATES_URL = "https://opensky-network.org/api/states/all"

# Анонимный ответ отдаёт 17 полей: category (индекс 17) приходит только
# аутентифицированным клиентам, поэтому короткий вектор дополняется None.
STATE_FIELDS = [
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
    "category",
]

# sensors — список id приёмников, у анонимных клиентов всегда null.
CSV_FIELDS = ["snapshot_ts"] + [f for f in STATE_FIELDS if f != "sensors"]

# Бенилюкс и запад Германии: 3° x 7° = 21 кв.°, то есть тариф
# "<= 25 кв.° — 1 кредит за запрос" (анонимный лимит — 400 кредитов в сутки).

BBOX = {"lamin": 50.0, "lomin": 4.0, "lamax": 53.0, "lomax": 11.0}


def make_session(retries: int = 3, backoff: float = 1.0) -> requests.Session:
    """
    Сессия с keep-alive и ретраями.

    Соединение переиспользуется между запросами: за прогон их под сотню к
    одному хосту, и без keep-alive каждый заново гонял бы TLS-рукопожатие.
    Ретраи покрывают обрывы соединения и 5xx; 429 намеренно не в списке —
    он означает исчерпанные кредиты, повторять запрос бессмысленно.
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "artemw9-etl"
    return session


def fetch_snapshot(
    session: requests.Session, bbox: dict = BBOX, timeout: int = 30
) -> tuple:
    """
    Забирает один снапшот состояний воздушных судов из OpenSky.

    Args:
        session : requests.Session Сессия с keep-alive и ретраями.
        bbox : dict Границы области: lamin, lomin, lamax, lomax (градусы).
        timeout : int Таймаут HTTP-запроса, секунды.

    Returns:
        tuple (snapshot_ts, rows), где snapshot_ts — unix-время снапшота по
        версии OpenSky, а rows — список словарей по одному на борт.

    Raises:
        requests.HTTPError При 429 (кредиты исчерпаны) и прочих ошибках HTTP.
    """
    response = session.get(STATES_URL, params=bbox, timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    snapshot_ts = payload["time"]
    states = payload.get("states") or []

    rows = []
    for vector in states:
        # Вектор дополняем до полной длины: анонимный ответ короче на category.
        padded = list(vector) + [None] * (len(STATE_FIELDS) - len(vector))
        state = dict(zip(STATE_FIELDS, padded))
        state.pop("sensors", None)
        state["snapshot_ts"] = snapshot_ts
        rows.append(state)

    return snapshot_ts, rows


def poll_states(
    output_dir: str = "/opt/synthetic_data/",
    bbox: dict = BBOX,
    window_sec: int = 1800,
    interval_sec: int = 20,
) -> pd.DataFrame:
    """
    Опрашивает OpenSky в течение окна и складывает снапшоты в траектории.

    Один вызов /states/all — это мгновенный срез всех бортов в bbox. Траектория
    борта собирается из последовательности таких срезов: каждые interval_sec
    секунд в течение window_sec делается запрос, все точки пишутся в общий CSV.
    При window_sec=1800 и interval_sec=20 это 90 запросов, то есть 90 кредитов
    из 400 доступных анонимному клиенту в сутки.

    Опрос прерывается досрочно при 429 (кредиты кончились) — уже собранные точки
    при этом сохраняются. Сетевые сбои отдельных запросов пропускаются.

    Args:
        output_dir : str Директория, куда будет сохранён states.csv.
        bbox : dict Границы области опроса.
        window_sec : int Длительность окна опроса, секунды.
        interval_sec : int Пауза между запросами, секунды.

    Returns:
        pd.DataFrame Все собранные точки со столбцами CSV_FIELDS.

    Raises:
        RuntimeError Если за всё окно не удалось собрать ни одной точки.
    """
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "states.csv")

    deadline = time.monotonic() + window_sec
    rows = []
    requests_made = 0
    seen_snapshots = set()
    session = make_session()

    while time.monotonic() < deadline:
        try:
            snapshot_ts, snapshot_rows = fetch_snapshot(session, bbox)
            requests_made += 1
        except requests.HTTPError as error:
            status = error.response.status_code if error.response is not None else None
            if status == 429:
                print("OpenSky вернул 429: кредиты исчерпаны, опрос остановлен.")
                break
            print(f"OpenSky HTTP {status}, запрос пропущен.")
            time.sleep(interval_sec)
            continue
        except requests.RequestException as error:
            # Сюда попадаем только когда ретраи сессии уже исчерпаны.
            print(f"Сетевая ошибка ({error}), запрос пропущен.")
            time.sleep(interval_sec)
            continue

        # OpenSky обновляет срез не чаще раза в ~5-10 с: одинаковые snapshot_ts
        # дали бы дубли точек, поэтому повторный срез отбрасываем.
        if snapshot_ts not in seen_snapshots:
            seen_snapshots.add(snapshot_ts)
            rows.extend(snapshot_rows)
            print(f"Снапшот {snapshot_ts}: {len(snapshot_rows)} бортов.")

        time.sleep(interval_sec)

    if not rows:
        raise RuntimeError(
            f"OpenSky не отдал ни одной точки за {requests_made} запросов — "
            f"проверь лимит кредитов и границы bbox."
        )

    df = pd.DataFrame(rows, columns=CSV_FIELDS)
    df.to_csv(output_file, index=False, quoting=csv.QUOTE_MINIMAL)

    print(
        f"Сохранено {len(df)} точек по {df['icao24'].nunique()} бортам "
        f"({len(seen_snapshots)} снапшотов, {requests_made} запросов) → {output_file}"
    )
    return df


if __name__ == "__main__":
    df = poll_states(output_dir=".", window_sec=120, interval_sec=20)
    print(df.head().to_string(index=False))
