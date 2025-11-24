import csv
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

DEFAULT_AXIS_MAP = {"1": "X", "2": "Y", "3": "Z"}
SENSOR_AXIS_MAP: Dict[str, Dict[str, str]] = {
    "1890727266": {"1": "X", "2": "Y", "3": "Z"},
}


def check_exising_source(client, source_id):
    source = client.get_source(source_id)
    if source is None:
        return None
    source_info = source.json()
    return source_info["source_id"]


def check_exising_measurements(client, source_id: str, timestamp: int):
    measurement = client.get_measurement_by_time(
        source_id=source_id,
        timestamp=timestamp,
    )
    if measurement is None:
        return None
    return measurement.json()


def load_json_payload(file_path: str) -> dict:
    with open(file_path, "r") as f:
        return json.load(f)


def localize_timestamp(date: str, timezone, date_format) -> int:
    tz = ZoneInfo(timezone)
    dt = datetime.strptime(date, date_format)
    localized_time = datetime.fromtimestamp(dt.timestamp(), tz)
    return int(localized_time.timestamp() * 1000)


def str_clean(s: str) -> str:
    # The source ID should not have spaces; replace them with underscores
    # Forbidden characters: / \ ? % * : | " < >
    # Please alwayes use standard English letters and numbers.
    forbidden_chars = ["/", "\\", "?", "%", "*", ":", "|", '"', "<", ">"]
    for char in forbidden_chars:
        s = s.replace(char, "")

    return s.strip().lower().replace(" ", "_")


def parse_information_file(info_path: Path) -> Dict[str, Any]:
    raw: Dict[str, str] = {}
    with info_path.open("r", encoding="utf-8") as f:
        for line in f:
            if ":" in line:
                k, v = line.split(":", 1)
                raw[k.strip()] = v.strip()

    # Duration -> seconds (e.g., "640ms", "0.64s", "1.2 sec")
    duration_s: Optional[float] = None
    dur = raw.get("Time Period", "")
    if dur:
        m = re.search(r"([\d\.]+)\s*(ms|s|sec|secs)?", dur, flags=re.I)
        if m:
            val = float(m.group(1))
            unit = (m.group(2) or "s").lower()
            duration_s = val / 1000.0 if unit == "ms" else val

    return {
        "location": raw.get("Device Serial", "") or raw.get("Device Name", ""),
        "machine": raw.get("Machine Name", ""),
        "sensor_id": raw.get("Sensor Serial", ""),
        "sensor_name": raw.get("Sensor Name", ""),
        "samples": int(raw.get("Samples", "0") or 0),
        "duration_s": duration_s,
        "date_time": raw.get("Recorded At", ""),
        # keep other fields if later needed
        "_raw": raw,
    }


def read_timewave_column(csv_path: Path) -> List[float]:
    # Tolerant CSV: sniff delimiter, handle BOM, decimal comma
    for enc in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            with csv_path.open("r", newline="", encoding=enc) as f:
                head = f.read(8192)
                f.seek(0)
                try:
                    delim = csv.Sniffer().sniff(head).delimiter
                except Exception:
                    delim = ","
                reader = csv.reader(f, delimiter=delim)
                header = [h.strip() for h in next(reader)]
                # get Timewave column index (case-insensitive)
                try:
                    idx = header.index("Timewave")
                except ValueError:
                    idx = next(
                        i for i, h in enumerate(header) if h.lower() == "timewave"
                    )
                out: List[float] = []
                for row in reader:
                    if len(row) > idx:
                        cell = row[idx].strip().replace(",", ".")
                        if cell:
                            try:
                                out.append(float(cell))
                            except ValueError:
                                pass
                return out
        except Exception:
            continue
    raise RuntimeError(f"Could not read CSV: {csv_path}")


def axis_for(sensor_id: str, values_filename: str) -> str:
    m = re.search(r"values_(\d+)\.csv$", values_filename, flags=re.I)
    if not m:
        return ""
    key = m.group(1)
    return SENSOR_AXIS_MAP.get(sensor_id, DEFAULT_AXIS_MAP).get(key, "")
