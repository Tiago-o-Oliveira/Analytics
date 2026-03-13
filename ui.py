
import json
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

DATA_DIR = Path("data")
STATUS_CACHE_FILE = Path("device_status_cache.json")

WARNING_THRESHOLD = pd.Timedelta(hours=1, minutes=30)
OFFLINE_THRESHOLD = pd.Timedelta(hours=3, minutes=30)


LOCAL_TIMEZONE = "Etc/GMT+3"
REFRESH_RATE_MS = 300000
PAGE_NAME = "Distance Sensor"

TELEGRAM_BOT_TOKEN = "8712115882:AAFwRwqoML0Y6czEg00mSD9dSrfwc_AoGsc"
TELEGRAM_CHAT_ID = "1271566578"


DEVICE_TABLE = pd.DataFrame(
    [
        {"device_id": "0x355025930370430", "device_name": "Hector"},
        {"device_id": "0x355025930370349", "device_name": "Tristan"},
        {"device_id": "0x355025930370422", "device_name": "Bedivere"},
        {"device_id": "0x355025930370380", "device_name": "Galahad"},
        {"device_id": "0x355025930370398", "device_name": "Lancelot"},
        {"device_id": "0x355025930370455", "device_name": "Arthur"},
        {"device_id": "0x355025930370331", "device_name": "Gauvain"},
        {"device_id": "0x355025930370463", "device_name": "Parcival"},
        {"device_id": "0x355025930370364", "device_name": "Caradoc"},
        {"device_id": "0x355025930370448", "device_name": "Constantino"},
        {"device_id": "0x355025930370588", "device_name": "Leodegrance"},
        {"device_id": "???", "device_name": "Mordred"},
    ]
)

DEVICE_NAME_MAP = dict(zip(DEVICE_TABLE["device_id"], DEVICE_TABLE["device_name"]))


def load_status_cache():
    if not STATUS_CACHE_FILE.exists():
        return {}

    try:
        with STATUS_CACHE_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_status_cache(cache):
    with STATUS_CACHE_FILE.open("w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def send_telegram_message(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        st.error(f"Failed to send Telegram message: {e}")


def notify_warning_to_offline(status_table: pd.DataFrame):
    cache = load_status_cache()

    for _, row in status_table.iterrows():
        device_id = row["device_id"]
        device_name = row["device_name"]
        current_status = row["status"]

        previous_status = cache.get(device_id, {}).get("status")

        if previous_status == "warning" and current_status == "offline":
            send_telegram_message(f'Device "{device_name}" is offline now')

        cache[device_id] = {
            "status": current_status
        }

    save_status_cache(cache)


def load_device_file(file_path: Path) -> pd.DataFrame:
    rows = []

    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            item = json.loads(line)

            device_id = item.get("device_id")

            row = {
                "device_id": device_id,
                "device_name": DEVICE_NAME_MAP.get(device_id, "Unknown device"),
                "topic": item.get("topic"),
                "timestamp": item.get("timestamp"),
                "payload_hex": item.get("payload_hex"),
            }

            decrypted = item.get("payload_decrypted", {})
            if isinstance(decrypted, dict):
                row.update(decrypted)
            else:
                row["decrypted_value"] = decrypted

            rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["timestamp"] = (
        pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        .dt.tz_convert(LOCAL_TIMEZONE)
    )   
    return df

def get_last_measure(df: pd.DataFrame):
    if df.empty or "timestamp" not in df.columns:
        return None, None

    if "medicao_atual" not in df.columns:
        return None, None

    latest_row = (
        df.dropna(subset=["timestamp"])
        .sort_values("timestamp")
        .tail(1)
    )

    if latest_row.empty:
        return None, None

    value = latest_row.iloc[0]["medicao_atual"]

    if pd.isna(value):
        return None, None

    return "medicao_atual", value

def get_device_status_table(files):
    now = pd.Timestamp.now(tz=LOCAL_TIMEZONE)
    status_rows = []

    for file_path in files:
        df = load_device_file(file_path)

        device_id = file_path.stem
        device_name = DEVICE_NAME_MAP.get(device_id, "Unknown device")

        if df.empty or "timestamp" not in df.columns:
            last_seen = pd.NaT
            status = "offline"
            age = pd.NaT
            last_measure_name = None
            last_measure_value = None
        else:
            last_seen = df["timestamp"].max()
            last_measure_name, last_measure_value = get_last_measure(df)

            if pd.isna(last_seen):
                status = "offline"
                age = pd.NaT
            else:
                age = now - last_seen

                if age > OFFLINE_THRESHOLD:
                    status = "offline"
                elif age > WARNING_THRESHOLD:
                    status = "warning"
                else:
                    status = "online"

        status_rows.append(
            {
                "device_id": device_id,
                "device_name": device_name,
                "last_timestamp": last_seen,
                "age": age,
                "status": status,
                "flag": (
                    "🔴 Offline" if status == "offline"
                    else "🟡 Warning" if status == "warning"
                    else "🟢 Online"
                ),
                "last_measure_name": last_measure_name,
                "last_measure_value": last_measure_value,
            }
        )

    return pd.DataFrame(status_rows)

def format_timedelta(td):
    if pd.isna(td):
        return "No data"

    total_seconds = int(td.total_seconds())
    if total_seconds < 60:
        return f"{total_seconds}s ago"

    minutes = total_seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"

    hours = minutes // 60
    rem_minutes = minutes % 60
    return f"{hours}h {rem_minutes}m ago"

def format_measure(name, value):
    if name is None or value is None or pd.isna(value):
        return "No numeric data"

    if isinstance(value, (float, np.floating)):
        return f"{name}: {value:.2f}"
    return f"{name}: {value}"

def render_device_cards(status_table: pd.DataFrame):
    st.subheader("Device status")

    if status_table.empty:
        st.info("No devices found.")
        return

    cols_per_row = 4
    rows = [status_table.iloc[i:i + cols_per_row] for i in range(0, len(status_table), cols_per_row)]

    for row_df in rows:
        cols = st.columns(cols_per_row)

        for col, (_, row) in zip(cols, row_df.iterrows()):
            with col:
                status = row["status"]

                if status == "offline":
                    border_color = "#ff4b4b"
                elif status == "warning":
                    border_color = "#f1c40f"
                else:
                    border_color = "#2ecc71"

                last_seen_text = (
                    row["last_timestamp"].strftime("%Y-%m-%d %H:%M:%S")
                    if pd.notna(row["last_timestamp"])
                    else "No timestamp"
                )

                age_text = format_timedelta(row["age"])
                value = row["last_measure_value"]

                if pd.isna(value):
                    last_measure_text = "No data"
                elif isinstance(value, (float, np.floating)):
                    last_measure_text = f"{value:.2f}"
                else:
                    last_measure_text = str(value)

                st.markdown(
                    f"""
                    <div style="
                        border: 2px solid {border_color};
                        border-radius: 6px;
                        padding: 16px;
                        margin-bottom: 16px;
                        min-height: 220px;
                    ">
                        <div style="font-size: 1.1rem; font-weight: 700; margin-bottom: 10px;">
                            {row["device_name"]}
                        </div>
                        <div style="margin-bottom: 6px;"><b>ID:</b> {row["device_id"]}</div>
                        <div style="margin-bottom: 6px;"><b>Status:</b> {row["flag"]}</div>
                        <div style="margin-bottom: 6px;"><b>Last seen:</b> {last_seen_text}</div>
                        <div style="margin-bottom: 6px;"><b>Age:</b> {age_text}</div>
                        <div><b>Last measure:</b> {last_measure_text} [mm]</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

def main():
    st.set_page_config(page_title=PAGE_NAME, layout="wide")

    st_autorefresh(interval=REFRESH_RATE_MS, key="device_dashboard_refresh")

    
    st.title("IoT Sensor Viewer")

    if not DATA_DIR.exists():
        st.warning("No data folder found.")
        return

    files = sorted(DATA_DIR.glob("*.jsonl"))
    if not files:
        st.warning("No device files found in data/.")
        return

    status_table = get_device_status_table(files)
    notify_warning_to_offline(status_table)

    status_table = get_device_status_table(files)

    online_count = int((status_table["status"] == "online").sum())
    warning_count = int((status_table["status"] == "warning").sum())
    offline_count = int((status_table["status"] == "offline").sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total devices", len(status_table))
    c2.metric("Online", online_count)
    c3.metric("Warning", warning_count)
    c4.metric("Offline", offline_count)

    render_device_cards(status_table)

    def format_device_option(file_path: Path) -> str:
        device_id = file_path.stem
        row = status_table[status_table["device_id"] == device_id]

        device_name = DEVICE_NAME_MAP.get(device_id, "Unknown device")

        if row.empty:
            icon = "🔴"
        else:
            status = row.iloc[0]["status"]
            if status == "offline":
                icon = "🔴"
            elif status == "warning":
                icon = "🟡"
            else:
                icon = "🟢"

        return f"{icon} {device_name}"

    selected_file = st.selectbox(
        "Select device",
        files,
        format_func=format_device_option,
    )

    df = load_device_file(selected_file)

    if df.empty:
        st.warning("Selected device file is empty.")
        return

    selected_device_id = selected_file.stem
    selected_status_row = status_table[status_table["device_id"] == selected_device_id]

    if selected_status_row.empty:
        selected_device_name = DEVICE_NAME_MAP.get(selected_device_id, "Unknown device")
        selected_flag = "🔴 Offline"
        selected_last_seen = pd.NaT
    else:
        selected_device_name = selected_status_row.iloc[0]["device_name"]
        selected_flag = selected_status_row.iloc[0]["flag"]
        selected_last_seen = selected_status_row.iloc[0]["last_timestamp"]

    st.subheader(f"{selected_flag} {selected_device_name} ({selected_device_id})")

    if pd.notna(selected_last_seen):
        st.caption(f"Last timestamp: {selected_last_seen.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    else:
        st.caption("Last timestamp: No data")

    with st.expander("Show reports list", expanded=False):
        st.dataframe(df, width="stretch")

    excluded = {"device_id", "device_name", "topic", "timestamp", "payload_hex"}
    numeric_columns = [
        c for c in df.columns
        if c not in excluded and pd.api.types.is_numeric_dtype(df[c])
    ]

    if not numeric_columns:
        st.info("No numeric sensor fields found in decrypted payload.")
        return

    sensor_field = st.selectbox("Sensor value to plot", numeric_columns)

    plot_df = df.dropna(subset=["timestamp", sensor_field]).sort_values("timestamp")

    if plot_df.empty:
        st.info("No valid rows available for plotting.")
        return

    st.subheader("Plot options")

    fit_type = st.selectbox(
        "Trend / smoothing",
        ["None", "Linear trend", "Rolling average"]
    )

    window_size = 5
    if fit_type == "Rolling average":
        window_size = st.slider("Smoothing window", 3, 15, 5)
    
    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=plot_df["timestamp"],
            y=plot_df[sensor_field],
            mode="lines+markers",
            name=sensor_field,
        )
    )

    if fit_type == "Linear trend":
        x = (plot_df["timestamp"] - plot_df["timestamp"].min()).dt.total_seconds()
        y = plot_df[sensor_field]

        if len(x) > 1:
            coeffs = np.polyfit(x, y, 1)
            trend = np.poly1d(coeffs)

            fig.add_trace(
                go.Scatter(
                    x=plot_df["timestamp"],
                    y=trend(x),
                    mode="lines",
                    name="Linear trend",
                )
            )

    elif fit_type == "Rolling average":
        smooth = plot_df[sensor_field].rolling(window=window_size).mean()

        fig.add_trace(
            go.Scatter(
                x=plot_df["timestamp"],
                y=smooth,
                mode="lines",
                name=f"Rolling avg ({window_size})",
            )
        )
        fig.update_layout(
            title=f"{selected_flag} {selected_device_name} ({selected_device_id}) - {sensor_field}",
            xaxis_title="Timestamp",
            yaxis_title=sensor_field,
        )

    st.plotly_chart(fig, width="stretch")


if __name__ == "__main__":
    main()