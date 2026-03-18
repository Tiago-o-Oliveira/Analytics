import argparse
import json
from datetime import datetime

import paho.mqtt.client as mqtt
import psycopg


def extract_device_id_from_topic(topic: str) -> str:
    """
    Example topic:
    ecomfort/iot/v1/s2g/gateway/LTE25082800003/device/0x355025930370398/event
    """
    parts = topic.strip("/").split("/")

    try:
        device_index = parts.index("device")
        return parts[device_index + 1]
    except (ValueError, IndexError):
        raise ValueError(f"Could not extract device id from topic: {topic}")


def decrypt_payload(payload: bytes) -> dict:
    """
    Parses payload into structured fields.
    """
    try:
        text = payload.decode("utf-8").strip()
    except UnicodeDecodeError:
        text = None

    value = None

    # Case 1: decimal text
    if text:
        try:
            value = int(text, 10)
        except ValueError:
            pass

        # Try hex text
        if value is None:
            try:
                value = int(text, 16)
            except ValueError:
                pass

    # Case 2: raw binary
    if value is None:
        if len(payload) >= 8:
            raw8 = payload[:8]
        else:
            raw8 = payload.rjust(8, b"\x00")
        value = int.from_bytes(raw8, byteorder="big", signed=False)

    # Normalize to 64-bit
    value &= 0xFFFFFFFFFFFFFFFF
    raw = value.to_bytes(8, byteorder="big", signed=False)

    return {
        "raw_u64": value,
        "raw_hex": raw.hex(),
        "fw_version": raw[0],
        "battery": raw[1],
        "temperature": raw[2],
        "signal": raw[3],
        "type": raw[4],
        "interval": raw[5],
        "medicao_atual": int.from_bytes(raw[6:8], byteorder="big", signed=False),
    }


class PostgresStorage:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = None
        self.connect()
        self.ensure_table()

    def connect(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass

        self.conn = psycopg.connect(self.dsn, autocommit=True)

    def ensure_connection(self) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
        except Exception:
            print("Database connection lost. Reconnecting...")
            self.connect()

    def ensure_table(self) -> None:
        self.ensure_connection()
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS mqtt_records (
                    id BIGSERIAL PRIMARY KEY,
                    device_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    payload_hex TEXT NOT NULL,
                    payload_decrypted JSONB NOT NULL
                );
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_mqtt_records_device_id ON mqtt_records(device_id);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_mqtt_records_timestamp ON mqtt_records(timestamp);"
            )

    def save_record(self, device_id: str, record: dict) -> None:
        self.ensure_connection()

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO mqtt_records (
                        device_id,
                        topic,
                        timestamp,
                        payload_hex,
                        payload_decrypted
                    )
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        device_id,
                        record["topic"],
                        record["timestamp"],
                        record["payload_hex"],
                        json.dumps(record["payload_decrypted"]),
                    ),
                )
        except Exception:
            print("Insert failed. Reconnecting and retrying once...")
            self.connect()
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO mqtt_records (
                        device_id,
                        topic,
                        timestamp,
                        payload_hex,
                        payload_decrypted
                    )
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        device_id,
                        record["topic"],
                        record["timestamp"],
                        record["payload_hex"],
                        json.dumps(record["payload_decrypted"]),
                    ),
                )


def process_message(topic: str, payload: bytes, storage: PostgresStorage) -> dict:
    device_id = extract_device_id_from_topic(topic)
    timestamp = datetime.now().astimezone().isoformat()
    decrypted = decrypt_payload(payload)

    record = {
        "device_id": device_id,
        "topic": topic,
        "timestamp": timestamp,
        "payload_hex": payload.hex(),
        "payload_decrypted": decrypted,
    }

    storage.save_record(device_id, record)
    return record


def on_connect(client, userdata, flags, reason_code, properties=None):
    topic = userdata["topic"]
    print(f"Connected with result code: {reason_code}")
    print(f"Subscribing to: {topic}")
    client.subscribe(topic)


def on_message(client, userdata, msg):
    try:
        storage = userdata["storage"]
        record = process_message(msg.topic, msg.payload, storage)
    except Exception as e:
        print(f"Error processing message from topic {msg.topic}: {e}")


def main():
    parser = argparse.ArgumentParser(description="MQTT IoT collector")

    parser.add_argument("--host", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument(
        "--topic",
        default="ecomfort/iot/v1/s2g/gateway/LTE25082800003/device/#",
        help="MQTT topic filter",
    )
    parser.add_argument("--username", default=None, help="MQTT username")
    parser.add_argument("--password", default=None, help="MQTT password")
    parser.add_argument("--client-id", default="iot-collector")

    parser.add_argument(
        "--db-dsn",
        required=True,
        help="PostgreSQL DSN (Neon), e.g. postgresql://user:pass@host/db?sslmode=require",
    )

    args = parser.parse_args()

    # Initialize storage
    storage = PostgresStorage(args.db_dsn)
    storage.ensure_table()

    userdata = {
        "topic": args.topic,
        "storage": storage,
    }

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=args.client_id,
        userdata=userdata,
    )

    if args.username:
        client.username_pw_set(args.username, args.password)

    client.on_connect = on_connect
    client.on_message = on_message

    print(f"Connecting to MQTT broker {args.host}:{args.port} ...")
    client.connect(args.host, args.port, keepalive=60)
    client.loop_forever()


if __name__ == "__main__":
    main()