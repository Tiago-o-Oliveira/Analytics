import argparse
import base64
import json
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

def extract_device_id_from_topic(topic: str) -> str:
    """
    Example topic:
    ecomfort/iot/v1/s2g/gateway/LTE25082800003/device/0x355025930370398/event

    Extracts:
    0x355025930370398
    """
    parts = topic.strip("/").split("/")

    try:
        device_index = parts.index("device")
        return parts[device_index + 1]
    except (ValueError, IndexError):
        raise ValueError(f"Could not extract device id from topic: {topic}")

def decrypt_payload(payload: bytes) -> dict:
    """
    Expected payload:
    - usually arrives as ASCII decimal text, e.g. b'137681305591676928'
    - represents one 64-bit unsigned integer

    Layout:
    byte 1 = firmware version
    byte 2 = battery
    byte 3 = temperature
    byte 4 = signal
    byte 5 = type
    byte 6 = interval
    byte 7-8 = current measurement

    This function keeps only the first 8 bytes.
    """
    # Try to interpret MQTT payload as text first
    try:
        text = payload.decode("utf-8").strip()
    except UnicodeDecodeError:
        text = None

    value = None

    # Case 1: decimal integer as text
    if text:
        try:
            value = int(text, 10)
        except ValueError:
            pass

        # Optional: accept hex text too
        if value is None:
            try:
                value = int(text, 16)
            except ValueError:
                pass

    # Case 2: raw binary payload
    if value is None:
        if len(payload) >= 8:
            raw8 = payload[:8]
        else:
            raw8 = payload.rjust(8, b"\x00")
        value = int.from_bytes(raw8, byteorder="big", signed=False)

    # Force to 64-bit and discard anything above 8 bytes
    value &= 0xFFFFFFFFFFFFFFFF

    # Convert to exactly 8 bytes
    raw = value.to_bytes(8, byteorder="big", signed=False)

    fw_version = raw[0]
    battery = raw[1]
    temperature = raw[2]
    signal = raw[3]
    msg_type = raw[4]
    interval = raw[5]
    medicao_atual = int.from_bytes(raw[6:8], byteorder="big", signed=False)

    return {
        "raw_u64": value,
        "raw_hex": raw.hex(),
        "fw_version": fw_version,
        "battery": battery,
        "temperature": temperature,
        "signal": signal,
        "type": msg_type,
        "interval": interval,
        "medicao_atual": medicao_atual,
    }

def save_record(device_id: str, record: dict) -> None:
    safe_name = device_id.replace("/", "_")
    file_path = DATA_DIR / f"{safe_name}.jsonl"

    with file_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def process_message(topic: str, payload: bytes) -> dict:
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

    save_record(device_id, record)
    return record

def on_connect(client, userdata, flags, reason_code, properties=None):
    topic = userdata["topic"]
    print(f"Connected with result code: {reason_code}")
    print(f"Subscribing to: {topic}")
    client.subscribe(topic)

def on_message(client, userdata, msg):
    try:
        record = process_message(msg.topic, msg.payload)
        #print(json.dumps(record, ensure_ascii=False))
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
    parser.add_argument("--client-id", default="iot-collector", help="MQTT client id")

    args = parser.parse_args()

    userdata = {"topic": args.topic}
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