#!/usr/bin/env python3
"""
ttn_mqtt_to_influx.py
Bridge: The Things Stack (MQTT v3) -> InfluxDB Cloud.
Designed to run on Railway (env vars set in project settings).
"""

import os
import ssl
import json
import time
import logging
from datetime import datetime, timezone

import certifi
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("ttn2influx")

# InfluxDB Cloud (env)
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "leituras")

# TTN MQTT (env)
MQTT_SERVER = os.getenv("TTN_MQTT_SERVER", "eu1.cloud.thethings.network")
MQTT_PORT = int(os.getenv("TTN_MQTT_PORT", "8883"))
MQTT_USERNAME = os.getenv("TTN_MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("TTN_MQTT_PASSWORD")
MQTT_TOPIC = os.getenv("TTN_MQTT_TOPIC", "v3/{app_id}@{tenant}/devices/+/up")
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))

# Basic checks
if not all([INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, MQTT_USERNAME, MQTT_PASSWORD]):
    log.error("Faltam variáveis de ambiente. Verifica INFLUX_... e TTN_MQTT_...")
    raise SystemExit(1)

# Influx client
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

def parse_iso_ts(ts):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)

def extract_measures(uplink):
    # Prefer decoded_payload
    dp = uplink.get("decoded_payload")
    if isinstance(dp, dict):
        return dp

    # fallback: frm_payload / payload_raw base64 decode
    frm = uplink.get("frm_payload") or uplink.get("payload_raw")
    if frm:
        try:
            raw = base64.b64decode(frm)
            try:
                j = json.loads(raw.decode("utf-8"))
                if isinstance(j, dict):
                    return j
            except Exception:
                txt = raw.decode("utf-8", errors="ignore").strip()
                try:
                    return {"value": float(txt)}
                except Exception:
                    return {"raw_hex": raw.hex()}
        except Exception:
            return {}
    return {}

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        log.info("Ligado ao MQTT broker (rc=%s).", rc)
        topic = MQTT_TOPIC
        if "{app_id}" in topic or "{tenant}" in topic:
            if "@" in MQTT_USERNAME:
                app_id, tenant = MQTT_USERNAME.split("@", 1)
                topic = topic.format(app_id=app_id, tenant=tenant)
        client.subscribe(topic, qos=MQTT_QOS)
        log.info("Subscrito: %s", topic)
    else:
        log.error("Erro de ligação MQTT (rc=%s)", rc)

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
    except Exception as e:
        log.exception("Mensagem MQTT não é JSON: %s", e)
        return

    end_device_ids = payload.get("end_device_ids", {})
    device_id = end_device_ids.get("device_id", "unknown")
    received_at = payload.get("received_at") or payload.get("uplink_message", {}).get("rx_metadata", [{}])[0].get("time")
    if not received_at:
        received_at = datetime.now(timezone.utc).isoformat()

    uplink = payload.get("uplink_message", {}) or payload
    measures = {}
    # try decoded_payload first
    dp = uplink.get("decoded_payload")
    if isinstance(dp, dict):
        measures = dp
    else:
        # try interpreted measures from frm_payload etc (not always present)
        # minimal: look in uplink.decoded_payload or use empty
        measures = uplink.get("decoded_payload") or {}

    timestamp = parse_iso_ts(received_at)

    try:
        point = Point("contador") \
            .tag("device", device_id) \
            .tag("application", end_device_ids.get("application_ids", {}).get("application_id", "")) \
            .time(timestamp, WritePrecision.NS)

        for k, v in measures.items():
            if v is None:
                continue
            if isinstance(v, bool):
                point.field(str(k), bool(v))
            else:
                try:
                    point.field(str(k), float(v))
                except Exception:
                    point.field(str(k), str(v))

        # optional metrics
        rx = uplink.get("rx_metadata")
        if isinstance(rx, list) and rx:
            first = rx[0]
            if "rssi" in first:
                try: point.field("rssi", float(first["rssi"]))
                except: pass
            if "snr" in first:
                try: point.field("snr", float(first.get("snr")))
                except: pass

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        log.info("Gravado: device=%s time=%s fields=%s", device_id, timestamp.isoformat(), list(measures.keys()))
    except Exception as e:
        log.exception("Erro ao escrever em Influx: %s", e)

def main():
    client = mqtt.Client(client_id="ttn-to-influx")
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.tls_set(ca_certs=certifi.where(), cert_reqs=ssl.CERT_REQUIRED)
    client.tls_insecure_set(False)

    client.on_connect = on_connect
    client.on_message = on_message

    while True:
        try:
            client.connect(MQTT_SERVER, MQTT_PORT, keepalive=60)
            client.loop_forever()
        except KeyboardInterrupt:
            log.info("Interrompido pelo utilizador.")
            try: client.disconnect()
            except: pass
            break
        except Exception as e:
            log.exception("Erro MQTT: %s — retry em 10s", e)
            time.sleep(10)

if __name__ == "__main__":
    main()
