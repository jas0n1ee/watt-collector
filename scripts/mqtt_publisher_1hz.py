#!/usr/bin/env python3
"""Publish a command payload to MQTT at a fixed rate (default 1 Hz)."""

import argparse
import signal
import sys
import time
from typing import Optional

import paho.mqtt.client as mqtt


_stop = False


def _handle_signal(_signum, _frame):
    global _stop
    _stop = True


def _parse_hex_payload(value: str) -> bytes:
    cleaned = value.replace(" ", "").replace("0x", "")
    if len(cleaned) % 2 != 0:
        raise ValueError("Hex payload length must be even")
    return bytes.fromhex(cleaned)


def _build_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish MQTT payload at a fixed interval.")
    parser.add_argument("--host", required=True, help="MQTT broker host/IP")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--username", help="MQTT username")
    parser.add_argument("--password", help="MQTT password")
    parser.add_argument("--topic", default="SmartHome/ElectricMeterCMD", help="Publish topic")
    parser.add_argument("--qos", type=int, choices=[0, 1, 2], default=0, help="MQTT QoS")
    parser.add_argument("--retain", action="store_true", help="Set retain flag")
    parser.add_argument("--interval", type=float, default=1.0, help="Publish interval in seconds")
    parser.add_argument("--count", type=int, default=0, help="Number of messages to send (0 = forever)")

    payload_group = parser.add_mutually_exclusive_group(required=True)
    payload_group.add_argument("--payload", help="String payload (UTF-8)")
    payload_group.add_argument("--payload-hex", help="Hex payload, e.g. '68 32 00 68 11 04 33 33' ")

    return parser.parse_args()


def _connect_client(args: argparse.Namespace) -> mqtt.Client:
    client = mqtt.Client()
    if args.username is not None:
        client.username_pw_set(args.username, args.password)

    def on_connect(_client, _userdata, _flags, rc):
        if rc != 0:
            raise RuntimeError(f"MQTT connect failed with rc={rc}")

    client.on_connect = on_connect
    client.connect(args.host, args.port, keepalive=60)
    client.loop_start()
    return client


def _resolve_payload(args: argparse.Namespace) -> bytes:
    if args.payload is not None:
        return args.payload.encode("utf-8")
    return _parse_hex_payload(args.payload_hex)


def main() -> int:
    args = _build_args()
    payload = _resolve_payload(args)

    client = _connect_client(args)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    sent = 0
    next_time = time.monotonic()

    try:
        while not _stop:
            now = time.monotonic()
            if now < next_time:
                time.sleep(min(0.05, next_time - now))
                continue

            result = client.publish(args.topic, payload, qos=args.qos, retain=args.retain)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"Publish failed: rc={result.rc}", file=sys.stderr)

            sent += 1
            if args.count > 0 and sent >= args.count:
                break

            next_time += args.interval

    finally:
        client.loop_stop()
        client.disconnect()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
