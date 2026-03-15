#!/usr/bin/env python3
"""
DL/T645 电表自动数据采集器（支持自动寻址）

功能：
    - 支持自动寻址：通过广播读取表号，自动获取电表地址
    - 支持固定地址：通过参数指定电表地址
    - 按天分档存储为 CSV 格式
    - 失败自动重试（最多15次）

使用示例：
    # 自动寻址模式（推荐）：先广播读表号，再用表号读电能
    python3 mqtt_collector.py
    
    # 固定地址模式：直接使用指定地址读电能
    python3 mqtt_collector.py --addr "66 55 44 33 22 11"
    
    # 自定义配置
    python3 mqtt_collector.py --interval 900 --data-dir /var/lib/meter_data

Docker 运行：
    docker run -v /host/data:/app/.data mqtt-collector --interval 60
"""

import argparse
import csv
import os
import signal
import sys
import threading
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple

import paho.mqtt.client as mqtt

# MQTT 默认配置
DEFAULT_BROKER = "10.0.0.10"
DEFAULT_PORT = 1883
CMD_TOPIC = "SmartHome/ElectricMeterCMD"
RESP_TOPIC = "SmartHome/ElectricMeterRESPONSE"

# DL/T645 命令
READ_METER_ID_CMD = "FE FE FE FE 68 AA AA AA AA AA AA 68 11 04 33 33 34 33 AE 16"
READ_ENERGY_CMD = "FE FE FE FE 68 AA AA AA AA AA AA 68 11 04 33 33 33 33 5A 16"

# 全局状态
_response_buffer = bytearray()
_buffer_lock = threading.Lock()
_response_complete = threading.Event()
_stop = False

# 跨进程间隔锁
LOCK_FILE = "/tmp/mqtt_collector.lock"
MIN_SEND_INTERVAL = 5.0

# 时区设置：如果设置了 TZ 环境变量，应用时区
if 'TZ' in os.environ:
    time.tzset()
    print(f"[时区] 使用环境变量 TZ={os.environ['TZ']}")


def _handle_signal(signum, frame):
    global _stop
    _stop = True
    print(f"\n[信号] 收到信号 {signum}，准备退出...")


def _parse_hex(hex_str: str) -> bytes:
    """解析十六进制字符串为字节。"""
    cleaned = hex_str.replace(" ", "").replace("0x", "").upper()
    if len(cleaned) % 2 != 0:
        raise ValueError("十六进制字符串长度必须为偶数")
    return bytes.fromhex(cleaned)


def _bytes_to_hex(data: bytes, sep: str = " ") -> str:
    """字节转为十六进制字符串。"""
    return sep.join(f"{b:02X}" for b in data)


def _sub33(data: bytes) -> bytes:
    """数据域减 33H 还原。"""
    return bytes((b - 0x33) & 0xFF for b in data)


def _find_complete_frame(data: bytes) -> Optional[bytes]:
    """在缓冲区中查找完整的 DL/T645 帧。"""
    if len(data) < 12:
        return None
    
    for i in range(len(data)):
        if data[i] == 0x68:
            if i + 7 < len(data) and data[i + 7] == 0x68:
                if i + 9 < len(data):
                    data_len = data[i + 9]
                    frame_len = 1 + 6 + 1 + 1 + 1 + data_len + 1 + 1
                    
                    start_pos = i
                    while start_pos > 0 and data[start_pos - 1] == 0xFE:
                        start_pos -= 1
                    
                    total_len = (i - start_pos) + frame_len
                    
                    if len(data) >= start_pos + total_len:
                        end_pos = start_pos + total_len - 1
                        if data[end_pos] == 0x16:
                            return bytes(data[start_pos:end_pos + 1])
    return None


def _build_request_with_addr(base_request: str, meter_addr: str) -> bytes:
    """构建带指定地址的请求帧。"""
    data = _parse_hex(base_request)
    addr_bytes = _parse_hex(meter_addr)[::-1]  # 倒序
    
    if len(addr_bytes) != 6:
        raise ValueError("电表地址必须是6字节")
    
    new_frame = bytearray()
    
    # 前导 FE
    fe_count = 0
    while fe_count < len(data) and data[fe_count] == 0xFE:
        new_frame.append(0xFE)
        fe_count += 1
    
    # 帧内容
    new_frame.append(0x68)
    new_frame.extend(addr_bytes)
    new_frame.append(0x68)
    
    # 控制码、长度、数据域
    ctrl_start = fe_count + 8
    new_frame.extend(data[ctrl_start:ctrl_start + 2])
    data_len = data[ctrl_start + 1]
    new_frame.extend(data[ctrl_start + 2:ctrl_start + 2 + data_len])
    
    # 校验和
    frame_content = new_frame[fe_count:]
    checksum = sum(frame_content) & 0xFF
    new_frame.append(checksum)
    new_frame.append(0x16)
    
    return bytes(new_frame)


def _on_message(client, userdata, msg):
    """MQTT 消息回调。"""
    global _response_buffer
    
    with _buffer_lock:
        _response_buffer.extend(msg.payload)
        complete_frame = _find_complete_frame(_response_buffer)
        if complete_frame:
            _response_complete.set()


def _on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(RESP_TOPIC)


def _update_lock_file():
    """更新发送时间锁文件。"""
    try:
        with open(LOCK_FILE, 'w') as f:
            f.write(str(time.time()))
    except IOError:
        pass


def _wait_min_interval():
    """确保最小发送间隔（跨进程）。"""
    try:
        if os.path.exists(LOCK_FILE):
            with open(LOCK_FILE, 'r') as f:
                last_time = float(f.read().strip())
        else:
            last_time = 0.0
    except (ValueError, IOError):
        last_time = 0.0
    
    elapsed = time.time() - last_time
    if elapsed < MIN_SEND_INTERVAL:
        sleep_time = MIN_SEND_INTERVAL - elapsed
        print(f"    [间隔等待] {sleep_time:.2f} 秒...")
        time.sleep(sleep_time)
    
    _update_lock_file()


def _send_and_wait(client: mqtt.Client, request_bytes: bytes, 
                   timeout: float) -> Optional[bytes]:
    """
    发送请求并等待完整响应。
    成功返回完整帧，失败返回 None。
    """
    global _response_buffer, _response_complete
    
    # 清空缓冲区
    with _buffer_lock:
        _response_buffer.clear()
    _response_complete.clear()
    
    # 等待最小间隔
    _wait_min_interval()
    
    # 发送命令
    result = client.publish(CMD_TOPIC, request_bytes, qos=0)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        return None
    
    # 等待响应
    if _response_complete.wait(timeout=timeout):
        with _buffer_lock:
            return _find_complete_frame(bytes(_response_buffer))
    
    return None


def _parse_meter_id(frame: bytes) -> Optional[str]:
    """
    从响应帧中解析电表表号/地址。
    返回格式化后的地址字符串（如 "66 55 44 33 22 11"）。
    """
    if not frame:
        return None
    
    # 查找帧起始
    start = 0
    while start < len(frame) and frame[start] == 0xFE:
        start += 1
    
    if start + 9 >= len(frame):
        return None
    
    # 验证结构
    if frame[start] != 0x68 or frame[start + 7] != 0x68:
        return None
    
    # 提取地址域（6字节，倒序存储）
    addr_bytes = frame[start + 1:start + 7]
    # 倒序还原为正常显示顺序
    addr_normal = addr_bytes[::-1]
    
    return _bytes_to_hex(addr_normal)


def _parse_energy(frame: bytes) -> Optional[float]:
    """
    从响应帧中解析电能值（kWh）。
    """
    if not frame:
        return None
    
    start = 0
    while start < len(frame) and frame[start] == 0xFE:
        start += 1
    
    if start + 9 >= len(frame):
        return None
    
    if frame[start] != 0x68 or frame[start + 7] != 0x68:
        return None
    
    control = frame[start + 8]
    data_len = frame[start + 9]
    
    if control != 0x91 or data_len < 8:
        return None
    
    # 提取数据域
    data_start = start + 10
    data_raw = frame[data_start:data_start + data_len]
    data_decoded = _sub33(data_raw)
    
    # 电能数据（后4字节）
    if len(data_decoded) < 8:
        return None
    
    energy_data = data_decoded[4:8]
    energy_bcd = energy_data[::-1]
    
    # BCD 转数值
    try:
        value = 0
        for b in energy_bcd:
            high = b >> 4
            low = b & 0x0F
            if high > 9 or low > 9:
                return None
            value = value * 100 + (high * 10 + low)
        return value / 100
    except Exception:
        return None


def _discover_meter_addr(client: mqtt.Client, timeout: float = 8.0, 
                         max_retries: int = 3) -> Optional[str]:
    """
    通过广播地址读取电表表号，支持重试。
    成功返回地址字符串，失败返回 None。
    """
    print("[自动寻址] 发送广播命令读取表号...")
    
    request = _parse_hex(READ_METER_ID_CMD)
    
    for attempt in range(max_retries):
        if _stop:
            return None
        
        if attempt > 0:
            print(f"[自动寻址] 第 {attempt + 1} 次尝试...")
        
        frame = _send_and_wait(client, request, timeout)
        
        if frame:
            addr = _parse_meter_id(frame)
            if addr:
                print(f"[自动寻址] 成功获取电表地址: {addr}")
                return addr
        
        if attempt < max_retries - 1:
            print(f"[自动寻址] 未收到响应，等待重试...")
            time.sleep(1.0)
    
    print("[自动寻址] 失败，未找到电表")
    return None


def _read_energy_with_retry(client: mqtt.Client, meter_addr: str,
                            max_retries: int = 15, timeout: float = 5.0) -> Tuple[Optional[float], Optional[str]]:
    """
    带重试的电能读取。
    返回: (电能值, 原始帧字符串)
    """
    request = _build_request_with_addr(READ_ENERGY_CMD, meter_addr)
    
    for attempt in range(max_retries):
        if _stop:
            return None, None
        
        frame = _send_and_wait(client, request, timeout)
        if frame:
            energy = _parse_energy(frame)
            if energy is not None:
                return energy, _bytes_to_hex(frame)
        
        if attempt < max_retries - 1:
            time.sleep(0.5)
    
    return None, None


def _get_csv_path(data_dir: Path, current_date: date) -> Path:
    """获取指定日期的 CSV 文件路径。"""
    filename = f"electric_meter_{current_date.isoformat()}.csv"
    return data_dir / filename


def _ensure_data_dir(data_dir: Path):
    """确保数据目录存在。"""
    data_dir.mkdir(parents=True, exist_ok=True)


def _init_csv_file(filepath: Path):
    """初始化 CSV 文件。"""
    if not filepath.exists():
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'energy_kwh', 'raw_frame'])
        print(f"[文件] 创建新文件: {filepath}")


def _append_record(filepath: Path, timestamp: str, energy: float, raw_frame: str):
    """追加一条记录到 CSV 文件。"""
    with open(filepath, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([timestamp, f"{energy:.2f}", raw_frame])
        f.flush()
        os.fsync(f.fileno())


def main():
    parser = argparse.ArgumentParser(description="DL/T645 电表自动数据采集器（支持自动寻址）")
    parser.add_argument("--host", default=DEFAULT_BROKER, help=f"MQTT Broker IP (默认: {DEFAULT_BROKER})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"MQTT 端口 (默认: {DEFAULT_PORT})")
    parser.add_argument("--addr", help="电表地址（6字节十六进制），不指定则自动寻址")
    parser.add_argument("--interval", type=int, default=60, help="采集间隔（秒），默认 60")
    parser.add_argument("--data-dir", default=".data", help="数据存储目录（默认: .data）")
    parser.add_argument("--timeout", type=float, default=5.0, help="单次读取超时（秒），默认 5")
    parser.add_argument("--discover-timeout", type=float, default=10.0, help="寻址超时（秒），默认 10")
    
    args = parser.parse_args()
    
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    
    # 准备数据目录
    data_dir = Path(args.data_dir)
    _ensure_data_dir(data_dir)
    
    print("=" * 60)
    print("DL/T645 电表自动数据采集器")
    print("=" * 60)
    print(f"Broker: {args.host}:{args.port}")
    print(f"采集间隔: {args.interval} 秒")
    print(f"数据目录: {data_dir.absolute()}")
    print(f"工作模式: {'固定地址' if args.addr else '自动寻址'}")
    print("=" * 60)
    
    # 连接 MQTT
    client = mqtt.Client()
    client.on_connect = _on_connect
    client.on_message = _on_message
    
    try:
        client.connect(args.host, args.port, keepalive=60)
        client.loop_start()
        time.sleep(0.5)
        
        # 确定电表地址
        meter_addr: Optional[str] = args.addr
        
        if not meter_addr:
            # 自动寻址模式
            print("[启动] 进入自动寻址模式...")
            meter_addr = _discover_meter_addr(client, args.discover_timeout)
            
            if not meter_addr:
                print("[错误] 自动寻址失败，无法继续")
                return 1
            
            print(f"[启动] 将使用地址 {meter_addr} 进行电能采集")
            # 寻址后已经有 ≥1 秒间隔（在 _send_and_wait 中处理）
        else:
            print(f"[启动] 使用固定地址: {meter_addr}")
        
        print("[启动] 开始采集循环...")
        print("-" * 60)
        
        last_energy: Optional[float] = None
        current_file_date: Optional[date] = None
        csv_path: Optional[Path] = None
        
        while not _stop:
            now = datetime.now()
            current_date = now.date()
            timestamp_str = now.isoformat()
            
            # 检查是否需要切换文件（跨天）
            if current_date != current_file_date:
                current_file_date = current_date
                csv_path = _get_csv_path(data_dir, current_date)
                _init_csv_file(csv_path)
                print(f"[日期切换] 当前文件: {csv_path.name}")
            
            # 执行采集
            print(f"[{timestamp_str}] 正在采集...", end=" ", flush=True)
            energy, raw_frame = _read_energy_with_retry(client, meter_addr, max_retries=15, timeout=args.timeout)
            
            if energy is not None:
                _append_record(csv_path, timestamp_str, energy, raw_frame)
                print(f"✓ 电能: {energy:.2f} kWh")
                last_energy = energy
            else:
                print(f"✗ 采集失败（已重试15次），跳过本次")
            
            # 计算下次采集时间
            next_time = now.timestamp() + args.interval
            sleep_time = next_time - time.time()
            
            while sleep_time > 0 and not _stop:
                time.sleep(min(0.1, sleep_time))
                sleep_time = next_time - time.time()
        
        print("\n[退出] 采集已停止")
        
    except Exception as e:
        print(f"\n[错误] {e}")
        return 1
    finally:
        client.loop_stop()
        client.disconnect()
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
