#!/usr/bin/env python3
"""
DL/T645 电表命令客户端：通过 MQTT 发送命令并解析响应。

支持命令:
  - read_meter_id: 读取电表表号（使用广播地址 AA AA AA AA AA AA）
  - read_energy: 读取正向有功总电能（需指定电表地址）

使用示例:
    # 读取表号（广播地址）
    python3 mqtt_command_client.py --cmd read_meter_id
    
    # 读取电能（指定地址）
    python3 mqtt_command_client.py --cmd read_energy --addr "66 55 44 33 22 11"
    
    # 使用自定义超时
    python3 mqtt_command_client.py --cmd read_meter_id --timeout 15

技术说明:
    - 自动处理 Elfin-EW11A 流式分片数据
    - 强制发送间隔 ≥ 1 秒（跨进程锁）
    - 自动验证 DL/T645 帧格式（起始符、校验和、结束符）
    - 数据域自动减 33H 还原
"""

import argparse
import signal
import sys
import time
import threading
from dataclasses import dataclass
from typing import Optional, Callable

import paho.mqtt.client as mqtt

# DL/T645 命令定义
COMMANDS = {
    "read_meter_id": {
        "name": "读取电表表号",
        "request": "FE FE FE FE 68 AA AA AA AA AA AA 68 11 04 33 33 34 33 AE 16",
        "data_id": "00 00 01 00",  # 减 33H 后的数据标识
    },
    "read_energy": {
        "name": "读取正向有功总电能",
        # 数据标识 00 00 00 00（加 33H → 33 33 33 33），校验码 5A
        "request": "FE FE FE FE 68 AA AA AA AA AA AA 68 11 04 33 33 33 33 5A 16",
        "data_id": "00 00 00 00",  # 正向有功总电能
    },
}

# MQTT 默认配置
DEFAULT_BROKER = "10.0.0.10"
DEFAULT_PORT = 1883
CMD_TOPIC = "SmartHome/ElectricMeterCMD"
RESP_TOPIC = "SmartHome/ElectricMeterRESPONSE"

# 全局状态
_response_buffer = bytearray()  # 响应数据缓冲区
_buffer_lock = threading.Lock()
_response_complete = threading.Event()
_expected_length = 0  # 期望的帧长度
_stop = False

# 发送间隔限制（秒）
MIN_SEND_INTERVAL = 1.0
_last_send_time = 0.0

# 跨进程间隔锁文件
import os
LOCK_FILE = "/tmp/mqtt_last_send.lock"

def _check_and_wait_interval():
    """检查并等待发送间隔，支持跨进程限制。"""
    global _last_send_time
    
    # 尝试读取上次发送时间
    last_time = 0.0
    try:
        if os.path.exists(LOCK_FILE):
            with open(LOCK_FILE, 'r') as f:
                last_time = float(f.read().strip())
    except (ValueError, IOError):
        last_time = 0.0
    
    # 使用进程内和进程间的最大值
    last_time = max(last_time, _last_send_time)
    
    current_time = time.time()
    elapsed = current_time - last_time
    
    if elapsed < MIN_SEND_INTERVAL:
        wait_time = MIN_SEND_INTERVAL - elapsed
        print(f"[发送间隔] 距离上次发送仅 {elapsed:.2f} 秒，需等待 {wait_time:.2f} 秒...")
        time.sleep(wait_time)
        current_time = time.time()
    
    # 更新发送时间
    _last_send_time = current_time
    try:
        with open(LOCK_FILE, 'w') as f:
            f.write(str(current_time))
    except IOError:
        pass


def _handle_signal(_signum, _frame):
    global _stop
    _stop = True


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


def _add33(data: bytes) -> bytes:
    """数据域加 33H 编码。"""
    return bytes((b + 0x33) & 0xFF for b in data)


def _calc_checksum(data: bytes) -> int:
    """计算 DL/T645 校验和（从帧起始符 68H 到校验码前所有字节的和，取低8位）。"""
    return sum(data) & 0xFF


def _verify_frame(frame: bytes) -> tuple[bool, str]:
    """
    验证 DL/T645 帧格式。
    返回: (是否有效, 错误信息)
    """
    if len(frame) < 12:
        return False, f"帧太短 ({len(frame)} 字节)"
    
    # 查找帧头（跳过前导 FE）
    start = 0
    while start < len(frame) - 1 and not (frame[start] == 0x68 and frame[start+1] != 0xFE):
        start += 1
    
    if start >= len(frame) - 1:
        return False, "未找到帧起始符 68H"
    
    # 解析帧结构
    try:
        # 起始符 68H
        if frame[start] != 0x68:
            return False, "帧起始符错误"
        
        # 地址域（6字节）
        addr = frame[start+1:start+7]
        
        # 第二个起始符 68H
        if frame[start+7] != 0x68:
            return False, "第二个起始符错误"
        
        # 控制码
        control = frame[start+8]
        
        # 数据长度
        data_len = frame[start+9]
        
        # 检查帧长度
        expected_len = 12 + data_len  # 68H + 6字节地址 + 68H + 控制码 + 长度 + 数据 + 校验 + 结束符
        if len(frame) < start + expected_len:
            return False, f"帧长度不足，期望 {expected_len} 字节，实际 {len(frame) - start} 字节"
        
        # 校验码（倒数第二字节）
        checksum_pos = start + expected_len - 2
        calc_checksum = sum(frame[start:checksum_pos]) & 0xFF
        recv_checksum = frame[checksum_pos]
        
        if calc_checksum != recv_checksum:
            return False, f"校验错误: 计算值 {calc_checksum:02X}，接收值 {recv_checksum:02X}"
        
        # 结束符 16H
        if frame[checksum_pos + 1] != 0x16:
            return False, "帧结束符错误"
        
        return True, ""
        
    except IndexError:
        return False, "帧结构解析失败"


def _parse_response(frame: bytes) -> dict:
    """
    解析 DL/T645 响应帧，返回解析结果字典。
    """
    result = {
        "raw": _bytes_to_hex(frame),
        "is_valid": False,
        "error": "",
        "meter_addr": "",
        "control_code": 0,
        "control_desc": "",
        "data_len": 0,
        "data_raw": b"",
        "data_decoded": b"",
        "data_type": "",
        "value": None,
    }
    
    is_valid, error = _verify_frame(frame)
    if not is_valid:
        result["error"] = error
        return result
    
    result["is_valid"] = True
    
    # 查找实际帧起始位置（跳过前导 FE）
    start = 0
    while start < len(frame) - 1 and not (frame[start] == 0x68 and frame[start+1] != 0xFE):
        start += 1
    
    # 解析地址域
    addr_bytes = frame[start+1:start+7]
    result["meter_addr"] = _bytes_to_hex(addr_bytes[::-1])  # 倒序显示
    
    # 控制码
    control = frame[start+8]
    result["control_code"] = control
    
    # 控制码含义
    if control == 0x91:
        result["control_desc"] = "正常响应（读数据）"
    elif control == 0xB1:
        result["control_desc"] = "异常响应"
    elif control == 0xD1:
        result["control_desc"] = "异常响应（无请求数据）"
    else:
        result["control_desc"] = f"未知控制码 {control:02X}"
    
    # 数据长度
    data_len = frame[start+9]
    result["data_len"] = data_len
    
    if data_len > 0:
        # 提取数据域
        data_start = start + 10
        data_raw = frame[data_start:data_start + data_len]
        result["data_raw"] = data_raw
        
        # 减 33H 还原
        data_decoded = _sub33(data_raw)
        result["data_decoded"] = data_decoded
        
        # 根据控制码解析数据
        if control == 0x91 and data_len >= 4:
            # 前4字节是数据标识
            data_id = data_decoded[:4]
            data_id_hex = _bytes_to_hex(data_id, "")
            
            # 根据数据标识解析
            DATA_IDS = {
                "00000100": ("电表表号", "bcd", 1),      # DI=00000100H
                "00000000": ("正向有功总电能", "energy", 0.01),  # DI=00000000H
                "00010000": ("正向有功尖电能", "energy", 0.01),  # DI=00010000H
                "00020000": ("正向有功峰电能", "energy", 0.01),  # DI=00020000H
                "00030000": ("正向有功平电能", "energy", 0.01),  # DI=00030000H
                "00040000": ("正向有功谷电能", "energy", 0.01),  # DI=00040000H
            }
            
            data_info = DATA_IDS.get(data_id_hex, (f"未知数据标识 {data_id_hex}", "raw", 1))
            result["data_type"] = data_info[0]
            parse_type = data_info[1]
            unit = data_info[2]
            
            if data_len > 4:
                value_data = data_decoded[4:]
                # 倒序排列（低位在前）
                value_bcd = value_data[::-1]
                
                if parse_type == "bcd":
                    # BCD 码直接显示
                    result["value"] = _bytes_to_hex(value_bcd, "")
                elif parse_type == "energy":
                    # 电能数据解析（BCD 码，单位 kWh）
                    try:
                        value = 0
                        for b in value_bcd:
                            high = b >> 4
                            low = b & 0x0F
                            if high > 9 or low > 9:
                                # 不是有效的 BCD 码
                                result["value"] = f"原始数据: {_bytes_to_hex(value_bcd)}"
                                break
                            value = value * 100 + (high * 10 + low)
                        else:
                            result["value"] = f"{value * unit:.2f} kWh"
                    except Exception as e:
                        result["value"] = f"解析失败: {e}"
                else:
                    result["value"] = _bytes_to_hex(value_bcd)
    
    return result


def _build_request_with_addr(base_request: str, meter_addr: Optional[str] = None) -> bytes:
    """
    构建请求帧，可选指定电表地址。
    """
    data = _parse_hex(base_request)
    
    if meter_addr:
        # 替换广播地址为实际地址
        addr_bytes = _parse_hex(meter_addr)[::-1]  # 倒序
        if len(addr_bytes) != 6:
            raise ValueError("电表地址必须是6字节")
        
        # 构建新帧
        # 前导 FE + 起始符 68H + 地址(6) + 68H + 控制码 + 长度 + 数据 + 校验 + 16H
        new_frame = bytearray()
        
        # 前导 FE
        fe_count = 0
        while fe_count < len(data) and data[fe_count] == 0xFE:
            new_frame.append(0xFE)
            fe_count += 1
        
        # 帧起始
        frame_start = fe_count
        new_frame.append(0x68)  # 起始符
        new_frame.extend(addr_bytes)  # 地址域
        new_frame.append(0x68)  # 第二个起始符
        
        # 控制码、数据长度、数据域（从原帧复制）
        ctrl_start = frame_start + 8
        new_frame.extend(data[ctrl_start:ctrl_start + 2])  # 控制码 + 数据长度
        data_len = data[ctrl_start + 1]
        new_frame.extend(data[ctrl_start + 2:ctrl_start + 2 + data_len])  # 数据域
        
        # 计算校验和（从帧起始符 68H 开始到数据域结束）
        frame_content = new_frame[fe_count:]  # 从第一个 68H 开始
        checksum = sum(frame_content) & 0xFF
        new_frame.append(checksum)
        
        # 结束符
        new_frame.append(0x16)
        
        return bytes(new_frame)
    
    return data


def _detect_frame_length(data: bytes) -> int:
    """
    从数据中提取期望的帧长度。
    DL/T645 帧结构：前导 FE + 68H + 地址(6) + 68H + 控制码(1) + 长度(1) + 数据(N) + 校验(1) + 16H
    总长度 = 前导 FE 数量 + 12 + 数据长度
    """
    # 查找帧起始（跳过前导 FE）
    i = 0
    while i < len(data) and data[i] == 0xFE:
        i += 1
    
    if i >= len(data) or data[i] != 0x68:
        return 0
    
    # 检查是否有足够的长度来读取数据长度字段
    # i 指向第一个 68H，数据长度在 i + 9
    if len(data) < i + 10:
        return 0  # 数据不够，暂时无法确定长度
    
    data_len = data[i + 9]
    fe_count = i  # 前导 FE 的数量
    
    # 完整帧长度 = 前导 FE + 68H + 6字节地址 + 68H + 控制码 + 长度字节 + 数据域 + 校验 + 16H
    total_len = fe_count + 1 + 6 + 1 + 1 + 1 + data_len + 1 + 1
    return total_len


def _find_complete_frame(data: bytes) -> Optional[bytes]:
    """
    在缓冲区中查找完整的 DL/T645 帧。
    返回完整帧或 None。
    """
    if len(data) < 12:
        return None
    
    # 查找帧起始（第一个 68H，前面可能有 FE 前导）
    for i in range(len(data)):
        if data[i] == 0x68:
            # 检查是否有第二个 68H（在 i+7 位置）
            if i + 7 < len(data) and data[i + 7] == 0x68:
                # 检查是否有数据长度字段
                if i + 9 < len(data):
                    data_len = data[i + 9]
                    # 计算完整帧需要的字节数
                    # 从当前位置 i 开始：68H + 6地址 + 68H + 控制码 + 长度 + 数据 + 校验 + 16H
                    frame_len = 1 + 6 + 1 + 1 + 1 + data_len + 1 + 1
                    
                    # 包含前导 FE 的总长度
                    start_pos = i
                    while start_pos > 0 and data[start_pos - 1] == 0xFE:
                        start_pos -= 1
                    
                    total_len = (i - start_pos) + frame_len
                    
                    if len(data) >= start_pos + total_len:
                        # 检查结束符
                        end_pos = start_pos + total_len - 1
                        if data[end_pos] == 0x16:
                            return bytes(data[start_pos:end_pos + 1])
    
    return None


def _on_message(client, userdata, msg):
    """MQTT 消息回调。"""
    global _response_buffer, _expected_length
    
    print(f"[收到片段] 长度: {len(msg.payload):3d} 字节 | {_bytes_to_hex(msg.payload)}")
    
    with _buffer_lock:
        _response_buffer.extend(msg.payload)
        
        # 尝试查找完整帧
        complete_frame = _find_complete_frame(_response_buffer)
        if complete_frame:
            _response_complete.set()
            return


def _on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[连接成功] 已订阅 {RESP_TOPIC}")
        client.subscribe(RESP_TOPIC)
    else:
        print(f"[连接失败] 错误码: {rc}")


def main():
    parser = argparse.ArgumentParser(description="DL/T645 电表命令客户端")
    parser.add_argument("--host", default=DEFAULT_BROKER, help=f"MQTT Broker IP (默认: {DEFAULT_BROKER})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"MQTT 端口 (默认: {DEFAULT_PORT})")
    parser.add_argument("--cmd", choices=list(COMMANDS.keys()), default="read_energy",
                        help="要执行的命令")
    parser.add_argument("--addr", help="电表地址（6字节十六进制），不指定则使用广播地址 AA AA AA AA AA AA")
    parser.add_argument("--timeout", type=float, default=5.0, help="等待响应超时时间（秒）")
    
    args = parser.parse_args()
    
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    
    # 获取命令配置
    cmd_config = COMMANDS[args.cmd]
    
    print(f"=" * 60)
    print(f"DL/T645 电表命令客户端")
    print(f"=" * 60)
    print(f"命令: {cmd_config['name']}")
    print(f"Broker: {args.host}:{args.port}")
    print(f"命令 Topic: {CMD_TOPIC}")
    print(f"响应 Topic: {RESP_TOPIC}")
    print(f"-" * 60)
    
    # 构建请求帧
    try:
        if args.addr:
            request_bytes = _build_request_with_addr(cmd_config["request"], args.addr)
        else:
            request_bytes = _parse_hex(cmd_config["request"])
    except ValueError as e:
        print(f"[错误] 构建请求帧失败: {e}")
        return 1
    
    print(f"[请求报文] {_bytes_to_hex(request_bytes)}")
    
    # 连接 MQTT
    client = mqtt.Client()
    client.on_connect = _on_connect
    client.on_message = _on_message
    
    try:
        client.connect(args.host, args.port, keepalive=60)
        client.loop_start()
        
        # 等待连接建立
        time.sleep(0.5)
        
        # 检查发送间隔（跨进程）
        _check_and_wait_interval()
        
        # 发送命令
        print(f"[发送命令] ...")
        result = client.publish(CMD_TOPIC, request_bytes, qos=0)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            print(f"[错误] 发送失败: rc={result.rc}")
            return 1
        
        # 等待响应（等待完整帧或超时）
        print(f"[等待响应] 超时时间: {args.timeout} 秒...")
        
        complete = _response_complete.wait(timeout=args.timeout)
        
        if not complete:
            print(f"\n[超时] 未在 {args.timeout} 秒内收到完整响应")
            # 打印已收到的原始数据
            with _buffer_lock:
                if _response_buffer:
                    print(f"[已收数据] {_bytes_to_hex(bytes(_response_buffer))}")
            return 1
        
        # 获取完整帧
        with _buffer_lock:
            complete_frame = _find_complete_frame(_response_buffer)
        
        if complete_frame:
            print(f"\n{'=' * 60}")
            print(f"完整帧接收成功")
            print(f"{'=' * 60}")
            print(f"[完整报文] {_bytes_to_hex(complete_frame)}")
            
            parsed = _parse_response(complete_frame)
            
            if parsed["is_valid"]:
                print(f"✓ 帧格式: 有效")
                print(f"  电表地址: {parsed['meter_addr']}")
                print(f"  控制码: {parsed['control_code']:02X} ({parsed['control_desc']})")
                print(f"  数据长度: {parsed['data_len']}")
                print(f"  原始数据域: {_bytes_to_hex(parsed['data_raw'])}")
                print(f"  还原数据域: {_bytes_to_hex(parsed['data_decoded'])}")
                
                if "data_type" in parsed:
                    print(f"\n  数据类型: {parsed['data_type']}")
                if parsed["value"] is not None:
                    print(f"  解析值: {parsed['value']}")
            else:
                print(f"✗ 帧格式: 无效")
                print(f"  错误: {parsed['error']}")
            
            print(f"{'=' * 60}")
        
    except Exception as e:
        print(f"[错误] {e}")
        return 1
    finally:
        client.loop_stop()
        client.disconnect()
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
