#!/usr/bin/env python3
"""
电表读取成功率测试脚本

功能：
    - 以固定间隔（默认1秒）读取电能数据
    - 统计读取成功率（成功返回/总次数）
    - 验证数据合理性（电能变化量应在合理范围内）
    - 生成测试报告

合理性验证规则：
    - 电能值应为有效数字（非None）
    - 相邻两次读数的变化量应 < 1 kWh（即 < 100 个单位，因为单位是0.01kWh）

使用示例：
    # 默认测试60秒（60次读取）
    python3 test_read_success_rate.py
    
    # 测试100次，每2秒一次
    python3 test_read_success_rate.py --count 100 --interval 2
    
    # 指定电表地址
    python3 test_read_success_rate.py --addr "11 22 33 44 55 66" --count 30
"""

import argparse
import signal
import sys
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List
from pathlib import Path

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
LOCK_FILE = "/tmp/mqtt_test.lock"
MIN_SEND_INTERVAL = 1.0


@dataclass
class TestResult:
    """单次测试结果"""
    timestamp: str
    success: bool
    energy: Optional[float] = None
    raw_frame: str = ""
    error_reason: str = ""
    energy_delta: Optional[float] = None  # 与上次的差值（kWh）
    reasonable: bool = True  # 数据是否合理


@dataclass
class TestSummary:
    """测试汇总统计"""
    total_count: int = 0
    success_count: int = 0
    fail_count: int = 0
    reasonable_count: int = 0
    unreasonable_count: int = 0
    results: List[TestResult] = field(default_factory=list)
    
    def add_result(self, result: TestResult):
        self.results.append(result)
        self.total_count += 1
        if result.success:
            self.success_count += 1
            if result.reasonable:
                self.reasonable_count += 1
            else:
                self.unreasonable_count += 1
        else:
            self.fail_count += 1
    
    @property
    def success_rate(self) -> float:
        if self.total_count == 0:
            return 0.0
        return (self.success_count / self.total_count) * 100
    
    @property
    def reasonableness_rate(self) -> float:
        if self.success_count == 0:
            return 0.0
        return (self.reasonable_count / self.success_count) * 100


def _handle_signal(signum, frame):
    global _stop
    _stop = True
    print(f"\n[信号] 收到信号 {signum}，准备停止测试...")


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
    addr_bytes = _parse_hex(meter_addr)[::-1]
    
    if len(addr_bytes) != 6:
        raise ValueError("电表地址必须是6字节")
    
    new_frame = bytearray()
    
    fe_count = 0
    while fe_count < len(data) and data[fe_count] == 0xFE:
        new_frame.append(0xFE)
        fe_count += 1
    
    new_frame.append(0x68)
    new_frame.extend(addr_bytes)
    new_frame.append(0x68)
    
    ctrl_start = fe_count + 8
    new_frame.extend(data[ctrl_start:ctrl_start + 2])
    data_len = data[ctrl_start + 1]
    new_frame.extend(data[ctrl_start + 2:ctrl_start + 2 + data_len])
    
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
    """确保最小发送间隔。"""
    import os
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
        time.sleep(sleep_time)
    
    _update_lock_file()


def _send_and_wait(client: mqtt.Client, request_bytes: bytes, 
                   timeout: float) -> Optional[bytes]:
    """发送请求并等待完整响应。"""
    global _response_buffer, _response_complete
    
    with _buffer_lock:
        _response_buffer.clear()
    _response_complete.clear()
    
    _wait_min_interval()
    
    result = client.publish(CMD_TOPIC, request_bytes, qos=0)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        return None
    
    if _response_complete.wait(timeout=timeout):
        with _buffer_lock:
            return _find_complete_frame(bytes(_response_buffer))
    
    return None


def _parse_meter_id(frame: bytes) -> Optional[str]:
    """从响应帧中解析电表地址。"""
    if not frame:
        return None
    
    start = 0
    while start < len(frame) and frame[start] == 0xFE:
        start += 1
    
    if start + 9 >= len(frame):
        return None
    
    if frame[start] != 0x68 or frame[start + 7] != 0x68:
        return None
    
    addr_bytes = frame[start + 1:start + 7]
    addr_normal = addr_bytes[::-1]
    
    return _bytes_to_hex(addr_normal)


def _parse_energy(frame: bytes) -> Optional[float]:
    """从响应帧中解析电能值（kWh）。"""
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
    
    data_start = start + 10
    data_raw = frame[data_start:data_start + data_len]
    data_decoded = _sub33(data_raw)
    
    if len(data_decoded) < 8:
        return None
    
    energy_data = data_decoded[4:8]
    energy_bcd = energy_data[::-1]
    
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
    """通过广播地址读取电表表号。"""
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
            time.sleep(1.0)
    
    print("[自动寻址] 失败，未找到电表")
    return None


def _read_once(client: mqtt.Client, meter_addr: str, timeout: float) -> TestResult:
    """单次读取电能数据，返回测试结果。"""
    timestamp = datetime.now().isoformat()
    
    try:
        request = _build_request_with_addr(READ_ENERGY_CMD, meter_addr)
        frame = _send_and_wait(client, request, timeout)
        
        if frame is None:
            return TestResult(
                timestamp=timestamp,
                success=False,
                error_reason="超时未收到响应"
            )
        
        energy = _parse_energy(frame)
        
        if energy is None:
            return TestResult(
                timestamp=timestamp,
                success=False,
                raw_frame=_bytes_to_hex(frame),
                error_reason="无法解析电能数据"
            )
        
        return TestResult(
            timestamp=timestamp,
            success=True,
            energy=energy,
            raw_frame=_bytes_to_hex(frame)
        )
        
    except Exception as e:
        return TestResult(
            timestamp=timestamp,
            success=False,
            error_reason=f"异常: {str(e)}"
        )


def _validate_reasonableness(result: TestResult, last_energy: Optional[float],
                             max_delta: float = 1.0) -> None:
    """
    验证数据合理性。
    
    Args:
        result: 当前测试结果
        last_energy: 上次读取的电能值
        max_delta: 最大允许变化量（kWh），默认1.0
    """
    if not result.success or result.energy is None:
        return
    
    if last_energy is None:
        # 第一次读取，无法验证变化量
        result.reasonable = True
        result.energy_delta = None
        return
    
    delta = abs(result.energy - last_energy)
    result.energy_delta = delta
    
    if delta > max_delta:
        result.reasonable = False
        result.error_reason = f"变化量过大: {delta:.2f} kWh (最大允许 {max_delta} kWh)"


def _print_report(summary: TestSummary, duration: float):
    """打印测试报告。"""
    print("\n" + "=" * 70)
    print("                        测试报告")
    print("=" * 70)
    
    print(f"\n【测试概况】")
    print(f"  总测试次数: {summary.total_count}")
    print(f"  测试耗时: {duration:.1f} 秒")
    print(f"  平均间隔: {duration/summary.total_count:.2f} 秒")
    
    print(f"\n【读取成功率】")
    print(f"  成功次数: {summary.success_count}")
    print(f"  失败次数: {summary.fail_count}")
    print(f"  成功率: {summary.success_rate:.1f}%")
    
    if summary.success_count > 0:
        print(f"\n【数据合理性】")
        print(f"  合理数据: {summary.reasonable_count}")
        print(f"  不合理数据: {summary.unreasonable_count}")
        print(f"  合理率: {summary.reasonableness_rate:.1f}%")
        print(f"  （变化量阈值: ≤ 1.0 kWh）")
    
    # 统计失败原因
    print(f"\n【失败原因分析】")
    fail_reasons = {}
    for r in summary.results:
        if not r.success:
            reason = r.error_reason if r.error_reason else "未知错误"
            fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
    
    if fail_reasons:
        for reason, count in sorted(fail_reasons.items(), key=lambda x: -x[1]):
            print(f"  - {reason}: {count} 次")
    else:
        print("  无失败记录")
    
    # 显示不合理数据详情
    if summary.unreasonable_count > 0:
        print(f"\n【不合理数据详情】")
        print(f"  {'序号':<6} {'时间':<26} {'电能值':<12} {'变化量':<12} 说明")
        print(f"  {'-'*65}")
        
        last_e = None
        for i, r in enumerate(summary.results, 1):
            if r.success and not r.reasonable:
                delta_str = f"{r.energy_delta:.2f} kWh" if r.energy_delta else "N/A"
                print(f"  {i:<6} {r.timestamp:<26} {r.energy:<12.2f} {delta_str:<12} {r.error_reason}")
    
    # 电能值范围
    energies = [r.energy for r in summary.results if r.success and r.energy is not None]
    if energies:
        print(f"\n【电能值统计】")
        print(f"  最小值: {min(energies):.2f} kWh")
        print(f"  最大值: {max(energies):.2f} kWh")
        print(f"  平均值: {sum(energies)/len(energies):.2f} kWh")
        if len(energies) > 1:
            print(f"  总变化量: {max(energies) - min(energies):.2f} kWh")
    
    print("\n" + "=" * 70)


def _save_detailed_report(summary: TestSummary, output_file: Path):
    """保存详细测试报告到文件。"""
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("timestamp,success,energy_kwh,energy_delta,reasonable,raw_frame,error_reason\n")
        
        for r in summary.results:
            energy_str = f"{r.energy:.2f}" if r.energy is not None else ""
            delta_str = f"{r.energy_delta:.4f}" if r.energy_delta is not None else ""
            f.write(f"{r.timestamp},{r.success},{energy_str},{delta_str},{r.reasonable},\"{r.raw_frame}\",\"{r.error_reason}\"\n")
    
    print(f"\n[报告] 详细数据已保存到: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="电表读取成功率测试")
    parser.add_argument("--host", default=DEFAULT_BROKER, help=f"MQTT Broker IP (默认: {DEFAULT_BROKER})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"MQTT 端口 (默认: {DEFAULT_PORT})")
    parser.add_argument("--addr", help="电表地址，不指定则自动寻址")
    parser.add_argument("--count", type=int, default=60, help="测试次数，默认 60")
    parser.add_argument("--interval", type=float, default=1.0, help="测试间隔（秒），默认 1.0")
    parser.add_argument("--timeout", type=float, default=5.0, help="单次读取超时（秒），默认 5")
    parser.add_argument("--max-delta", type=float, default=1.0, 
                        help="最大允许变化量（kWh），默认 1.0")
    parser.add_argument("--output", help="详细报告输出文件（CSV格式）")
    
    args = parser.parse_args()
    
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    
    print("=" * 70)
    print("                  电表读取成功率测试")
    print("=" * 70)
    print(f"Broker: {args.host}:{args.port}")
    print(f"测试次数: {args.count}")
    print(f"测试间隔: {args.interval} 秒")
    print(f"读取超时: {args.timeout} 秒")
    print(f"变化量阈值: ≤ {args.max_delta} kWh")
    print(f"工作模式: {'固定地址' if args.addr else '自动寻址'}")
    print("=" * 70)
    
    client = mqtt.Client()
    client.on_connect = _on_connect
    client.on_message = _on_message
    
    try:
        client.connect(args.host, args.port, keepalive=60)
        client.loop_start()
        time.sleep(0.5)
        
        # 确定电表地址
        meter_addr = args.addr
        
        if not meter_addr:
            print("\n[准备] 进入自动寻址模式...")
            meter_addr = _discover_meter_addr(client, args.timeout)
            if not meter_addr:
                print("[错误] 自动寻址失败，无法继续测试")
                return 1
        else:
            print(f"\n[准备] 使用固定地址: {meter_addr}")
        
        # 执行测试
        print(f"\n[开始] 执行 {args.count} 次读取测试...")
        print("-" * 70)
        
        summary = TestSummary()
        last_energy: Optional[float] = None
        start_time = time.time()
        
        for i in range(args.count):
            if _stop:
                print(f"\n[中断] 用户中断测试，已完成 {i}/{args.count} 次")
                break
            
            print(f"[{i+1}/{args.count}] ", end="", flush=True)
            
            result = _read_once(client, meter_addr, args.timeout)
            
            # 验证数据合理性
            _validate_reasonableness(result, last_energy, args.max_delta)
            
            # 记录结果
            summary.add_result(result)
            
            # 输出本次结果
            if result.success:
                status = "✓"
                if result.reasonable:
                    if result.energy_delta is not None:
                        print(f"{status} {result.energy:.2f} kWh (Δ{result.energy_delta:.3f})")
                    else:
                        print(f"{status} {result.energy:.2f} kWh (首次)")
                else:
                    print(f"✗ {result.energy:.2f} kWh - {result.error_reason}")
                
                # 更新 last_energy（只记录成功的合理数据）
                if result.reasonable:
                    last_energy = result.energy
            else:
                print(f"✗ 失败 - {result.error_reason}")
            
            # 等待下一次测试（精确间隔）
            if i < args.count - 1 and not _stop:
                next_time = start_time + (i + 1) * args.interval
                sleep_time = next_time - time.time()
                while sleep_time > 0 and not _stop:
                    time.sleep(min(0.05, sleep_time))
                    sleep_time = next_time - time.time()
        
        end_time = time.time()
        duration = end_time - start_time
        
        # 打印报告
        _print_report(summary, duration)
        
        # 保存详细报告
        if args.output:
            _save_detailed_report(summary, Path(args.output))
        
        return 0 if summary.success_rate >= 90 else 1  # 成功率<90%返回错误码
        
    except Exception as e:
        print(f"\n[错误] {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
