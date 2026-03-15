"""
电表数据处理和功率计算模块。

功率计算逻辑：
- 从 CSV 文件中读取时间戳和总电量
- 计算相邻数据点的电量差 (ΔE) 和时间差 (Δt)
- 只有当电量差 >= 0.1 kWh 时才计算功率，过滤掉小波动造成的异常峰值
- 功率 = ΔE / Δt (单位：kW)
"""

import csv
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass


# 电量变化阈值 (kWh)，小于此值的数据点将被忽略
MIN_ENERGY_DELTA = 0.1


@dataclass
class PowerDataPoint:
    """功率数据点"""
    timestamp: datetime
    power_kw: float  # 功率 (kW)
    energy_kwh: float  # 累计电量 (kWh)
    energy_delta: float  # 电量变化量 (kWh)
    time_delta_minutes: float  # 时间变化量 (分钟)


@dataclass
class EnergyDataPoint:
    """原始电量数据点"""
    timestamp: datetime
    energy_kwh: float
    raw_frame: str


def parse_csv_file(filepath: Path) -> List[EnergyDataPoint]:
    """
    解析单个 CSV 文件，返回按时间排序的数据点列表。
    
    Args:
        filepath: CSV 文件路径
        
    Returns:
        按时间升序排列的 EnergyDataPoint 列表
    """
    data_points = []
    
    if not filepath.exists():
        return data_points
    
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        # 跳过表头（如果有）
        first_row = True
        for row in reader:
            if first_row:
                first_row = False
                # 检查是否是表头行
                if len(row) >= 2 and ('timestamp' in row[0].lower() or 'energy' in row[1].lower()):
                    continue
            
            if len(row) < 2:
                continue
            
            try:
                # 解析时间戳，支持多种格式
                timestamp_str = row[0].strip()
                # 尝试 ISO 格式
                try:
                    timestamp = datetime.fromisoformat(timestamp_str)
                except ValueError:
                    # 尝试其他常见格式
                    try:
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
                    except ValueError:
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                
                energy = float(row[1])
                raw_frame = row[2] if len(row) > 2 else ""
                
                data_points.append(EnergyDataPoint(
                    timestamp=timestamp,
                    energy_kwh=energy,
                    raw_frame=raw_frame
                ))
            except (ValueError, IndexError) as e:
                # 跳过格式错误的行
                continue
    
    # 按时间排序
    data_points.sort(key=lambda x: x.timestamp)
    return data_points


def calculate_power(data_points: List[EnergyDataPoint]) -> List[PowerDataPoint]:
    """
    根据电量数据计算功率。
    
    算法：
    1. 遍历排序后的数据点
    2. 计算相邻点的电量差 ΔE 和时间差 Δt
    3. 只有当 ΔE >= MIN_ENERGY_DELTA 时才计算功率
    4. 功率 = ΔE / (Δt / 3600) kW，即每小时消耗的电量
    
    Args:
        data_points: 按时间排序的电量数据点列表
        
    Returns:
        功率数据点列表
    """
    if len(data_points) < 2:
        return []
    
    power_points = []
    
    # 使用前一个有效点作为基准
    last_valid_point = data_points[0]
    
    for i in range(1, len(data_points)):
        current = data_points[i]
        
        # 计算差值
        energy_delta = current.energy_kwh - last_valid_point.energy_kwh
        time_delta_seconds = (current.timestamp - last_valid_point.timestamp).total_seconds()
        
        # 跳过无效的时间差（为零或负数，可能是数据问题）
        if time_delta_seconds <= 0:
            continue
        
        # 关键逻辑：只有电量变化超过阈值才计算功率
        # 这避免了因网络重试导致的小时间差产生的异常峰值
        if energy_delta >= MIN_ENERGY_DELTA:
            # 计算功率：kWh / (seconds / 3600) = kW
            power_kw = energy_delta / (time_delta_seconds / 3600.0)
            
            power_points.append(PowerDataPoint(
                timestamp=current.timestamp,
                power_kw=round(power_kw, 3),
                energy_kwh=current.energy_kwh,
                energy_delta=round(energy_delta, 3),
                time_delta_minutes=round(time_delta_seconds / 60.0, 2)
            ))
            
            # 更新基准点
            last_valid_point = current
    
    return power_points


def load_data_from_directory(data_dir: Path, days: Optional[int] = None) -> List[EnergyDataPoint]:
    """
    从数据目录加载所有 CSV 文件的数据。
    
    Args:
        data_dir: 数据目录路径
        days: 限制加载最近几天的数据，None 表示加载全部
        
    Returns:
        合并后的电量数据点列表
    """
    all_data = []
    
    if not data_dir.exists():
        return all_data
    
    # 获取所有 CSV 文件并按日期排序
    csv_files = sorted(data_dir.glob("electric_meter_*.csv"))
    
    # 如果指定了天数限制，只取最近的文件
    if days is not None:
        csv_files = csv_files[-days:]
    
    for csv_file in csv_files:
        data_points = parse_csv_file(csv_file)
        all_data.extend(data_points)
    
    # 再次整体排序
    all_data.sort(key=lambda x: x.timestamp)
    return all_data


def get_power_data(data_dir: Path, days: Optional[int] = None) -> List[Dict]:
    """
    获取功率数据（用于 API 接口）。
    
    Args:
        data_dir: 数据目录路径
        days: 限制加载最近几天的数据
        
    Returns:
        可用于 JSON 序列化的字典列表
    """
    energy_data = load_data_from_directory(data_dir, days)
    power_data = calculate_power(energy_data)
    
    return [
        {
            'timestamp': p.timestamp.isoformat(),
            'power_kw': p.power_kw,
            'energy_kwh': p.energy_kwh,
            'energy_delta': p.energy_delta,
            'time_delta_minutes': p.time_delta_minutes
        }
        for p in power_data
    ]


def get_statistics(data_dir: Path, days: Optional[int] = None) -> Dict:
    """
    获取统计数据。
    
    Args:
        data_dir: 数据目录路径
        days: 限制统计最近几天的数据
        
    Returns:
        统计信息字典
    """
    energy_data = load_data_from_directory(data_dir, days)
    power_data = calculate_power(energy_data)
    
    if not power_data:
        return {
            'total_points': 0,
            'avg_power_kw': 0,
            'max_power_kw': 0,
            'min_power_kw': 0,
            'total_energy_delta': 0,
            'time_range': None
        }
    
    powers = [p.power_kw for p in power_data]
    total_energy = sum(p.energy_delta for p in power_data)
    
    return {
        'total_points': len(power_data),
        'avg_power_kw': round(sum(powers) / len(powers), 3),
        'max_power_kw': round(max(powers), 3),
        'min_power_kw': round(min(powers), 3),
        'total_energy_delta': round(total_energy, 3),
        'time_range': {
            'start': power_data[0].timestamp.isoformat(),
            'end': power_data[-1].timestamp.isoformat()
        }
    }


def get_available_dates(data_dir: Path) -> List[str]:
    """
    获取可用的数据日期列表。
    
    Args:
        data_dir: 数据目录路径
        
    Returns:
        日期字符串列表 (YYYY-MM-DD)
    """
    if not data_dir.exists():
        return []
    
    dates = []
    for csv_file in sorted(data_dir.glob("electric_meter_*.csv")):
        # 从文件名提取日期
        date_str = csv_file.stem.replace("electric_meter_", "")
        dates.append(date_str)
    
    return dates
