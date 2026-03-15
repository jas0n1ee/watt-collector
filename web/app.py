#!/usr/bin/env python3
"""
电表数据可视化 Web 服务

基于 Flask 的 Web 服务，提供：
- 功率数据可视化图表
- RESTful API 接口
- 支持按日期范围筛选

使用方式：
    # 使用默认配置（数据目录：../.data）
    python app.py
    
    # 指定数据目录
    python app.py --data-dir /path/to/data
    
    # 指定端口
    python app.py --port 8080

API 端点：
    GET /api/power              - 获取功率数据
    GET /api/power?days=7       - 获取最近7天的数据
    GET /api/statistics         - 获取统计数据
    GET /api/dates              - 获取可用日期列表
    GET /api/energy?date=2026-03-15  - 获取指定日期的原始电量数据
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from flask import Flask, jsonify, render_template, request

# 添加当前目录到路径，以便导入 data_processor
sys.path.insert(0, str(Path(__file__).parent))
from data_processor import (
    get_power_data, 
    get_statistics, 
    get_available_dates,
    load_data_from_directory,
    calculate_power
)

# 创建 Flask 应用
def create_app(data_dir: Path):
    app = Flask(__name__)
    app.config['DATA_DIR'] = data_dir
    
    @app.route('/')
    def index():
        """首页 - 功率可视化图表"""
        return render_template('index.html')
    
    @app.route('/api/power')
    def api_power():
        """
        获取功率数据 API。
        
        Query Parameters:
            days: 获取最近几天的数据（可选，默认全部）
            start: 开始日期 (YYYY-MM-DD)（可选）
            end: 结束日期 (YYYY-MM-DD)（可选）
            
        Returns:
            JSON 格式的功率数据列表
        """
        try:
            days = request.args.get('days', type=int)
            start_date = request.args.get('start')
            end_date = request.args.get('end')
            
            data_dir = app.config['DATA_DIR']
            
            # 如果指定了日期范围，需要特殊处理
            if start_date or end_date:
                all_data = load_data_from_directory(data_dir)
                
                # 过滤日期范围
                if start_date:
                    start_dt = datetime.fromisoformat(start_date)
                    all_data = [d for d in all_data if d.timestamp >= start_dt]
                
                if end_date:
                    end_dt = datetime.fromisoformat(end_date) + timedelta(days=1)
                    all_data = [d for d in all_data if d.timestamp < end_dt]
                
                power_data = calculate_power(all_data)
                result = [
                    {
                        'timestamp': p.timestamp.isoformat(),
                        'power_kw': p.power_kw,
                        'energy_kwh': p.energy_kwh,
                        'energy_delta': p.energy_delta,
                        'time_delta_minutes': p.time_delta_minutes
                    }
                    for p in power_data
                ]
            else:
                result = get_power_data(data_dir, days)
            
            return jsonify({
                'success': True,
                'count': len(result),
                'data': result
            })
            
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/statistics')
    def api_statistics():
        """
        获取统计数据 API。
        
        Query Parameters:
            days: 统计最近几天的数据（可选，默认全部）
            
        Returns:
            JSON 格式的统计信息
        """
        try:
            days = request.args.get('days', type=int)
            data_dir = app.config['DATA_DIR']
            stats = get_statistics(data_dir, days)
            
            return jsonify({
                'success': True,
                'statistics': stats
            })
            
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/dates')
    def api_dates():
        """
        获取可用日期列表 API。
        
        Returns:
            可用的数据日期列表 (YYYY-MM-DD)
        """
        try:
            data_dir = app.config['DATA_DIR']
            dates = get_available_dates(data_dir)
            
            return jsonify({
                'success': True,
                'dates': dates
            })
            
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/energy')
    def api_energy():
        """
        获取原始电量数据 API。
        
        Query Parameters:
            date: 指定日期 (YYYY-MM-DD)（可选，默认今天）
            
        Returns:
            JSON 格式的原始电量数据列表
        """
        try:
            date_str = request.args.get('date', datetime.now().strftime('%Y-%m-%d'))
            data_dir = app.config['DATA_DIR']
            
            # 构造文件名
            csv_file = data_dir / f"electric_meter_{date_str}.csv"
            
            if not csv_file.exists():
                return jsonify({
                    'success': True,
                    'date': date_str,
                    'count': 0,
                    'data': []
                })
            
            from data_processor import parse_csv_file
            data = parse_csv_file(csv_file)
            
            result = [
                {
                    'timestamp': d.timestamp.isoformat(),
                    'energy_kwh': d.energy_kwh,
                    'raw_frame': d.raw_frame
                }
                for d in data
            ]
            
            return jsonify({
                'success': True,
                'date': date_str,
                'count': len(result),
                'data': result
            })
            
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    return app


def main():
    parser = argparse.ArgumentParser(description='电表数据可视化 Web 服务')
    parser.add_argument('--data-dir', default='../.data', 
                        help='数据目录路径（默认：../.data）')
    parser.add_argument('--host', default='0.0.0.0',
                        help='监听地址（默认：0.0.0.0）')
    parser.add_argument('--port', type=int, default=5000,
                        help='监听端口（默认：5000）')
    parser.add_argument('--debug', action='store_true',
                        help='启用调试模式')
    
    args = parser.parse_args()
    
    # 解析数据目录路径
    data_dir = Path(args.data_dir).resolve()
    
    # 检查数据目录是否存在
    if not data_dir.exists():
        print(f"[警告] 数据目录不存在: {data_dir}")
        print("[提示] 请确保采集器已运行并生成数据文件")
        # 尝试创建目录
        try:
            data_dir.mkdir(parents=True, exist_ok=True)
            print(f"[信息] 已创建数据目录: {data_dir}")
        except Exception as e:
            print(f"[错误] 无法创建数据目录: {e}")
            return 1
    
    # 创建应用
    app = create_app(data_dir)
    
    print("=" * 60)
    print("电表数据可视化 Web 服务")
    print("=" * 60)
    print(f"数据目录: {data_dir}")
    print(f"访问地址: http://{args.host}:{args.port}")
    print(f"调试模式: {'开启' if args.debug else '关闭'}")
    print("=" * 60)
    print("按 Ctrl+C 停止服务")
    print("")
    
    # 运行服务
    app.run(host=args.host, port=args.port, debug=args.debug)
    
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
