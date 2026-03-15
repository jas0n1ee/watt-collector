#!/usr/bin/env python3
"""
电表可视化 Web 服务启动脚本

使用方式：
    python start_server.py
    
    # 指定数据目录（如果与默认位置不同）
    python start_server.py --data-dir ../.data
    
    # 指定端口
    python start_server.py --port 8080

访问：
    启动后打开浏览器访问 http://localhost:5000
"""

import sys
from pathlib import Path

# 确保可以导入 app 模块
sys.path.insert(0, str(Path(__file__).parent))

from app import main

if __name__ == '__main__':
    raise SystemExit(main())
