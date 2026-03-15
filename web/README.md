# 电表数据可视化 Web 服务

基于 Flask 的功率可视化系统，通过计算电量的差分来推测功率。

## 功能特性

- 📊 **实时功率图表** - 使用 Chart.js 绘制功率趋势图
- 📈 **统计数据** - 平均功率、最大功率、累计用电等统计信息
- 🔍 **时间筛选** - 支持按 1天/2天/7天/30天 查看数据
- 🔄 **自动刷新** - 每分钟自动刷新数据
- 📱 **响应式设计** - 适配桌面和移动设备

## 功率计算逻辑

由于采集的是**总电量**（kWh），功率需要通过差分计算：

```
功率 (kW) = Δ电量 (kWh) / Δ时间 (小时)
```

### 关键过滤机制

为避免网络重试导致的异常峰值，系统设置了**电量变化阈值**：

- **阈值**: `0.1 kWh`
- **逻辑**: 只有当电量变化 ≥ 0.1 kWh 时才计算功率

这样可以确保：
1. 过滤掉因重复采集产生的极小时间差导致的异常峰值
2. 保留真正有意义的功率变化数据点

## 项目结构

```
web/
├── app.py              # Flask 应用主程序
├── data_processor.py   # 数据处理和功率计算模块
├── start_server.py     # 启动脚本
├── README.md           # 本文档
├── templates/
│   └── index.html      # 前端页面
├── static/
│   ├── css/            # 样式文件
│   └── js/             # JavaScript 文件
```

## 快速开始

### 1. 确保虚拟环境已激活

```bash
source ../venv/bin/activate  # macOS/Linux
# 或
..\venv\Scripts\activate     # Windows
```

### 2. 启动 Web 服务

```bash
# 方法1：使用启动脚本
python start_server.py

# 方法2：指定数据目录
python start_server.py --data-dir ../.data --port 8080

# 方法3：直接使用 Flask 运行
python app.py --data-dir ../.data
```

### 3. 访问可视化页面

打开浏览器访问：http://localhost:5000

## API 接口

### 获取功率数据
```http
GET /api/power
GET /api/power?days=7              # 最近7天
GET /api/power?start=2026-03-14&end=2026-03-15  # 日期范围
```

响应示例：
```json
{
  "success": true,
  "count": 68,
  "data": [
    {
      "timestamp": "2026-03-14T23:34:57.479695",
      "power_kw": 1.99,
      "energy_kwh": 49864.78,
      "energy_delta": 0.13,
      "time_delta_minutes": 3.92
    }
  ]
}
```

### 获取统计数据
```http
GET /api/statistics
GET /api/statistics?days=7
```

响应示例：
```json
{
  "success": true,
  "statistics": {
    "avg_power_kw": 0.799,
    "max_power_kw": 3.552,
    "min_power_kw": 0.4,
    "total_energy_delta": 7.69,
    "total_points": 68,
    "time_range": {
      "start": "2026-03-14T23:34:57.479695",
      "end": "2026-03-15T11:56:53.128963"
    }
  }
}
```

### 获取可用日期
```http
GET /api/dates
```

### 获取原始电量数据
```http
GET /api/energy?date=2026-03-15
```

## 与采集系统集成

可视化服务与 MQTT 采集器完全独立：

1. **独立运行** - 可视化服务只读取 CSV 文件，不影响采集过程
2. **数据共享** - 两者通过 `.data` 目录共享数据文件
3. **无依赖** - 采集器无需知道可视化服务的存在

### 推荐部署方式

```bash
# 终端1：启动采集器（持续运行）
python scripts/mqtt_collector.py --interval 60

# 终端2：启动 Web 服务（持续运行）
python web/start_server.py --data-dir .data
```

## Docker 部署

可以在 `docker-compose.yml` 中添加 Web 服务：

```yaml
services:
  mqtt-collector:
    build: .
    image: mqtt-collector:latest
    volumes:
      - ./.data:/app/.data
    command: ["--interval", "60"]
  
  web-visualizer:
    build: .
    image: mqtt-collector:latest
    ports:
      - "5000:5000"
    volumes:
      - ./.data:/app/.data
    command: ["python", "web/start_server.py", "--data-dir", ".data", "--host", "0.0.0.0"]
    depends_on:
      - mqtt-collector
```

## 注意事项

1. **数据量** - 如果数据量很大（超过几个月），建议定期归档或限制查询天数
2. **时区** - 系统使用本地时区显示时间，确保服务器时区设置正确
3. **性能** - 首次加载大量数据可能需要几秒时间，建议限制 `days` 参数

## 故障排查

### 页面显示"无数据"
- 检查 `.data` 目录是否有 CSV 文件
- 确认 `start_server.py` 的 `--data-dir` 参数指向正确位置

### 功率数据太少
- 这是正常的，因为只有电量变化 ≥ 0.1 kWh 的点才会显示
- 如果用电功率很低，可能几个小时才有一个数据点

### 图表显示异常峰值
- 检查原始数据是否有异常值
- 可以调整 `data_processor.py` 中的 `MIN_ENERGY_DELTA` 阈值
