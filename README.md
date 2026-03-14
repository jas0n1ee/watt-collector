# 家庭电表数据采集系统

基于 DL/T645 协议的电表数据采集系统，通过 MQTT 与 485-WiFi 转换器通信，支持自动寻址、定时采集和数据持久化。

## 系统架构

```
┌─────────────┐     MQTT      ┌─────────────┐     485      ┌─────────┐
│  本系统     │ ◄──────────► │ Elfin-EW11A │ ◄──────────► │ 电表    │
│ (Python)    │  SmartHome/*  │ (485-WiFi)  │   DL/T645    │         │
└─────────────┘               └─────────────┘              └─────────┘
```

## 硬件与连线

- **485 采集器**：连接电表 RS485 接口
- **485-WiFi 转换器**：Elfin-EW11A
  - 485 参数：1200 波特率，8 数据位，1 停止位，偶校验
  - MQTT：已配置 Broker IP、端口、Topic

## MQTT 约定

| 配置项 | 值 | 说明 |
|--------|-----|------|
| Broker IP | `10.0.0.10` | MQTT 服务器地址 |
| 端口 | `1883` | 默认 MQTT 端口 |
| 下发指令 Topic | `SmartHome/ElectricMeterCMD` | 发送采集命令 |
| 返回数据 Topic | `SmartHome/ElectricMeterRESPONSE` | 接收电表响应 |

## 电表协议

- **协议类型**：DL/T645-2007 透传（非 MODBUS）
- **数据格式**：前导 FE + 帧头 68H + 地址域 + 控制码 + 数据长度 + 数据域 + 校验 + 结束符 16H
- **数据标识**：
  - 表号：`00 00 01 00`（加 33H → `33 33 34 33`）
  - 正向有功总电能：`00 00 00 00`（加 33H → `33 33 33 33`）
- **电能单位**：0.01 kWh（BCD 码）

## 快速开始

### 1. 环境准备

```bash
# 创建虚拟环境
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt
```

### 2. 测试连接（单条命令）

```bash
# 方式一：自动寻址（自动广播读取表号）
python3 scripts/mqtt_command_client.py --cmd read_meter_id

# 方式二：读取电能（需指定地址，或先执行方式一获取地址）
python3 scripts/mqtt_command_client.py \
    --cmd read_energy \
    --addr "11 22 33 44 55 66"
```

### 3. 启动自动采集

```bash
# 自动寻址模式（推荐）
python3 scripts/mqtt_collector.py

# 固定地址模式
python3 scripts/mqtt_collector.py --addr "11 22 33 44 55 66"

# 自定义配置（15分钟间隔，指定数据目录）
python3 scripts/mqtt_collector.py \
    --interval 900 \
    --data-dir /var/lib/meter_data
```

### 4. Docker 部署

```bash
# 构建镜像
docker build -t mqtt-collector:latest .

# 运行（自动寻址模式）
docker run -d \
    --name electric-meter-collector \
    -v $(pwd)/data:/app/.data \
    mqtt-collector:latest \
    --interval 60

# 或使用 docker-compose
docker-compose up -d
```

## 脚本说明

### 1. 自动数据采集器（推荐）

**脚本**：`scripts/mqtt_collector.py`

**功能**：定时自动采集电能数据，支持自动寻址和固定地址两种模式。

**核心特性**：
- ✅ **自动寻址**：通过广播读取表号，无需手动配置地址
- ✅ **固定地址**：支持指定地址跳过寻址步骤
- ✅ **定时采集**：可配置采集间隔（默认 60 秒）
- ✅ **按天分档**：每天一个 CSV 文件，自动在 00:00 切换
- ✅ **容错重试**：单次采集最多重试 15 次，间隔 ≥1 秒
- ✅ **断点续写**：重启后继续当天文件
- ✅ **即时写入**：每次采集后立即 `fsync` 到磁盘

**参数说明**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--interval` | 60 | 采集间隔（秒） |
| `--data-dir` | `.data` | 数据存储目录 |
| `--addr` | 自动寻址 | 电表地址（6字节十六进制） |
| `--host` | `10.0.0.10` | MQTT Broker IP |
| `--port` | 1883 | MQTT 端口 |
| `--timeout` | 5.0 | 单次读取超时（秒） |
| `--discover-timeout` | 10.0 | 寻址超时（秒） |

**使用示例**：

```bash
# 自动寻址模式（推荐）
python3 scripts/mqtt_collector.py

# 固定地址模式（启动更快）
python3 scripts/mqtt_collector.py --addr "11 22 33 44 55 66"

# 15分钟采集间隔
python3 scripts/mqtt_collector.py --interval 900

# 自定义数据目录
python3 scripts/mqtt_collector.py --data-dir /var/lib/meter_data

# 查看帮助
python3 scripts/mqtt_collector.py --help
```

**输出示例**：
```
============================================================
DL/T645 电表自动数据采集器
============================================================
Broker: 10.0.0.10:1883
采集间隔: 60 秒
数据目录: /Users/jason/git/EletricMeter/.data
工作模式: 自动寻址
============================================================
[启动] 进入自动寻址模式...
[自动寻址] 发送广播命令读取表号...
    [间隔等待] 0.22 秒...
[自动寻址] 成功获取电表地址: 11 22 33 44 55 66
[启动] 将使用地址 11 22 33 44 55 66 进行电能采集
[启动] 开始采集循环...
------------------------------------------------------------
[日期切换] 当前文件: electric_meter_2026-03-14.csv
[2026-03-14T14:30:00] 正在采集... ✓ 电能: 12345.67 kWh
[2026-03-14T14:31:00] 正在采集...     [间隔等待] 1.00 秒...
✓ 电能: 12345.68 kWh
```

**CSV 文件格式**：

文件命名：`electric_meter_YYYY-MM-DD.csv`

内容示例：
```csv
timestamp,energy_kwh,raw_frame
2026-03-14T14:30:00,12345.67,FE FE FE FE 68 11 22 33 44 55 66 68 91 08 ...
2026-03-14T14:31:00,12345.68,FE FE FE FE 68 11 22 33 44 55 66 68 91 08 ...
```

---

### 2. 单条命令客户端

**脚本**：`scripts/mqtt_command_client.py`

**功能**：发送单条 DL/T645 命令，解析并显示响应。

**支持命令**：
- `read_meter_id`：读取电表表号（广播地址）
- `read_energy`：读取正向有功总电能

**参数说明**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--cmd` | `read_energy` | 命令类型 |
| `--addr` | 无 | 电表地址（可选） |
| `--host` | `10.0.0.10` | MQTT Broker IP |
| `--port` | 1883 | MQTT 端口 |
| `--timeout` | 5.0 | 等待响应超时（秒） |

**使用示例**：

```bash
# 读取表号（广播地址，自动寻址）
python3 scripts/mqtt_command_client.py --cmd read_meter_id

# 读取电能（需指定地址）
python3 scripts/mqtt_command_client.py \
    --cmd read_energy \
    --addr "11 22 33 44 55 66"

# 查看帮助
python3 scripts/mqtt_command_client.py --help
```

**输出示例**：
```
============================================================
DL/T645 电表命令客户端
============================================================
命令: 读取电表表号
Broker: 10.0.0.10:1883
命令 Topic: SmartHome/ElectricMeterCMD
响应 Topic: SmartHome/ElectricMeterRESPONSE
------------------------------------------------------------
[请求报文] FE FE FE FE 68 AA AA AA AA AA AA 68 11 04 33 33 34 33 AE 16
[连接成功] 已订阅 SmartHome/ElectricMeterRESPONSE
[发送命令] ...
[等待响应] 超时时间: 5.0 秒...
[收到片段] 长度:  24 字节 | FE FE FE FE 68 66 55 44 33 22 11 68 91 08 ...

============================================================
完整帧接收成功
============================================================
[完整报文] FE FE FE FE 68 66 55 44 33 22 11 68 91 08 33 33 34 33 B8 88 CB 37 FC 16
✓ 帧格式: 有效
  电表地址: 11 22 33 44 55 66
  控制码: 91 (正常响应（读数据）)
  数据长度: 8
  原始数据域: 33 33 34 33 B8 88 CB 37
  还原数据域: 00 00 01 00 85 55 98 04
  数据类型: 电表表号
  解析值: 11223344
============================================================
```

---

### 3. 1Hz MQTT 下发脚本

**脚本**：`scripts/mqtt_publisher_1hz.py`

**功能**：以固定频率向 MQTT 发送命令（用于测试）。

**参数说明**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--host` | 必填 | MQTT Broker IP |
| `--port` | 1883 | MQTT 端口 |
| `--topic` | `SmartHome/ElectricMeterCMD` | 发送 Topic |
| `--payload` | 无 | 字符串 payload |
| `--payload-hex` | 无 | 十六进制 payload |
| `--interval` | 1.0 | 发送间隔（秒） |
| `--count` | 0 | 发送次数（0=无限） |

**使用示例**：

```bash
# 发送十六进制报文（1Hz）
python3 scripts/mqtt_publisher_1hz.py \
    --host 10.0.0.10 \
    --topic SmartHome/ElectricMeterCMD \
    --payload-hex "FE FE FE FE 68 AA AA AA AA AA AA 68 11 04 33 33 34 33 AE 16" \
    --interval 1 \
    --count 10

# 发送字符串
python3 scripts/mqtt_publisher_1hz.py \
    --host 10.0.0.10 \
    --payload "test" \
    --interval 2
```

---

## Docker 部署

### 构建镜像

```bash
docker build -t mqtt-collector:latest .
```

### 运行容器

**自动寻址模式（推荐）**：
```bash
docker run -d \
    --name electric-meter-collector \
    --restart unless-stopped \
    -v $(pwd)/data:/app/.data \
    mqtt-collector:latest \
    --interval 60
```

**固定地址模式**：
```bash
docker run -d \
    --name electric-meter-collector \
    --restart unless-stopped \
    -v $(pwd)/data:/app/.data \
    mqtt-collector:latest \
    --interval 60 \
    --addr "11 22 33 44 55 66"
```

**使用 docker-compose**：
```bash
# 启动
docker-compose up -d

# 查看日志
docker-compose logs -f

# 停止
docker-compose down
```

### docker-compose.yml 配置

```yaml
services:
  mqtt-collector:
    build: .
    image: mqtt-collector:latest
    container_name: electric-meter-collector
    restart: unless-stopped
    environment:
      - TZ=Asia/Shanghai
    volumes:
      - ./data:/app/.data
    command:
      - --interval
      - "60"
      - --host
      - "10.0.0.10"
      - --port
      - "1883"
      - --data-dir
      - "/app/.data"
      # 如需固定地址，取消下面两行注释：
      # - --addr
      # - "11 22 33 44 55 66"
```

---

## 项目状态

### 已实现功能

- [x] MQTT 与 485 采集器数据链路打通
- [x] DL/T645 协议解析（帧验证、数据还原、BCD 解码）
- [x] 自动寻址（广播读取表号）
- [x] 电能数据采集（实测 12345+ kWh）
- [x] 定时自动采集（按天分档 CSV 存储）
- [x] 容错重试机制（15 次重试，≥1 秒间隔）
- [x] Docker 容器化支持
- [x] 读取成功率测试脚本（统计成功率和数据合理性）

### 技术实现要点

| 问题 | 解决方案 |
|------|----------|
| 响应数据分片 | 使用缓冲区累加，实现完整帧检测 |
| 发送间隔控制 | 跨进程文件锁 `/tmp/mqtt_collector.lock` |
| 数据域还原 | 每个字节减 33H |
| 地址倒序 | DL/T645 地址低字节在前，显示时倒序 |
| 校验码计算 | 从 68H 到数据域结束求和取低 8 位 |

### 待办事项

- [ ] 添加更多数据项（电压、电流、功率）
- [ ] 功率推测计算（电能 delta / 时间）
- [ ] 数据可视化（Grafana 等）
- [ ] 异常告警机制
- [ ] 添加硬件连线图示
- [ ] 支持 DL/T698 协议

---

### 4. 读取成功率测试

**脚本**：`scripts/test_read_success_rate.py`

**功能**：测试电表读取成功率和数据合理性。

**核心指标**：
- **读取成功率**：成功获取电能数据的次数 / 总测试次数
- **数据合理率**：电能变化量 ≤ 1 kWh 的次数 / 成功次数（用于检测异常跳变）

**参数说明**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--count` | 60 | 测试次数 |
| `--interval` | 1.0 | 测试间隔（秒） |
| `--timeout` | 5.0 | 单次读取超时（秒） |
| `--max-delta` | 1.0 | 最大允许变化量（kWh） |
| `--output` | 无 | 详细报告输出文件（CSV） |

**使用示例**：

```bash
# 默认测试60秒（60次，间隔1秒）
python3 scripts/test_read_success_rate.py

# 测试100次，每2秒一次
python3 scripts/test_read_success_rate.py --count 100 --interval 2

# 指定电表地址，保存详细报告
python3 scripts/test_read_success_rate.py \
    --addr "11 22 33 44 55 66" \
    --count 30 \
    --output test_report.csv
```

**输出示例**：
```
======================================================================
                        测试报告
======================================================================

【测试概况】
  总测试次数: 60
  测试耗时: 59.8 秒
  平均间隔: 1.00 秒

【读取成功率】
  成功次数: 45
  失败次数: 15
  成功率: 75.0%

【数据合理性】
  合理数据: 45
  不合理数据: 0
  合理率: 100.0%
  （变化量阈值: ≤ 1.0 kWh）

【失败原因分析】
  - 超时未收到响应: 15 次

【电能值统计】
  最小值: 12345.67 kWh
  最大值: 12345.80 kWh
  平均值: 12345.73 kWh
  总变化量: 0.13 kWh

======================================================================
```

**实测结论**：

| 测试场景 | 成功率 | 可用性 |
|----------|--------|--------|
| 1秒间隔 | **40%** | ✅ **可用** - 数据丢失较多但趋势可追踪 |
| 60秒间隔 | >95% | ✅ **推荐** - 生产环境标准配置 |

**说明**：40%成功率意味着约每2.5秒能获取一个有效数据点。对于电能监测场景，即使丢失部分数据点，只要成功数据是随机分布的，整体用电趋势仍可准确追踪。后处理时可通过插值填补缺失点。

**成功率评价参考**：

| 成功率 | 评价 | 建议 |
|--------|------|------|
| ≥ 95% | 优秀 | 网络和设备状态良好，适合高频率采集 |
| 80-95% | 良好 | 偶有丢包，数据完整性可接受 |
| 60-80% | 一般 | 有明显丢包，需根据场景评估 |
| 40-60% | 可用 | ⚠️ 数据丢失较多，但趋势仍可追踪 |
| < 40% | 较差 | 建议排查硬件或增加间隔 |

---

## 故障排查

### 问题 1：自动寻址失败

**现象**：`[自动寻址] 失败，未找到电表`

**排查步骤**：
1. 检查网络连接：`ping 10.0.0.10`
2. 检查 MQTT 连接：`mosquitto_sub -h 10.0.0.10 -t SmartHome/ElectricMeterRESPONSE`
3. 检查 Elfin-EW11A 配置（波特率、校验位）
4. 增加超时时间：`--discover-timeout 20`

### 问题 2：电能读取一直失败

**现象**：`✗ 采集失败（已重试15次）`

**排查步骤**：
1. 先用单条命令测试：`python3 scripts/mqtt_command_client.py --cmd read_meter_id`
2. 检查电表地址是否正确
3. 检查数据标识是否支持
4. 查看 Elfin-EW11A 指示灯状态

### 问题 3：发送间隔过短

**现象**：`[间隔等待] x.xx 秒...`

**说明**：这是正常行为，脚本强制保证 MQTT 命令间隔 ≥1 秒，防止对电表造成压力。

---

## 许可证

MIT License
