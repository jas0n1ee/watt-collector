# DL/T645 电表数据采集器 Docker 镜像
# 支持架构: amd64, arm64, armv7

FROM python:3.11-slim

# 安装依赖
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制脚本
COPY scripts/mqtt_collector.py .

# 创建数据目录
RUN mkdir -p /app/.data

# 健康检查（检查进程是否运行）
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep -f mqtt_collector.py || exit 1

# 默认入口
ENTRYPOINT ["python3", "/app/mqtt_collector.py"]

# 默认参数（可在运行时被覆盖）
CMD ["--interval", "60", "--data-dir", "/app/.data"]
