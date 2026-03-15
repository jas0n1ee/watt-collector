# 电表数据可视化 Web 服务 Docker 镜像
# 
# 构建镜像:
#   docker build -t eletric-meter-web .
#
# 运行容器:
#   docker run -d -p 5000:5000 -v /path/to/data:/data eletric-meter-web
#
# 自定义数据目录:
#   docker run -d -p 5000:5000 -v /path/to/data:/custom/path eletric-meter-web --data-dir /custom/path

FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 先复制依赖文件，利用 Docker 缓存层
COPY requirements.txt .

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY app.py .
COPY data_processor.py .
COPY templates/ ./templates/
COPY static/ ./static/

# 创建数据目录（默认挂载点）
RUN mkdir -p /data

# 暴露服务端口
EXPOSE 5000

# 设置入口点，允许传递命令行参数
ENTRYPOINT ["python", "app.py"]

# 默认参数：监听所有地址，数据目录指向 /data
CMD ["--host", "0.0.0.0", "--port", "5000", "--data-dir", "/data"]
