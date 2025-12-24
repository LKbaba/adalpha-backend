# ADALPHA Backend Dockerfile
# 用于部署到 Google Cloud Run

FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 安装 confluent-kafka 所需的系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# 先复制 requirements.txt 以利用 Docker 缓存
COPY requirements.txt .

# 安装 Python 依赖
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY app/ ./app/

# 设置环境变量
# Cloud Run 会自动设置 PORT 环境变量
ENV PORT=8080

# 暴露端口（文档用途）
EXPOSE 8080

# 启动应用
# 使用 shell 形式以支持环境变量替换
# Cloud Run 会设置 PORT 环境变量
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}
