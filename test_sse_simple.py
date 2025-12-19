#!/usr/bin/env python3
"""简单的 SSE 测试"""
import requests
import threading
import time

def listen_sse():
    """监听 SSE"""
    print("🔌 Connecting to SSE...")
    try:
        response = requests.get("http://localhost:8000/api/stream/all", stream=True, timeout=30)
        print("✅ Connected!")
        for line in response.iter_lines(decode_unicode=True):
            if line:
                print(f"📨 {line}")
    except Exception as e:
        print(f"❌ Error: {e}")

# 启动 SSE 监听线程
thread = threading.Thread(target=listen_sse, daemon=True)
thread.start()

# 等待连接
time.sleep(2)

# 发送测试消息
print("\n🚀 Sending test message...")
resp = requests.post("http://localhost:8000/api/stream/test-vks")
print(f"📡 Response: {resp.json()}")

# 等待接收
print("\n⏳ Waiting for SSE message...")
time.sleep(3)

print("\n✅ Done!")
