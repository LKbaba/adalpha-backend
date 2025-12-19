#!/usr/bin/env python3
"""
测试 SSE 数据流
验证：Kafka 消息 → stream_manager → SSE 广播

用法：
1. 启动后端: cd adalpha-backend && source venv/bin/activate && uvicorn app.main:app --port 8000
2. 运行测试: python test_sse_flow.py
"""

import json
import asyncio
import aiohttp

BACKEND_URL = "http://localhost:8000"

async def test_sse_connection():
    """测试 SSE 连接和接收消息"""
    print("=" * 60)
    print("SSE 数据流测试")
    print("=" * 60)
    
    # 1. 检查后端状态
    print("\n[1] 检查后端状态...")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{BACKEND_URL}/api/stream/status") as resp:
                status = await resp.json()
                print(f"    Stream Manager: {'运行中' if status['running'] else '未运行'}")
                print(f"    连接客户端数: {status['client_count']}")
        except Exception as e:
            print(f"    ❌ 后端未启动: {e}")
            return
    
    # 2. 发送测试 VKS 数据
    print("\n[2] 发送测试 VKS 数据...")
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{BACKEND_URL}/api/stream/test-vks") as resp:
            result = await resp.json()
            print(f"    结果: {result}")
    
    # 3. 连接 SSE 并监听
    print("\n[3] 连接 SSE 并监听 5 秒...")
    received_events = []
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{BACKEND_URL}/api/stream/all") as resp:
                # 设置超时
                async def read_events():
                    async for line in resp.content:
                        line = line.decode('utf-8').strip()
                        if line.startswith('event:'):
                            event_type = line[6:].strip()
                        elif line.startswith('data:'):
                            data = line[5:].strip()
                            try:
                                parsed = json.loads(data)
                                received_events.append({
                                    'event': event_type,
                                    'data': parsed
                                })
                                print(f"    📨 收到 {event_type}: {list(parsed.keys())[:5]}")
                            except:
                                pass
                
                try:
                    await asyncio.wait_for(read_events(), timeout=5.0)
                except asyncio.TimeoutError:
                    pass
        except Exception as e:
            print(f"    ❌ SSE 连接失败: {e}")
    
    # 4. 总结
    print("\n[4] 测试结果:")
    print(f"    收到事件数: {len(received_events)}")
    event_types = set(e['event'] for e in received_events)
    print(f"    事件类型: {event_types}")
    
    if 'vks_update' in event_types:
        print("    ✅ vks_update 事件正常")
    else:
        print("    ⚠️ 未收到 vks_update 事件")
    
    if 'heartbeat' in event_types:
        print("    ✅ heartbeat 事件正常")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    asyncio.run(test_sse_connection())
