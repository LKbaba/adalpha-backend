#!/usr/bin/env python3
"""
数据流诊断脚本

检查从 Kafka 到前端的完整数据链路
"""

import asyncio
import aiohttp
import json
from datetime import datetime

BACKEND_URL = "http://localhost:8000"
SPIDER_URL = "http://localhost:8001"


async def check_spider_server():
    """检查爬虫服务器状态"""
    print("\n" + "=" * 60)
    print("1. 检查爬虫服务器 (spider6p)")
    print("=" * 60)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{SPIDER_URL}/health", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    print(f"✅ 爬虫服务器在线: {data}")
                    return True
    except aiohttp.ClientConnectorError:
        print(f"❌ 爬虫服务器离线 - 请运行: cd spider6p && npm run server")
    except Exception as e:
        print(f"❌ 检查失败: {e}")
    return False


async def check_kafka_connection():
    """检查 Kafka 连接"""
    print("\n" + "=" * 60)
    print("2. 检查 Kafka 连接")
    print("=" * 60)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BACKEND_URL}/api/stream/kafka-debug", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                data = await resp.json()
                print(f"Kafka 连接状态: {'✅ 已连接' if data.get('kafka_connected') else '❌ 未连接'}")
                print(f"Topics: {data.get('topics', [])}")
                print(f"Stream Manager 运行中: {'✅' if data.get('stream_manager_running') else '❌'}")
                print(f"SSE 客户端数: {data.get('sse_client_count', 0)}")
                return data
    except Exception as e:
        print(f"❌ 检查失败: {e}")
    return None


async def check_history_store():
    """检查历史数据存储"""
    print("\n" + "=" * 60)
    print("3. 检查历史数据存储")
    print("=" * 60)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BACKEND_URL}/api/history/stats", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                data = await resp.json()
                print(f"总记录数: {data.get('total_records', 0)}")
                print(f"平台分布: {data.get('platforms', {})}")
                print(f"平均分数: {data.get('average_scores', {})}")
                print(f"最新记录: {data.get('newest_record', 'N/A')}")
                print(f"最旧记录: {data.get('oldest_record', 'N/A')}")
                return data
    except Exception as e:
        print(f"❌ 检查失败: {e}")
    return None


async def check_rankings():
    """检查排名数据"""
    print("\n" + "=" * 60)
    print("4. 检查排名数据")
    print("=" * 60)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BACKEND_URL}/api/history/rankings?top_n=5", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                data = await resp.json()
                for platform, info in data.items():
                    records = info.get('records', [])
                    print(f"\n{platform}: {len(records)} 条记录")
                    for r in records[:3]:
                        print(f"  #{r.get('rank')} {r.get('hashtag')} - Score: {r.get('trend_score', 0):.1f}")
                return data
    except Exception as e:
        print(f"❌ 检查失败: {e}")
    return None


async def test_sse_connection():
    """测试 SSE 连接"""
    print("\n" + "=" * 60)
    print("5. 测试 SSE 连接 (等待 5 秒)")
    print("=" * 60)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{BACKEND_URL}/api/stream/vks", timeout=aiohttp.ClientTimeout(total=10)) as resp:
                print(f"SSE 连接状态: {resp.status}")
                
                # 读取前几个事件
                events_received = 0
                async for line in resp.content:
                    line = line.decode('utf-8').strip()
                    if line.startswith('event:'):
                        event_type = line.split(':', 1)[1].strip()
                        print(f"  收到事件: {event_type}")
                        events_received += 1
                    elif line.startswith('data:'):
                        data = line.split(':', 1)[1].strip()
                        try:
                            parsed = json.loads(data)
                            print(f"    数据: {json.dumps(parsed, ensure_ascii=False)[:100]}...")
                        except:
                            print(f"    数据: {data[:100]}...")
                    
                    if events_received >= 3:
                        break
                        
                print(f"✅ SSE 连接正常，收到 {events_received} 个事件")
                return True
    except asyncio.TimeoutError:
        print("⚠️ SSE 连接超时 - 可能没有新数据")
    except Exception as e:
        print(f"❌ SSE 连接失败: {e}")
    return False


async def send_test_vks():
    """发送测试 VKS 数据"""
    print("\n" + "=" * 60)
    print("6. 发送测试 VKS 数据")
    print("=" * 60)
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{BACKEND_URL}/api/stream/test-vks", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                data = await resp.json()
                if data.get('success'):
                    print(f"✅ 测试数据已发送: {data.get('data', {}).get('hashtag')}")
                else:
                    print(f"⚠️ 发送失败: {data.get('message')}")
                return data
    except Exception as e:
        print(f"❌ 发送失败: {e}")
    return None


async def main():
    print("\n" + "=" * 60)
    print("🔍 ADALPHA 数据流诊断工具")
    print(f"时间: {datetime.now().isoformat()}")
    print("=" * 60)
    
    # 1. 检查爬虫服务器
    spider_ok = await check_spider_server()
    
    # 2. 检查 Kafka
    kafka_info = await check_kafka_connection()
    
    # 3. 检查历史存储
    history_info = await check_history_store()
    
    # 4. 检查排名
    rankings = await check_rankings()
    
    # 5. 测试 SSE
    # sse_ok = await test_sse_connection()
    
    # 6. 发送测试数据
    await send_test_vks()
    
    # 总结
    print("\n" + "=" * 60)
    print("📋 诊断总结")
    print("=" * 60)
    
    issues = []
    
    if not spider_ok:
        issues.append("❌ 爬虫服务器未启动 - 运行: cd spider6p && npm run server")
    
    if kafka_info:
        if not kafka_info.get('kafka_connected'):
            issues.append("❌ Kafka 未连接 - 检查 .env 中的 Kafka 配置")
        if not kafka_info.get('stream_manager_running'):
            issues.append("❌ Stream Manager 未运行 - 重启后端服务")
    else:
        issues.append("❌ 后端服务未启动 - 运行: cd adalpha-backend && python -m uvicorn app.main:app --reload")
    
    if history_info:
        if history_info.get('total_records', 0) == 0:
            issues.append("⚠️ 历史数据为空 - 需要触发爬虫或等待 Kafka 数据")
    
    if issues:
        print("\n发现以下问题:")
        for issue in issues:
            print(f"  {issue}")
    else:
        print("\n✅ 所有检查通过!")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
