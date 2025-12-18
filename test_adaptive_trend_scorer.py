#!/usr/bin/env python3
"""
自适应 Trend Score 计算测试脚本

测试不同平台数据的适应性处理

用法: python test_adaptive_trend_scorer.py [crawl_file_path]
"""

import json
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from app.services.adaptive_trend_scorer import (
    AdaptiveTrendScorer,
    adaptive_trend_scorer,
    compute_adaptive_trend_score,
    process_crawl_file,
    Platform
)


def test_platform_detection():
    """测试平台检测"""
    print("\n" + "=" * 60)
    print("测试 1: 平台检测")
    print("=" * 60)
    
    scorer = AdaptiveTrendScorer()
    
    # 基于实际爬虫支持的平台
    test_cases = [
        ("tiktok", Platform.TIKTOK),
        ("TikTok", Platform.TIKTOK),
        ("instagram", Platform.INSTAGRAM),
        ("Instagram", Platform.INSTAGRAM),
        ("reddit", Platform.REDDIT),
        ("Reddit", Platform.REDDIT),
        ("youtube", Platform.YOUTUBE),
        ("YouTube", Platform.YOUTUBE),
        ("twitter", Platform.TWITTER),
        ("Twitter", Platform.TWITTER),
        ("x", Platform.TWITTER),
        ("linkedin", Platform.LINKEDIN),
        ("LinkedIn", Platform.LINKEDIN),
        ("unknown_platform", Platform.UNKNOWN),
    ]
    
    for platform_str, expected in test_cases:
        result = scorer.detect_platform(platform_str)
        status = "✓" if result == expected else "✗"
        print(f"  {status} '{platform_str}' -> {result.value} (expected: {expected.value})")


def test_metric_extraction():
    """测试指标提取"""
    print("\n" + "=" * 60)
    print("测试 2: 指标提取 (基于实际爬虫数据结构)")
    print("=" * 60)
    
    scorer = AdaptiveTrendScorer()
    
    # TikTok 数据 (完整)
    tiktok_stats = {
        "views": 1000000,
        "likes": 50000,
        "comments": 2000,
        "shares": 1000,
        "saves": 5000
    }
    
    # Instagram 数据 (可能为0)
    instagram_stats = {
        "views": 500000,
        "likes": 30000,
        "comments": 1500,
        "shares": 500
    }
    
    # Twitter 数据
    twitter_stats = {
        "views": 200000,
        "likes": 5000,
        "comments": 300,
        "retweets": 1000,
        "bookmarks": 500
    }
    
    # YouTube 数据 (likes/comments 可能为0)
    youtube_stats = {
        "views": 33632,
        "likes": 0,
        "comments": 0
    }
    
    # Reddit 数据
    reddit_stats = {
        "upvotes": 5000,
        "downvotes": 200,
        "score": 4800,
        "comments": 300
    }
    
    # LinkedIn 数据 (只有 services)
    linkedin_stats = {
        "services": 0
    }
    
    print("\n  TikTok 指标提取:")
    tiktok_metrics = scorer.extract_metrics(tiktok_stats, Platform.TIKTOK)
    for k, v in tiktok_metrics.items():
        if not k.startswith("_"):
            print(f"    {k}: {v}")
    
    print("\n  Instagram 指标提取:")
    instagram_metrics = scorer.extract_metrics(instagram_stats, Platform.INSTAGRAM)
    for k, v in instagram_metrics.items():
        if not k.startswith("_"):
            print(f"    {k}: {v}")
    
    print("\n  Reddit 指标提取:")
    reddit_metrics = scorer.extract_metrics(reddit_stats, Platform.REDDIT)
    for k, v in reddit_metrics.items():
        if not k.startswith("_"):
            print(f"    {k}: {v}")
    
    print("\n  Twitter 指标提取:")
    twitter_metrics = scorer.extract_metrics(twitter_stats, Platform.TWITTER)
    for k, v in twitter_metrics.items():
        if not k.startswith("_"):
            print(f"    {k}: {v}")
    
    print("\n  YouTube 指标提取:")
    youtube_metrics = scorer.extract_metrics(youtube_stats, Platform.YOUTUBE)
    for k, v in youtube_metrics.items():
        if not k.startswith("_"):
            print(f"    {k}: {v}")
    
    print("\n  LinkedIn 指标提取 (数据有限):")
    linkedin_metrics = scorer.extract_metrics(linkedin_stats, Platform.LINKEDIN)
    for k, v in linkedin_metrics.items():
        if not k.startswith("_"):
            print(f"    {k}: {v}")


def test_score_calculation():
    """测试分数计算"""
    print("\n" + "=" * 60)
    print("测试 3: 分数计算 (基于实际爬虫平台)")
    print("=" * 60)
    
    test_cases = [
        {
            "name": "TikTok 热门视频",
            "keyword": "music",
            "platform": "tiktok",
            "stats": {
                "views": 1500000,
                "likes": 85000,
                "comments": 3200,
                "shares": 1500,
                "saves": 2800
            },
            "posts": 20
        },
        {
            "name": "Instagram 帖子",
            "keyword": "dance",
            "platform": "instagram",
            "stats": {
                "views": 500000,
                "likes": 25000,
                "comments": 800,
                "shares": 200
            },
            "posts": 15
        },
        {
            "name": "Twitter 推文",
            "keyword": "music",
            "platform": "twitter",
            "stats": {
                "views": 200000,
                "likes": 5000,
                "comments": 300,
                "retweets": 1000,
                "bookmarks": 500
            },
            "posts": 10
        },
        {
            "name": "YouTube 视频",
            "keyword": "music",
            "platform": "youtube",
            "stats": {
                "views": 33632,
                "likes": 500,
                "comments": 50
            },
            "posts": 10
        },
        {
            "name": "Reddit 帖子",
            "keyword": "music",
            "platform": "reddit",
            "stats": {
                "upvotes": 5000,
                "downvotes": 200,
                "score": 4800,
                "comments": 300
            },
            "posts": 10
        },
        {
            "name": "LinkedIn (数据有限)",
            "keyword": "music",
            "platform": "linkedin",
            "stats": {
                "services": 0
            },
            "posts": 10
        },
        {
            "name": "AI 工具关键词 (TikTok)",
            "keyword": "ai headshot",
            "platform": "tiktok",
            "stats": {
                "views": 2000000,
                "likes": 120000,
                "comments": 5000,
                "shares": 3000,
                "saves": 8000
            },
            "posts": 50
        },
    ]
    
    for case in test_cases:
        print(f"\n  {case['name']}:")
        print(f"    关键词: {case['keyword']}")
        print(f"    平台: {case['platform']}")
        
        result = compute_adaptive_trend_score(
            keyword=case["keyword"],
            platform_str=case["platform"],
            raw_stats=case["stats"],
            posts=case["posts"]
        )
        
        print(f"    ─────────────────────────")
        print(f"    Trend Score: {result['trend_score']}/100")
        print(f"    ─────────────────────────")
        print(f"    H (热度):     {result['H']:.3f}")
        print(f"    V (增速):     {result['V']:.3f}")
        print(f"    D (密度):     {result['D']:.3f}")
        print(f"    F (可行性):   {result['F']:.3f}")
        print(f"    M (商业化):   {result['M']:.3f}")
        print(f"    R (风险):     {result['R']:.3f}")
        print(f"    ─────────────────────────")
        print(f"    生命周期: {result['lifecycle']}")
        print(f"    优先级: {result['priority']}")
        print(f"    Agent就绪: {result['agent_ready']}")


def test_growth_calculation():
    """测试增长率计算"""
    print("\n" + "=" * 60)
    print("测试 4: 增长率计算")
    print("=" * 60)
    
    # 当前数据
    current_stats = {
        "views": 2000000,
        "likes": 100000,
        "comments": 4000,
        "shares": 2000,
        "saves": 6000
    }
    
    # 上一周期数据 (100% 增长)
    prev_stats_100 = {
        "views": 1000000,
        "likes": 50000,
        "comments": 2000,
        "shares": 1000,
        "saves": 3000
    }
    
    # 上一周期数据 (下降)
    prev_stats_decline = {
        "views": 4000000,
        "likes": 200000,
        "comments": 8000,
        "shares": 4000,
        "saves": 12000
    }
    
    print("\n  无历史数据 (V=0.5 中性值):")
    result_no_growth = compute_adaptive_trend_score(
        keyword="test",
        platform_str="tiktok",
        raw_stats=current_stats,
        posts=10
    )
    print(f"    V (增速): {result_no_growth['V']:.3f}")
    print(f"    Trend Score: {result_no_growth['trend_score']}")
    
    print("\n  有历史数据 (100% 增长):")
    result_with_growth = compute_adaptive_trend_score(
        keyword="test",
        platform_str="tiktok",
        raw_stats=current_stats,
        prev_raw_stats=prev_stats_100,
        posts=10
    )
    print(f"    V (增速): {result_with_growth['V']:.3f}")
    print(f"    Trend Score: {result_with_growth['trend_score']}")
    
    print("\n  有历史数据 (50% 下降):")
    result_decline = compute_adaptive_trend_score(
        keyword="test",
        platform_str="tiktok",
        raw_stats=current_stats,
        prev_raw_stats=prev_stats_decline,
        posts=10
    )
    print(f"    V (增速): {result_decline['V']:.3f}")
    print(f"    Trend Score: {result_decline['trend_score']}")
    
    print(f"\n  增长 vs 无数据: {result_with_growth['trend_score'] - result_no_growth['trend_score']:+d} 分")
    print(f"  下降 vs 无数据: {result_decline['trend_score'] - result_no_growth['trend_score']:+d} 分")
    print(f"  增长 vs 下降: {result_with_growth['trend_score'] - result_decline['trend_score']:+d} 分")


def test_crawl_file(file_path: str):
    """测试处理爬虫文件"""
    print("\n" + "=" * 60)
    print(f"测试 5: 处理爬虫文件")
    print(f"文件: {file_path}")
    print("=" * 60)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            crawl_data = json.load(f)
    except Exception as e:
        print(f"  ✗ 无法读取文件: {e}")
        return
    
    # 处理数据
    results = process_crawl_file(crawl_data)
    
    print(f"\n  处理结果:")
    print(f"  ─────────────────────────")
    
    for platform, scores in results.items():
        print(f"\n  平台: {platform.upper()}")
        print(f"  关键词数量: {len(scores)}")
        
        if scores:
            print(f"  Top 5 趋势:")
            for i, score in enumerate(scores[:5], 1):
                print(f"    {i}. {score['keyword']}: {score['trend_score']}/100 "
                      f"(H={score['H']:.2f}, V={score['V']:.2f}, "
                      f"优先级={score['priority']})")
    
    # 获取跨平台 Top 10
    print(f"\n  ─────────────────────────")
    print(f"  跨平台 Top 10 趋势:")
    
    scorer = AdaptiveTrendScorer()
    top_trends = scorer.get_top_trends(crawl_data, top_n=10, min_score=0)
    
    for i, trend in enumerate(top_trends, 1):
        print(f"    {i}. [{trend['platform']}] {trend['keyword']}: "
              f"{trend['trend_score']}/100 (优先级={trend['priority']})")


def main():
    print("=" * 60)
    print("自适应 Trend Score 计算器测试")
    print("=" * 60)
    
    # 基础测试
    test_platform_detection()
    test_metric_extraction()
    test_score_calculation()
    test_growth_calculation()
    
    # 如果提供了爬虫文件路径，测试文件处理
    if len(sys.argv) > 1:
        test_crawl_file(sys.argv[1])
    else:
        # 尝试默认路径
        default_path = "../spider6p/output/crawl_2025-12-17T13-34-50.json"
        if Path(default_path).exists():
            test_crawl_file(default_path)
        else:
            print("\n  提示: 可以传入爬虫文件路径进行测试")
            print("  用法: python test_adaptive_trend_scorer.py <crawl_file_path>")
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
