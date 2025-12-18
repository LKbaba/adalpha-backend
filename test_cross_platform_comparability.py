#!/usr/bin/env python3
"""
跨平台分数可比性验证测试

基于实际爬虫数据结构验证不同平台在相同热度级别下的分数是否具有可比性

实际支持的平台:
- TikTok: views, likes, comments, shares, saves (完整)
- Instagram: views, likes, comments, shares (数据可能为0)
- Twitter: views, likes, comments, retweets, bookmarks (完整)
- YouTube: views, likes, comments (likes/comments 可能为0)
- LinkedIn: services (人员资料，数据有限)
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from app.services.adaptive_trend_scorer import compute_adaptive_trend_score


def test_viral_level():
    """测试爆款级别 - 各平台顶级热度"""
    print("\n" + "=" * 70)
    print("测试 1: 爆款级别 (Viral) - 预期分数范围: 60-85")
    print("=" * 70)
    
    test_cases = [
        {
            "name": "TikTok 爆款视频",
            "keyword": "ai headshot",
            "platform": "tiktok",
            "stats": {
                "views": 50000000,      # 5000万播放
                "likes": 3000000,       # 300万赞
                "comments": 150000,     # 15万评论
                "shares": 80000,        # 8万分享
                "saves": 200000         # 20万收藏
            },
            "posts": 500
        },
        {
            "name": "Instagram 爆款帖子",
            "keyword": "ai headshot",
            "platform": "instagram",
            "stats": {
                "views": 30000000,      # 3000万曝光
                "likes": 2000000,       # 200万赞
                "comments": 100000,     # 10万评论
                "shares": 50000         # 5万分享
            },
            "posts": 400
        },
        {
            "name": "Twitter 爆款推文",
            "keyword": "ai headshot",
            "platform": "twitter",
            "stats": {
                "views": 20000000,      # 2000万曝光
                "likes": 500000,        # 50万赞
                "comments": 30000,      # 3万评论
                "retweets": 100000,     # 10万转发
                "bookmarks": 50000      # 5万书签
            },
            "posts": 300
        },
        {
            "name": "YouTube 爆款视频",
            "keyword": "ai headshot",
            "platform": "youtube",
            "stats": {
                "views": 100000000,     # 1亿播放
                "likes": 2000000,       # 200万赞
                "comments": 80000       # 8万评论
            },
            "posts": 200
        },
        {
            "name": "Reddit 爆款帖子",
            "keyword": "ai headshot",
            "platform": "reddit",
            "stats": {
                "upvotes": 50000,       # 5万赞成
                "downvotes": 2000,      # 2千反对
                "score": 48000,         # 净得分
                "comments": 3000        # 3千评论
            },
            "posts": 100
        },
    ]
    
    results = []
    for case in test_cases:
        result = compute_adaptive_trend_score(
            keyword=case["keyword"],
            platform_str=case["platform"],
            raw_stats=case["stats"],
            posts=case["posts"]
        )
        results.append((case["name"], case["platform"], result["trend_score"]))
        
        print(f"\n  {case['name']}:")
        print(f"    Trend Score: {result['trend_score']}/100")
        print(f"    H={result['H']:.2f}, V={result['V']:.2f}, M={result['M']:.2f}, R={result['R']:.2f}")
        print(f"    优先级: {result['priority']}, 生命周期: {result['lifecycle']}")
    
    # 验证分数范围
    scores = [r[2] for r in results]
    min_score, max_score = min(scores), max(scores)
    spread = max_score - min_score
    
    print(f"\n  ─────────────────────────────────────────────────────────────────")
    print(f"  分数范围: {min_score} - {max_score} (差距: {spread})")
    print(f"  平均分: {sum(scores)/len(scores):.1f}")
    
    if spread <= 25:
        print(f"  ✓ 跨平台可比性良好 (差距 ≤ 25)")
    else:
        print(f"  ⚠ 跨平台差距较大，可能需要调整权重")
    
    return results


def test_hot_level():
    """测试热门级别 - 各平台中上热度"""
    print("\n" + "=" * 70)
    print("测试 2: 热门级别 (Hot) - 预期分数范围: 45-65")
    print("=" * 70)
    
    test_cases = [
        {
            "name": "TikTok 热门视频",
            "keyword": "anime filter",
            "platform": "tiktok",
            "stats": {
                "views": 5000000,       # 500万播放
                "likes": 300000,        # 30万赞
                "comments": 15000,      # 1.5万评论
                "shares": 8000,         # 8千分享
                "saves": 20000          # 2万收藏
            },
            "posts": 100
        },
        {
            "name": "Instagram 热门帖子",
            "keyword": "anime filter",
            "platform": "instagram",
            "stats": {
                "views": 3000000,       # 300万曝光
                "likes": 200000,        # 20万赞
                "comments": 10000,      # 1万评论
                "shares": 5000          # 5千分享
            },
            "posts": 80
        },
        {
            "name": "Twitter 热门推文",
            "keyword": "anime filter",
            "platform": "twitter",
            "stats": {
                "views": 2000000,       # 200万曝光
                "likes": 50000,         # 5万赞
                "comments": 3000,       # 3千评论
                "retweets": 10000,      # 1万转发
                "bookmarks": 5000       # 5千书签
            },
            "posts": 60
        },
        {
            "name": "YouTube 热门视频",
            "keyword": "anime filter",
            "platform": "youtube",
            "stats": {
                "views": 10000000,      # 1000万播放
                "likes": 200000,        # 20万赞
                "comments": 8000        # 8千评论
            },
            "posts": 50
        },
        {
            "name": "Reddit 热门帖子",
            "keyword": "anime filter",
            "platform": "reddit",
            "stats": {
                "upvotes": 8000,        # 8千赞成
                "downvotes": 500,       # 500反对
                "score": 7500,          # 净得分
                "comments": 500         # 500评论
            },
            "posts": 30
        },
    ]
    
    results = []
    for case in test_cases:
        result = compute_adaptive_trend_score(
            keyword=case["keyword"],
            platform_str=case["platform"],
            raw_stats=case["stats"],
            posts=case["posts"]
        )
        results.append((case["name"], case["platform"], result["trend_score"]))
        
        print(f"\n  {case['name']}:")
        print(f"    Trend Score: {result['trend_score']}/100")
        print(f"    H={result['H']:.2f}, V={result['V']:.2f}, M={result['M']:.2f}, R={result['R']:.2f}")
        print(f"    优先级: {result['priority']}, 生命周期: {result['lifecycle']}")
    
    scores = [r[2] for r in results]
    min_score, max_score = min(scores), max(scores)
    spread = max_score - min_score
    
    print(f"\n  ─────────────────────────────────────────────────────────────────")
    print(f"  分数范围: {min_score} - {max_score} (差距: {spread})")
    print(f"  平均分: {sum(scores)/len(scores):.1f}")
    
    if spread <= 25:
        print(f"  ✓ 跨平台可比性良好 (差距 ≤ 25)")
    else:
        print(f"  ⚠ 跨平台差距较大，可能需要调整权重")
    
    return results


def test_moderate_level():
    """测试中等级别 - 各平台一般热度"""
    print("\n" + "=" * 70)
    print("测试 3: 中等级别 (Moderate) - 预期分数范围: 35-50")
    print("=" * 70)
    
    test_cases = [
        {
            "name": "TikTok 普通视频",
            "keyword": "music",
            "platform": "tiktok",
            "stats": {
                "views": 500000,        # 50万播放
                "likes": 25000,         # 2.5万赞
                "comments": 1000,       # 1千评论
                "shares": 500,          # 500分享
                "saves": 1500           # 1500收藏
            },
            "posts": 20
        },
        {
            "name": "Instagram 普通帖子",
            "keyword": "music",
            "platform": "instagram",
            "stats": {
                "views": 300000,        # 30万曝光
                "likes": 15000,         # 1.5万赞
                "comments": 800,        # 800评论
                "shares": 300           # 300分享
            },
            "posts": 15
        },
        {
            "name": "Twitter 普通推文",
            "keyword": "music",
            "platform": "twitter",
            "stats": {
                "views": 200000,        # 20万曝光
                "likes": 5000,          # 5千赞
                "comments": 300,        # 300评论
                "retweets": 1000,       # 1千转发
                "bookmarks": 500        # 500书签
            },
            "posts": 12
        },
        {
            "name": "YouTube 普通视频",
            "keyword": "music",
            "platform": "youtube",
            "stats": {
                "views": 1000000,       # 100万播放
                "likes": 20000,         # 2万赞
                "comments": 800         # 800评论
            },
            "posts": 10
        },
        {
            "name": "Reddit 普通帖子",
            "keyword": "music",
            "platform": "reddit",
            "stats": {
                "upvotes": 1500,        # 1500赞成
                "downvotes": 100,       # 100反对
                "score": 1400,          # 净得分
                "comments": 100         # 100评论
            },
            "posts": 10
        },
        {
            "name": "LinkedIn (数据有限)",
            "keyword": "music",
            "platform": "linkedin",
            "stats": {
                "services": 0           # LinkedIn 只有这个字段
            },
            "posts": 10
        },
    ]
    
    results = []
    for case in test_cases:
        result = compute_adaptive_trend_score(
            keyword=case["keyword"],
            platform_str=case["platform"],
            raw_stats=case["stats"],
            posts=case["posts"]
        )
        results.append((case["name"], case["platform"], result["trend_score"]))
        
        print(f"\n  {case['name']}:")
        print(f"    Trend Score: {result['trend_score']}/100")
        print(f"    H={result['H']:.2f}, V={result['V']:.2f}, M={result['M']:.2f}, R={result['R']:.2f}")
        print(f"    优先级: {result['priority']}, 生命周期: {result['lifecycle']}")
    
    # 排除 LinkedIn 计算可比性（数据结构不同）
    scores_without_linkedin = [r[2] for r in results if r[1] != "linkedin"]
    min_score, max_score = min(scores_without_linkedin), max(scores_without_linkedin)
    spread = max_score - min_score
    
    print(f"\n  ─────────────────────────────────────────────────────────────────")
    print(f"  分数范围 (不含LinkedIn): {min_score} - {max_score} (差距: {spread})")
    print(f"  平均分 (不含LinkedIn): {sum(scores_without_linkedin)/len(scores_without_linkedin):.1f}")
    
    if spread <= 20:
        print(f"  ✓ 跨平台可比性良好 (差距 ≤ 20)")
    else:
        print(f"  ⚠ 跨平台差距较大，可能需要调整权重")
    
    return results


def test_growth_impact():
    """测试增长率对分数的影响"""
    print("\n" + "=" * 70)
    print("测试 4: 增长率影响验证")
    print("=" * 70)
    
    # 基础数据
    current_stats = {
        "views": 5000000,
        "likes": 300000,
        "comments": 15000,
        "shares": 8000,
        "saves": 20000
    }
    
    # 不同增长场景
    scenarios = [
        ("无历史数据", None),
        ("+200% 增长", {
            "views": 1666666,
            "likes": 100000,
            "comments": 5000,
            "shares": 2666,
            "saves": 6666
        }),
        ("+50% 增长", {
            "views": 3333333,
            "likes": 200000,
            "comments": 10000,
            "shares": 5333,
            "saves": 13333
        }),
        ("0% 增长", current_stats.copy()),
        ("-30% 下降", {
            "views": 7142857,
            "likes": 428571,
            "comments": 21428,
            "shares": 11428,
            "saves": 28571
        }),
    ]
    
    print("\n  TikTok 平台测试:")
    tiktok_results = []
    for name, prev_stats in scenarios:
        result = compute_adaptive_trend_score(
            keyword="anime filter",
            platform_str="tiktok",
            raw_stats=current_stats,
            prev_raw_stats=prev_stats,
            posts=100
        )
        tiktok_results.append((name, result["trend_score"], result["V"]))
        print(f"    {name}: Score={result['trend_score']}, V={result['V']:.3f}")
    
    print("\n  Twitter 平台测试:")
    twitter_current = {
        "views": 2000000,
        "likes": 50000,
        "comments": 3000,
        "retweets": 10000,
        "bookmarks": 5000
    }
    twitter_scenarios = [
        ("无历史数据", None),
        ("+200% 增长", {
            "views": 666666,
            "likes": 16666,
            "comments": 1000,
            "retweets": 3333,
            "bookmarks": 1666
        }),
        ("+50% 增长", {
            "views": 1333333,
            "likes": 33333,
            "comments": 2000,
            "retweets": 6666,
            "bookmarks": 3333
        }),
        ("0% 增长", twitter_current.copy()),
        ("-30% 下降", {
            "views": 2857142,
            "likes": 71428,
            "comments": 4285,
            "retweets": 14285,
            "bookmarks": 7142
        }),
    ]
    
    twitter_results = []
    for name, prev_stats in twitter_scenarios:
        result = compute_adaptive_trend_score(
            keyword="anime filter",
            platform_str="twitter",
            raw_stats=twitter_current,
            prev_raw_stats=prev_stats,
            posts=60
        )
        twitter_results.append((name, result["trend_score"], result["V"]))
        print(f"    {name}: Score={result['trend_score']}, V={result['V']:.3f}")
    
    # 验证增长率影响
    print(f"\n  ─────────────────────────────────────────────────────────────────")
    print(f"  TikTok 增长影响: +200%增长 vs -30%下降 = {tiktok_results[1][1] - tiktok_results[4][1]:+d} 分")
    print(f"  Twitter 增长影响: +200%增长 vs -30%下降 = {twitter_results[1][1] - twitter_results[4][1]:+d} 分")


def test_keyword_impact():
    """测试关键词类型对分数的影响"""
    print("\n" + "=" * 70)
    print("测试 5: 关键词类型影响验证")
    print("=" * 70)
    
    # 相同数据，不同关键词
    stats = {
        "views": 5000000,
        "likes": 300000,
        "comments": 15000,
        "shares": 8000,
        "saves": 20000
    }
    
    keywords = [
        ("ai headshot", "AI工具-高可行性高商业化"),
        ("background remover", "AI工具-高可行性高商业化"),
        ("ghibli filter", "IP风险-中等可行性"),
        ("arcane filter", "IP风险-中等可行性"),
        ("music", "通用关键词-中等各项"),
        ("dance", "通用关键词-中等各项"),
    ]
    
    print("\n  TikTok 平台 - 相同数据不同关键词:")
    for keyword, desc in keywords:
        result = compute_adaptive_trend_score(
            keyword=keyword,
            platform_str="tiktok",
            raw_stats=stats,
            posts=100
        )
        print(f"    {keyword:20s} ({desc})")
        print(f"      Score={result['trend_score']}, F={result['F']:.2f}, M={result['M']:.2f}, R={result['R']:.2f}")


def test_real_crawl_data():
    """测试真实爬虫数据"""
    print("\n" + "=" * 70)
    print("测试 6: 真实爬虫数据验证")
    print("=" * 70)
    
    import json
    crawl_file = Path(__file__).parent.parent / "spider6p/output/crawl_2025-12-17T13-34-50.json"
    
    if not crawl_file.exists():
        print("  ⚠ 爬虫数据文件不存在，跳过测试")
        return
    
    with open(crawl_file, 'r', encoding='utf-8') as f:
        crawl_data = json.load(f)
    
    from app.services.adaptive_trend_scorer import process_crawl_file, AdaptiveTrendScorer
    
    results = process_crawl_file(crawl_data)
    
    print(f"\n  实际爬虫数据处理结果:")
    print(f"  ─────────────────────────────────────────────────────────────────")
    
    all_scores = []
    for platform, scores in results.items():
        if scores:
            avg_score = sum(s['trend_score'] for s in scores) / len(scores)
            all_scores.extend([(platform, s['keyword'], s['trend_score']) for s in scores])
            print(f"  {platform.upper():12s}: {len(scores)} 关键词, 平均分 {avg_score:.1f}")
            for s in scores[:3]:
                print(f"    - {s['keyword']}: {s['trend_score']}/100 (H={s['H']:.2f})")
    
    # 跨平台排名
    print(f"\n  跨平台 Top 10:")
    all_scores.sort(key=lambda x: x[2], reverse=True)
    for i, (platform, keyword, score) in enumerate(all_scores[:10], 1):
        print(f"    {i}. [{platform}] {keyword}: {score}/100")


def print_summary():
    """打印总结"""
    print("\n" + "=" * 70)
    print("跨平台可比性验证总结")
    print("=" * 70)
    print("""
  实际支持的平台 (基于爬虫数据):
  ─────────────────────────────────────────────────────────────────
  | 平台      | 数据字段                              | 数据完整度 |
  |-----------|---------------------------------------|-----------|
  | TikTok    | views, likes, comments, shares, saves | 高        |
  | Instagram | views, likes, comments, shares        | 中 (可能为0)|
  | Reddit    | upvotes, downvotes, score, comments   | 高        |
  | Twitter   | views, likes, comments, retweets, bookmarks | 高   |
  | YouTube   | views, likes, comments                | 中 (可能为0)|
  | LinkedIn  | services (人员资料)                   | 低 (不适合) |
  ─────────────────────────────────────────────────────────────────
  
  校准策略:
  - TikTok: 基准平台，标准权重
  - Instagram: 提高互动权重补偿数据不完整
  - Reddit: 用 score 体系映射，大幅提高 comments 权重
  - Twitter: 提高 retweets 权重，适应传播特点
  - YouTube: 提高 views 权重，适应长视频特点
  - LinkedIn: 数据结构不同，分数上限较低
  
  验证标准:
  - 同级别热度跨平台分数差距 ≤ 25 分 (不含LinkedIn)
  - 增长率变化应产生 10-20 分的影响
  - AI工具类关键词应比通用关键词高 15-25 分
    """)


def main():
    print("=" * 70)
    print("跨平台分数可比性验证测试")
    print("基于实际爬虫数据结构: TikTok, Instagram, Reddit, Twitter, YouTube, LinkedIn")
    print("=" * 70)
    
    test_viral_level()
    test_hot_level()
    test_moderate_level()
    test_growth_impact()
    test_keyword_impact()
    test_real_crawl_data()
    print_summary()
    
    print("\n" + "=" * 70)
    print("测试完成")
    print("=" * 70)


if __name__ == "__main__":
    main()
