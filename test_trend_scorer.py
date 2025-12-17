"""
Trend Score 计算测试脚本

用法: python test_trend_scorer.py
"""

from app.services.trend_scorer import trend_scorer, compute_trend_score

def test_basic():
    """基础测试"""
    print("=" * 60)
    print("Trend Score 计算测试")
    print("=" * 60)
    
    # 测试数据
    test_cases = [
        {
            "name": "高热度 TikTok 音乐",
            "keyword": "music",
            "platform": "tiktok",
            "metrics": {
                "views": 15000000,
                "likes": 850000,
                "comments": 32000,
                "shares": 15000,
                "saves": 28000,
                "posts": 1420
            }
        },
        {
            "name": "AI 头像生成器",
            "keyword": "ai headshot",
            "platform": "linkedin",
            "metrics": {
                "views": 5000000,
                "likes": 200000,
                "comments": 8000,
                "shares": 5000,
                "saves": 15000,
                "posts": 500
            }
        },
        {
            "name": "动漫滤镜 (有 IP 风险)",
            "keyword": "ghibli filter",
            "platform": "instagram",
            "metrics": {
                "views": 8000000,
                "likes": 400000,
                "comments": 15000,
                "shares": 8000,
                "saves": 20000,
                "posts": 800
            }
        },
        {
            "name": "低热度新趋势",
            "keyword": "new trend",
            "platform": "twitter",
            "metrics": {
                "views": 100000,
                "likes": 5000,
                "comments": 200,
                "shares": 100,
                "saves": 300,
                "posts": 50
            }
        }
    ]
    
    print("\n" + "-" * 60)
    print("测试结果")
    print("-" * 60)
    
    for case in test_cases:
        result = compute_trend_score(
            keyword=case["keyword"],
            platform=case["platform"],
            metrics=case["metrics"]
        )
        
        print(f"\n📊 {case['name']}")
        print(f"   关键词: {result['keyword']}")
        print(f"   平台: {result['platform']}")
        print(f"   类别: {result['category']}")
        print(f"   ─────────────────────────")
        print(f"   Trend Score: {result['trend_score']}/100")
        print(f"   ─────────────────────────")
        print(f"   H (热度):     {result['H']:.3f}")
        print(f"   V (增速):     {result['V']:.3f}")
        print(f"   D (密度):     {result['D']:.3f}")
        print(f"   F (可行性):   {result['F']:.3f}")
        print(f"   M (商业化):   {result['M']:.3f}")
        print(f"   R (风险):     {result['R']:.3f}")
        print(f"   ─────────────────────────")
        print(f"   生命周期: {result['lifecycle']}")
        print(f"   优先级: {result['priority']}")
        print(f"   Agent就绪: {'✅' if result['agent_ready'] else '❌'}")
    
    print("\n" + "=" * 60)
    print("公式说明")
    print("=" * 60)
    print("""
trend_score = 0.20*H + 0.30*V + 0.15*D + 0.15*F + 0.20*M - 0.25*R

各维度说明:
- H (Hotness):      热度 - 基于播放量、互动、帖子数
- V (Velocity):     增速 - 基于各指标增长率
- D (Density):      密度 - 基于帖子数量和创作者多样性
- F (Feasibility):  可行性 - AI 技术可行性 (1-5 → 0-1)
- M (Monetization): 商业化 - 商业潜力 + 流量加成
- R (Risk):         风险 - IP 风险 + 竞争风险 (惩罚项)

Agent 就绪条件:
- trend_score >= 60 AND F >= 0.5

优先级分级:
- P0: score >= 85, M >= 0.85, F >= 0.8
- P1: score >= 75, M >= 0.70, F >= 0.6
- P2: score >= 60, M >= 0.50, F >= 0.5
- P3: 不推荐
""")


def test_with_growth():
    """测试增长率计算"""
    print("\n" + "=" * 60)
    print("增长率测试")
    print("=" * 60)
    
    # 当前数据
    current = {
        "views": 2000000,
        "likes": 100000,
        "comments": 5000,
        "shares": 2000,
        "saves": 3000,
        "posts": 200
    }
    
    # 上一周期数据
    previous = {
        "views": 1000000,
        "likes": 50000,
        "comments": 2500,
        "shares": 1000,
        "saves": 1500,
        "posts": 100
    }
    
    # 无增长数据
    result_no_growth = compute_trend_score(
        keyword="test",
        platform="tiktok",
        metrics=current
    )
    
    # 有增长数据
    result_with_growth = compute_trend_score(
        keyword="test",
        platform="tiktok",
        metrics=current,
        prev_metrics=previous
    )
    
    print(f"\n无历史数据:")
    print(f"  V (增速): {result_no_growth['V']:.3f}")
    print(f"  Trend Score: {result_no_growth['trend_score']}")
    
    print(f"\n有历史数据 (100% 增长):")
    print(f"  V (增速): {result_with_growth['V']:.3f}")
    print(f"  Trend Score: {result_with_growth['trend_score']}")
    
    print(f"\n增长率对分数的影响: +{result_with_growth['trend_score'] - result_no_growth['trend_score']} 分")


if __name__ == "__main__":
    test_basic()
    test_with_growth()
    print("\n✅ 测试完成!")
