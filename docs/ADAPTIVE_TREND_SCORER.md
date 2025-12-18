# 自适应 Trend Score 计算引擎

## 概述

自适应 Trend Score 计算引擎是一个跨平台的趋势评分系统，能够根据不同社交媒体平台返回的数据特征，自动调整算法权重和计算方式，确保不同平台的分数具有可比性。

## 核心公式

```
trend_score = 0.20×H + 0.30×V + 0.15×D + 0.15×F + 0.20×M - 0.25×R
```

| 维度 | 名称 | 权重 | 说明 |
|------|------|------|------|
| H | 热度 (Hotness) | 0.20 | 当前流量和互动强度 |
| V | 增速 (Velocity) | 0.30 | 周期环比增长率（最高权重） |
| D | 密度 (Density) | 0.15 | 内容数量和创作者分布 |
| F | 可行性 (Feasibility) | 0.15 | AI 实现难度 |
| M | 商业化 (Monetization) | 0.20 | 变现潜力 |
| R | 风险 (Risk) | -0.25 | IP风险和竞争风险（惩罚项） |

**分数范围**: 0-100

---

## 支持平台（基于实际爬虫数据）

| 平台 | 状态 | 数据字段 | 数据完整度 |
|------|------|----------|-----------|
| TikTok | ✅ 完整支持 | views, likes, comments, shares, saves | 高 |
| Instagram | ✅ 完整支持 | views, likes, comments, shares | 中（数据可能为0） |
| Reddit | ✅ 完整支持 | upvotes, downvotes, score, comments | 高 |
| Twitter | ✅ 完整支持 | views, likes, comments, retweets, bookmarks | 高 |
| YouTube | ✅ 完整支持 | views, likes, comments | 中（likes/comments可能为0） |
| LinkedIn | ⚠️ 基础支持 | services（人员资料） | 低（不适合趋势评分） |

---

## 平台配置详解

### 1. TikTok（基准平台）

TikTok 作为数据最完整的平台，是其他平台校准的基准。

**可用字段**:
- `views` - 播放量
- `likes` - 点赞数
- `comments` - 评论数
- `shares` - 分享数
- `saves` - 收藏数

**权重配置**:
```python
views_weight = 1.0      # 标准权重
likes_weight = 1.0
comments_weight = 1.0
shares_weight = 1.0
saves_weight = 1.0
```

**热度计算权重**:
```python
hotness_views_weight = 0.50      # 播放量占 50%
hotness_engagement_weight = 0.30  # 互动率占 30%
hotness_posts_weight = 0.20       # 帖子数占 20%
```

---

### 2. Instagram

**可用字段**:
- `views` - 播放量/曝光量
- `likes` - 点赞数
- `comments` - 评论数
- `shares` - 分享数
- ~~`saves`~~ - 爬虫数据不可用

**权重调整**（补偿数据不完整）:
```python
views_weight = 1.0
likes_weight = 1.2      # +20% 补偿
comments_weight = 1.3   # +30% 补偿
shares_weight = 1.1     # +10% 补偿
saves_weight = 0.0      # 不可用
```

**热度计算权重**:
```python
hotness_views_weight = 0.35       # 降低到 35%
hotness_engagement_weight = 0.45  # 提高到 45%（更依赖互动）
hotness_posts_weight = 0.20
```

**商业化调整**:
- 基础商业化潜力 ×1.1（Instagram 商业化能力强）

---

### 3. Twitter/X

**可用字段**:
- `views` - 曝光量
- `likes` - 点赞数
- `comments` - 评论数（爬虫已映射 replies）
- `retweets` → `shares` - 转发数
- `bookmarks` → `saves` - 书签数

**权重配置**:
```python
views_weight = 0.8       # -20%（Twitter views 可能包含无效曝光）
likes_weight = 1.0
comments_weight = 1.2    # +20%
shares_weight = 1.5      # +50%（转发在 Twitter 非常重要）
saves_weight = 0.8
```

**热度计算权重**:
```python
hotness_views_weight = 0.40
hotness_engagement_weight = 0.40
hotness_posts_weight = 0.20
```

**商业化调整**:
- 基础商业化潜力 ×0.9（Twitter 商业化能力中等）

---

### 3. Reddit

Reddit 使用投票系统而非播放量，数据结构与其他平台不同。

**可用字段**:
- `upvotes` - 赞成票
- `downvotes` - 反对票
- `score` - 净得分 (upvotes - downvotes)
- `comments` - 评论数
- ~~`views`~~ - 不可用
- ~~`shares`~~ - 不可用

**字段映射**:
```python
# Reddit 特殊映射
views = score × 10      # 用 score 估算曝光量
likes = upvotes 或 score  # 用投票数代替点赞
```

**权重配置**:
```python
views_weight = 0.0       # 无原生 views
likes_weight = 0.0       # 用 upvotes 代替
comments_weight = 2.0    # 大幅提高（Reddit 评论是核心互动）
shares_weight = 0.0
saves_weight = 0.0
```

**热度计算权重**:
```python
hotness_views_weight = 0.0        # 无 views，不计算
hotness_engagement_weight = 0.70  # 互动占 70%（主要看 score + comments）
hotness_posts_weight = 0.30
```

**商业化调整**:
- 基础商业化潜力 ×0.7（Reddit 商业化能力较弱）

**风险评估阈值**:
| 竞争级别 | 条件 | 风险值 |
|---------|------|--------|
| 高竞争 | score > 10,000 | 0.6 |
| 中竞争 | score > 5,000 | 0.4 |
| 低竞争 | score > 1,000 | 0.2 |

---

### 4. YouTube

**可用字段**:
- `views` - 播放量
- `likes` - 点赞数（可能为0）
- `comments` - 评论数（可能为0）
- ~~`shares`~~ - 爬虫数据不可用
- ~~`saves`~~ - 爬虫数据不可用

**权重配置**:
```python
views_weight = 1.3       # +30%（YouTube 以播放量为核心）
likes_weight = 1.0
comments_weight = 1.5    # +50%（YouTube 评论更有价值）
shares_weight = 0.0
saves_weight = 0.0
```

**热度计算权重**:
```python
hotness_views_weight = 0.65       # 播放量占主导
hotness_engagement_weight = 0.20
hotness_posts_weight = 0.15
```

---

### 5. LinkedIn（数据有限）

LinkedIn 返回的是人员资料而非帖子，数据结构完全不同。

**可用字段**:
- `services` - 服务数量（唯一字段）

**特殊处理**:
- 所有互动权重设为 0
- 热度仅基于帖子数量计算
- 热度上限较低（×0.3）
- 商业化潜力 ×0.5

**注意**: LinkedIn 数据不适合趋势评分，分数会明显低于其他平台。

---

## 跨平台可比性验证

### 验证结果

| 热度级别 | 分数范围 | 差距 | 状态 |
|---------|---------|------|------|
| 爆款 (Viral) | 63-69 | 6分 | ✓ 良好 |
| 热门 (Hot) | 52-57 | 5分 | ✓ 良好 |
| 中等 (Moderate) | 38-43 | 5分 | ✓ 良好 |

*注：以上数据不含 LinkedIn（数据结构不同）*

### 增长率影响

| 场景 | TikTok | Twitter |
|------|--------|---------|
| +200% 增长 vs -30% 下降 | +17分 | +17分 |

### 关键词影响

| 关键词类型 | 分数示例 | 说明 |
|-----------|---------|------|
| AI工具类 (ai headshot) | 65分 | 高可行性、高商业化 |
| IP风险类 (ghibli filter) | 51分 | 中等可行性、有IP风险 |
| 通用类 (music) | 47分 | 中等各项 |

---

## 各维度计算详解

### H - 热度 (Hotness)

**计算公式**:
```
H = w_views × views_norm + w_engagement × engagement + w_posts × posts_norm
```

**播放量归一化**:
```python
views_norm = log(1 + views) / log(1 + 100,000,000)
```

**互动率计算**:
```python
engagement_rate = (likes + comments×2 + shares×3) / views
engagement = min(engagement_rate, 0.5) / 0.5
```

### V - 增速 (Velocity)

**计算公式**:
```python
# 各指标增长率
growth_rate = (current - previous) / previous

# 加权平均
velocity_weights = {
    "views": 0.45,
    "likes": 0.25,
    "comments": 0.15,
    "shares": 0.10,
    "saves": 0.05,
}

# 归一化到 [0, 1]
V = (weighted_growth + 1) / 4
```

**无历史数据处理**: V = 0.5（中性值）

### D - 密度 (Density)

```python
posts_norm = log(1 + posts) / log(1 + 10,000)
D = posts_norm
```

### F - 可行性 (Feasibility)

| 分数 | 说明 | 示例 |
|------|------|------|
| 5 | 非常容易 | 背景移除、图片放大 |
| 4 | 较容易 | 动漫滤镜、风格迁移 |
| 3 | 中等难度 | 通用图像处理 |
| 2 | 较难 | 复杂视频编辑 |
| 1 | 非常难 | 全新 AI 能力 |

### M - 商业化 (Monetization)

**平台调整**:
- Instagram: ×1.1
- Twitter: ×0.9
- LinkedIn: ×0.5

**流量加成**:
- views ≥ 5000万: +0.10
- views ≥ 1000万: +0.05

### R - 风险 (Risk)

```python
R = 0.6 × ip_risk + 0.4 × competition_risk
```

---

## 输出结果

### 字段说明

```json
{
    "keyword": "ai headshot",
    "platform": "tiktok",
    "platform_type": "tiktok",
    "trend_score": 75,
    "H": 0.650,
    "V": 0.720,
    "D": 0.450,
    "F": 1.000,
    "M": 0.900,
    "R": 0.100,
    "lifecycle": "rising",
    "priority": "P1",
    "agent_ready": true,
    "category": "portrait",
    "raw_metrics": {
        "views": 15000000,
        "likes": 850000,
        "comments": 32000,
        "shares": 15000,
        "saves": 28000,
        "posts": 200
    },
    "computed_at": "2024-12-18T10:30:00"
}
```

### 生命周期判断

| 生命周期 | 条件 | 说明 |
|---------|------|------|
| rising | V ≥ 0.8 | 快速上升期 |
| flash | V ≥ 0.7 且 H < 0.6 | 闪现趋势 |
| sustained | V ≥ 0.6 且 H ≥ 0.6 | 持续增长 |
| evergreen | H ≥ 0.7 且 D ≥ 0.6 且 V < 0.8 | 常青趋势 |
| declining | V ≤ 0.2 | 下降期 |

### 优先级判断

| 优先级 | 条件 | 建议行动 |
|--------|------|---------|
| P0 | score ≥ 85, M ≥ 0.85, F ≥ 0.8 | 立即开发 |
| P1 | score ≥ 75, M ≥ 0.70, F ≥ 0.6 | 优先开发 |
| P2 | score ≥ 60, M ≥ 0.50, F ≥ 0.5 | 计划开发 |
| P3 | 其他 | 观察或放弃 |

---

## 使用示例

### 单条数据计算

```python
from app.services.adaptive_trend_scorer import compute_adaptive_trend_score

result = compute_adaptive_trend_score(
    keyword="ai headshot",
    platform_str="tiktok",
    raw_stats={
        "views": 2000000,
        "likes": 120000,
        "comments": 5000,
        "shares": 3000,
        "saves": 8000
    },
    posts=50
)

print(f"Trend Score: {result['trend_score']}/100")
print(f"Priority: {result['priority']}")
```

### 处理爬虫数据文件

```python
from app.services.adaptive_trend_scorer import process_crawl_file
import json

with open("spider6p/output/crawl_2025-12-17T13-34-50.json") as f:
    crawl_data = json.load(f)

results = process_crawl_file(crawl_data)

for platform, scores in results.items():
    print(f"\n{platform}:")
    for score in scores[:5]:
        print(f"  {score['keyword']}: {score['trend_score']}/100")
```

### 获取跨平台 Top N

```python
from app.services.adaptive_trend_scorer import AdaptiveTrendScorer

scorer = AdaptiveTrendScorer()
top_trends = scorer.get_top_trends(crawl_data, top_n=10, min_score=50)

for trend in top_trends:
    print(f"[{trend['platform']}] {trend['keyword']}: {trend['trend_score']}")
```

---

## 版本历史

| 版本 | 日期 | 变更 |
|------|------|------|
| 1.1 | 2024-12-18 | 基于实际爬虫数据重构，移除未支持平台 |
| 1.0 | 2024-12-18 | 初始版本 |
