#!/usr/bin/env python3
"""
generate_report.py — 根据采集数据和评分准则生成每日精选报告
输出 Hugo 兼容的 Markdown 文件到 content/posts/YYYY-MM-DD.md
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data" / "raw"
POSTS_DIR = ROOT / "content" / "posts"
POSTS_DIR.mkdir(parents=True, exist_ok=True)

# === 评分相关配置 ===

# 核心关注领域关键词 (+5 bonus)
CORE_FOCUS_KEYWORDS = [
    "architecture", "attention", "moe", "mixture of experts", "transformer variant",
    "state space", "mamba", "rwkv", "linear attention", "sparse",
    "pretraining", "pre-training", "pretrain", "pre-train",
    "training data", "data quality", "data curation", "data mixture", "data filtering",
    "synthetic data", "curriculum learning", "data composition", "scaling law",
    "optimizer", "adam", "lion", "sophia", "learning rate", "schedule",
    "training stability", "loss spike", "gradient", "weight decay", "warmup",
    "muon", "schedule-free",
]

# 一般关注领域关键词 (+3 bonus)
GENERAL_FOCUS_KEYWORDS = [
    "reasoning", "chain-of-thought", "cot", "step-by-step",
    "agent", "tool use", "function calling", "tool-use",
    "long context", "rag", "retrieval", "retrieval-augmented",
    "multimodal", "vision-language", "multi-modal",
    "quantization", "distillation", "inference", "efficiency", "pruning",
    "knowledge distillation", "speculative decoding",
]

# 知名机构
NOTABLE_INSTITUTIONS = [
    "openai", "anthropic", "deepmind", "google research", "google brain",
    "meta ai", "fair", "microsoft research", "nvidia", "apple",
    "tsinghua", "peking university", "stanford", "mit", "berkeley",
    "carnegie mellon", "cmu", "princeton", "oxford", "cambridge",
    "allen ai", "ai2", "together ai", "mistral", "cohere",
    "baidu", "alibaba", "tencent", "bytedance", "zhipu",
]

# 热门话题关键词
HOT_TOPICS = [
    "gpt", "claude", "gemini", "llama", "qwen", "deepseek",
    "o1", "o3", "reasoning model", "world model",
]


def score_paper(paper: dict) -> int:
    """基于规则的论文评分（满分 100）"""
    score = 0
    title = paper.get("title", "").lower()
    summary = paper.get("summary", "").lower()
    authors = " ".join(paper.get("authors", [])).lower()
    content = f"{title} {summary} {authors}"

    # 1. 新颖性 (0-30): 基于关键词和标题模式估算
    novelty = 10  # 基础分
    novelty_signals = ["novel", "new approach", "first", "introduce", "propose",
                       "state-of-the-art", "sota", "breakthrough", "surpass",
                       "outperform", "rethink", "beyond", "revisit"]
    for kw in novelty_signals:
        if kw in content:
            novelty += 3
    incremental_signals = ["slightly", "marginal", "minor improvement", "small gain"]
    for kw in incremental_signals:
        if kw in content:
            novelty -= 3
    score += max(0, min(30, novelty))

    # 2. 影响力信号 (0-25): 机构 + 作者
    influence = 5  # 基础分
    for inst in NOTABLE_INSTITUTIONS:
        if inst in content:
            influence += 8
            break
    for topic in HOT_TOPICS:
        if topic in content:
            influence += 4
            break
    # 多作者协作通常说明大项目
    num_authors = len(paper.get("authors", []))
    if num_authors >= 10:
        influence += 3
    elif num_authors >= 5:
        influence += 1
    score += max(0, min(25, influence))

    # 3. 实用性 (0-20): 基于内容关键词
    practicality = 5  # 基础分
    practical_signals = ["code", "github", "open-source", "open source", "released",
                         "available at", "implementation", "benchmark", "reproducib",
                         "library", "toolkit", "framework", "pip install"]
    for kw in practical_signals:
        if kw in content:
            practicality += 4
    score += max(0, min(20, practicality))

    # 4. 话题相关度 (0-15): 匹配关注领域
    relevance = 3  # 基础分
    for kw in CORE_FOCUS_KEYWORDS:
        if kw in content:
            relevance += 3
    for kw in GENERAL_FOCUS_KEYWORDS:
        if kw in content:
            relevance += 1.5
    score += max(0, min(15, int(relevance)))

    # 5. 写作质量 (0-10): 粗略估算
    quality = 5  # 默认中等
    if len(summary) > 500:  # 详细摘要
        quality += 2
    if len(summary) < 100:  # 过短摘要
        quality -= 2
    score += max(0, min(10, quality))

    # === 领域加权 bonus ===
    core_match = any(kw in content for kw in CORE_FOCUS_KEYWORDS)
    general_match = any(kw in content for kw in GENERAL_FOCUS_KEYWORDS)
    if core_match:
        score += 5
    elif general_match:
        score += 3

    # 降权: survey
    if "survey" in title or "comprehensive review" in title:
        score -= 5

    return max(0, min(100, score))


def score_news(item: dict) -> int:
    """对 HN 新闻评分"""
    score = 0
    hn_score = item.get("score", 0)
    comments = item.get("comments", 0)
    title = item.get("title", "").lower()

    # 基于 HN score 映射到 0-40
    if hn_score >= 500:
        score += 40
    elif hn_score >= 300:
        score += 30
    elif hn_score >= 200:
        score += 25
    elif hn_score >= 100:
        score += 20

    # 评论热度 0-20
    if comments >= 300:
        score += 20
    elif comments >= 100:
        score += 15
    elif comments >= 50:
        score += 10
    else:
        score += 5

    # AI 话题相关度 0-20
    content = title
    relevance = 5
    for kw in CORE_FOCUS_KEYWORDS:
        if kw in content:
            relevance += 5
    for kw in GENERAL_FOCUS_KEYWORDS:
        if kw in content:
            relevance += 3
    for kw in HOT_TOPICS:
        if kw in content:
            relevance += 3
    score += min(20, relevance)

    # 实用性 0-20
    score += 10  # 默认

    return max(0, min(100, score))


def score_project(item: dict) -> int:
    """对 GitHub 项目评分"""
    score = 0
    stars_today = item.get("stars_today", 0)
    total_stars = item.get("total_stars", 0)
    desc = item.get("description", "").lower()
    title = item.get("title", "").lower()
    content = f"{title} {desc}"

    # 今日 star 0-30
    if stars_today >= 500:
        score += 30
    elif stars_today >= 200:
        score += 25
    elif stars_today >= 100:
        score += 20
    elif stars_today >= 50:
        score += 15
    else:
        score += 10

    # 总 star 0-20
    if total_stars >= 10000:
        score += 20
    elif total_stars >= 5000:
        score += 15
    elif total_stars >= 1000:
        score += 10
    else:
        score += 5

    # 相关度 0-30
    relevance = 5
    for kw in CORE_FOCUS_KEYWORDS:
        if kw in content:
            relevance += 5
    for kw in GENERAL_FOCUS_KEYWORDS:
        if kw in content:
            relevance += 3
    score += min(30, relevance)

    # 实用性 0-20
    score += 15  # 开源项目默认实用

    return max(0, min(100, score))


def categorize_paper(paper: dict) -> str:
    """为论文分配领域标签"""
    content = f"{paper.get('title', '')} {paper.get('summary', '')}".lower()
    
    categories_map = {
        "🏗️ Architecture": ["architecture", "attention", "transformer", "moe", "mixture of experts",
                             "state space", "mamba", "rwkv", "linear attention"],
        "📊 Pre-training Data": ["pretraining data", "data quality", "data curation", "data mixture",
                                  "synthetic data", "curriculum learning", "scaling law", "data filtering"],
        "⚡ Optimizer": ["optimizer", "adam", "learning rate", "schedule", "training stability",
                         "gradient", "weight decay", "lion", "sophia", "muon"],
        "🧠 Reasoning": ["reasoning", "chain-of-thought", "cot", "step-by-step", "mathematical"],
        "🤖 Agent": ["agent", "tool use", "function calling", "planning", "tool-use"],
        "📚 RAG/Retrieval": ["rag", "retrieval", "long context", "retrieval-augmented"],
        "🖼️ Multimodal": ["multimodal", "vision-language", "multi-modal", "image-text"],
        "🚀 Efficiency": ["quantization", "distillation", "pruning", "inference speed", "efficient"],
        "🗣️ NLP": ["nlp", "language model", "text generation", "sentiment", "translation"],
        "👁️ Vision": ["computer vision", "image", "object detection", "segmentation", "video"],
    }
    
    for label, keywords in categories_map.items():
        if any(kw in content for kw in keywords):
            return label
    
    return "📄 General AI"


def generate_markdown(data: dict, date_str: str) -> str:
    """生成 Hugo 兼容的 Markdown 报告"""
    papers = data.get("papers", [])
    news = data.get("news", [])
    projects = data.get("projects", [])

    # 评分
    scored_papers = []
    for p in papers:
        s = score_paper(p)
        if s >= 60:  # 入选门槛
            p["score"] = s
            p["category"] = categorize_paper(p)
            scored_papers.append(p)
    scored_papers.sort(key=lambda x: x["score"], reverse=True)
    scored_papers = scored_papers[:12]  # 最多 12 篇

    scored_news = []
    for n in news:
        s = score_news(n)
        n["score"] = s
        scored_news.append(n)
    scored_news.sort(key=lambda x: x["score"], reverse=True)
    scored_news = scored_news[:5]  # 最多 5 条

    scored_projects = []
    for p in projects:
        s = score_project(p)
        p["score"] = s
        scored_projects.append(p)
    scored_projects.sort(key=lambda x: x["score"], reverse=True)
    scored_projects = scored_projects[:5]  # 最多 5 个

    # 统计
    total_scanned = len(papers)
    total_selected = len(scored_papers)
    avg_score = sum(p["score"] for p in scored_papers) / len(scored_papers) if scored_papers else 0
    max_score = scored_papers[0]["score"] if scored_papers else 0

    # 构建 Markdown
    lines = []
    
    # Hugo front matter
    lines.append("---")
    lines.append(f'title: "🗞️ AI 研究精选 — {date_str}"')
    lines.append(f'date: {date_str}T08:00:00+08:00')
    lines.append(f'summary: "今日精选 {total_selected} 篇论文，{len(scored_news)} 条新闻，{len(scored_projects)} 个开源项目"')
    tags = set()
    for p in scored_papers:
        cat = p.get("category", "").split(" ", 1)[-1] if p.get("category") else ""
        if cat:
            tags.add(cat)
    lines.append(f'tags: {json.dumps(list(tags), ensure_ascii=False)}')
    lines.append("draft: false")
    lines.append("---")
    lines.append("")

    # Today's Highlight
    if scored_papers:
        lines.append("## 🌟 Today's Highlight")
        lines.append("")
        top = scored_papers[0]
        lines.append(f"### [{top['title']}]({top['url']})")
        lines.append("")
        authors_str = ", ".join(top.get("authors", [])[:5])
        if len(top.get("authors", [])) > 5:
            authors_str += " et al."
        lines.append(f"**Authors:** {authors_str}")
        lines.append("")
        lines.append(f"**Score:** {top['score']}/100 | **Category:** {top.get('category', 'N/A')}")
        lines.append("")
        # 显示摘要（截取前 500 字符）
        summary = top.get("summary", "")
        if len(summary) > 500:
            summary = summary[:500] + "..."
        lines.append(f"> {summary}")
        lines.append("")
        lines.append("---")
        lines.append("")

    # 论文精选
    if scored_papers:
        lines.append("## 📑 论文精选")
        lines.append("")
        for p in scored_papers:
            lines.append(f"### [{p['title']}]({p['url']})")
            lines.append("")
            lines.append(f"- 🏷️ **领域**: {p.get('category', 'N/A')}")
            lines.append(f"- 📊 **评分**: {p['score']}/100")
            
            # 一句话总结（用摘要首句）
            summary = p.get("summary", "")
            first_sentence = summary.split(". ")[0] + "." if summary else "N/A"
            if len(first_sentence) > 200:
                first_sentence = first_sentence[:200] + "..."
            lines.append(f"- 📝 **摘要**: {first_sentence}")
            
            authors = ", ".join(p.get("authors", [])[:3])
            if len(p.get("authors", [])) > 3:
                authors += " et al."
            lines.append(f"- 👤 **作者**: {authors}")
            
            cats = ", ".join(p.get("categories", [])[:3])
            lines.append(f"- 📂 **分类**: {cats}")
            lines.append("")

        lines.append("---")
        lines.append("")

    # 社区热点
    if scored_news or scored_projects:
        lines.append("## 🔥 社区热点")
        lines.append("")

        if scored_news:
            lines.append("### Hacker News")
            lines.append("")
            for n in scored_news:
                hn_link = n.get("hn_url", n["url"])
                lines.append(f"- [{n['title']}]({n['url']}) — ⬆️ {n.get('score', 0)} points, "
                           f"💬 {n.get('comments', 0)} comments "
                           f"([discussion]({hn_link}))")
            lines.append("")

        if scored_projects:
            lines.append("### GitHub Trending")
            lines.append("")
            for p in scored_projects:
                desc = p.get("description", "")
                if len(desc) > 100:
                    desc = desc[:100] + "..."
                lang = f" `{p['language']}`" if p.get("language") else ""
                lines.append(f"- [{p['title']}]({p['url']}){lang} — "
                           f"⭐ {p.get('total_stars', 0):,} total, "
                           f"+{p.get('stars_today', 0)} today — {desc}")
            lines.append("")

        lines.append("---")
        lines.append("")

    # 统计
    lines.append("## 📊 今日统计")
    lines.append("")
    lines.append(f"- 扫描论文数: {total_scanned}")
    lines.append(f"- 入选论文数: {total_selected}")
    lines.append(f"- 平均分: {avg_score:.1f}")
    lines.append(f"- 最高分: {max_score}")
    lines.append(f"- 新闻数: {len(scored_news)}")
    lines.append(f"- 开源项目数: {len(scored_projects)}")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(f"*Generated by Bro 🤖 — {datetime.now().strftime('%Y-%m-%d %H:%M')}*")

    return "\n".join(lines)


def main():
    date_str = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    
    input_path = DATA_DIR / f"{date_str}.json"
    if not input_path.exists():
        print(f"❌ No data found at {input_path}")
        print(f"   Run fetch_papers.py first: python scripts/fetch_papers.py {date_str}")
        sys.exit(1)
    
    print(f"📝 Generating report for {date_str}...")
    
    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    markdown = generate_markdown(data, date_str)
    
    output_path = POSTS_DIR / f"{date_str}.md"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(markdown)
    
    print(f"✅ Report saved to {output_path}")
    
    # 也输出一些统计
    papers_count = len([p for p in data.get("papers", []) if score_paper(p) >= 60])
    print(f"   Papers selected: {papers_count}")
    print(f"   News items: {len(data.get('news', []))}")
    print(f"   Projects: {len(data.get('projects', []))}")


if __name__ == "__main__":
    main()
