#!/usr/bin/env python3
"""
fetch_papers.py — 从 arXiv / Hacker News / GitHub Trending 采集 AI 相关内容
输出结构化 JSON 到 data/raw/YYYY-MM-DD.json
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

# 项目根目录
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# PDF 存储目录（在 workspace 下，不在 blog repo 内）
PAPERS_DIR = ROOT.parent / "papers"


def sanitize_filename(title: str) -> str:
    """将论文标题转为安全文件名"""
    # 替换特殊字符为下划线
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', title)
    # 空格和连续特殊字符替换为单个下划线
    name = re.sub(r'[\s_]+', '_', name)
    # 去除首尾下划线/点
    name = name.strip('_.')
    # 截断过长文件名（保留 200 字符）
    if len(name) > 200:
        name = name[:200].rstrip('_')
    return name


def download_pdf(pdf_url: str, title: str, date_str: str) -> str:
    """下载论文 PDF 到本地，返回本地路径（失败返回空字符串）"""
    if not pdf_url:
        return ""
    
    day_dir = PAPERS_DIR / date_str
    day_dir.mkdir(parents=True, exist_ok=True)
    
    filename = sanitize_filename(title) + ".pdf"
    filepath = day_dir / filename
    
    # 跳过已下载
    if filepath.exists() and filepath.stat().st_size > 1000:
        return str(filepath)
    
    try:
        req = urllib.request.Request(pdf_url, headers={
            "User-Agent": "AIResearchDaily/1.0"
        })
        with urllib.request.urlopen(req, timeout=60) as resp:
            with open(filepath, "wb") as f:
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    f.write(chunk)
        
        size_kb = filepath.stat().st_size / 1024
        print(f"    📥 Downloaded: {filename} ({size_kb:.0f} KB)")
        return str(filepath)
    except Exception as e:
        print(f"    ⚠️ PDF download failed ({filename}): {e}")
        # 清理不完整文件
        if filepath.exists():
            filepath.unlink()
        return ""

# arXiv 分类
ARXIV_CATEGORIES = ["cs.AI", "cs.CL", "cs.LG", "cs.CV", "cs.MA"]

# AI 相关关键词（用于过滤 HN / GitHub）
AI_KEYWORDS = [
    "ai", "artificial intelligence", "machine learning", "deep learning",
    "neural network", "llm", "large language model", "gpt", "transformer",
    "diffusion", "generative", "nlp", "natural language", "computer vision",
    "reinforcement learning", "rl", "agent", "multimodal", "embedding",
    "fine-tune", "finetune", "pretraining", "pre-training", "reasoning",
    "rag", "retrieval", "attention", "moe", "mixture of experts",
    "openai", "anthropic", "deepmind", "meta ai", "gemini", "claude",
    "llama", "mistral", "stable diffusion", "midjourney",
]


def fetch_arxiv(date_str: str, max_per_cat: int = 20, skip_pdf: bool = False) -> list:
    """从 arXiv API 获取最近提交的论文"""
    papers = []
    
    for cat in ARXIV_CATEGORIES:
        print(f"  Fetching arXiv: {cat} (max {max_per_cat})...")
        # arXiv API 查询：最近提交的论文
        query = f"cat:{cat}"
        params = urllib.parse.urlencode({
            "search_query": query,
            "start": 0,
            "max_results": max_per_cat,
            "sortBy": "submittedDate",
            "sortOrder": "descending",
        })
        url = f"https://export.arxiv.org/api/query?{params}"
        
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "AIResearchDaily/1.0"})
            # Retry up to 2 times on failure
            data = None
            for attempt in range(3):
                try:
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        data = resp.read().decode("utf-8")
                    break
                except Exception as retry_err:
                    if attempt < 2:
                        wait = 5 * (attempt + 1)
                        print(f"    ⚠️ Attempt {attempt+1} failed for {cat}: {retry_err}. Retrying in {wait}s...")
                        time.sleep(wait)
                    else:
                        raise
            
            if not data:
                print(f"    ⚠️ No data received for {cat}, skipping")
                continue
            
            # 解析 Atom XML
            ns = {"atom": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}
            root = ET.fromstring(data)
            
            for entry in root.findall("atom:entry", ns):
                title_el = entry.find("atom:title", ns)
                summary_el = entry.find("atom:summary", ns)
                published_el = entry.find("atom:published", ns)
                
                if title_el is None or summary_el is None:
                    continue
                
                title = " ".join(title_el.text.strip().split())
                summary = " ".join(summary_el.text.strip().split())
                published = published_el.text.strip() if published_el is not None else ""
                
                # 获取链接
                link = ""
                pdf_link = ""
                for l in entry.findall("atom:link", ns):
                    href = l.get("href", "")
                    if l.get("title") == "pdf":
                        pdf_link = href
                    elif l.get("type") == "text/html":
                        link = href
                if not link:
                    id_el = entry.find("atom:id", ns)
                    if id_el is not None:
                        link = id_el.text.strip()
                
                # 获取作者
                authors = []
                for author in entry.findall("atom:author", ns):
                    name_el = author.find("atom:name", ns)
                    if name_el is not None:
                        authors.append(name_el.text.strip())
                
                # 获取所有分类
                categories = []
                for category in entry.findall("atom:category", ns):
                    term = category.get("term", "")
                    if term:
                        categories.append(term)
                
                # 提取 arXiv ID
                arxiv_id = ""
                if link:
                    m = re.search(r"(\d{4}\.\d{4,5})(v\d+)?$", link)
                    if m:
                        arxiv_id = m.group(1)
                
                papers.append({
                    "source": "arxiv",
                    "type": "paper",
                    "title": title,
                    "url": link,
                    "pdf_url": pdf_link,
                    "arxiv_id": arxiv_id,
                    "authors": authors,
                    "categories": categories,
                    "primary_category": cat,
                    "summary": summary,
                    "published": published,
                })
            
            # Rate limit: be nice to arXiv
            time.sleep(3)
            
        except Exception as e:
            print(f"    ⚠️ Error fetching {cat}: {e}")
            continue
    
    # 去重（同一篇论文可能出现在多个分类）
    seen = set()
    unique = []
    for p in papers:
        key = p.get("arxiv_id") or p["url"]
        if key not in seen:
            seen.add(key)
            unique.append(p)
    
    print(f"  ✅ arXiv: {len(unique)} unique papers")
    
    # 下载 PDF（可选）
    if skip_pdf:
        print(f"  ⏭️ Skipping PDF downloads (--no-pdf)")
    else:
        print(f"  📥 Downloading PDFs to {PAPERS_DIR / date_str}...")
        downloaded = 0
        for p in unique:
            pdf_url = p.get("pdf_url", "")
            if pdf_url:
                local_path = download_pdf(pdf_url, p["title"], date_str)
                p["local_pdf"] = local_path
                if local_path:
                    downloaded += 1
                time.sleep(1)  # Rate limit PDF downloads
        print(f"  ✅ Downloaded {downloaded}/{len(unique)} PDFs")
    
    return unique


def fetch_hackernews() -> list:
    """从 Hacker News API 获取 AI 相关热帖（score > 100）"""
    print("  Fetching Hacker News...")
    items = []
    
    try:
        # 获取 top stories
        req = urllib.request.Request(
            "https://hacker-news.firebaseio.com/v0/topstories.json",
            headers={"User-Agent": "AIResearchDaily/1.0"}
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            top_ids = json.loads(resp.read().decode("utf-8"))
        
        # 检查前 100 个帖子
        for story_id in top_ids[:100]:
            try:
                req = urllib.request.Request(
                    f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json",
                    headers={"User-Agent": "AIResearchDaily/1.0"}
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    story = json.loads(resp.read().decode("utf-8"))
                
                if not story or story.get("type") != "story":
                    continue
                
                score = story.get("score", 0)
                if score < 100:
                    continue
                
                title = story.get("title", "").lower()
                text = story.get("text", "").lower() if story.get("text") else ""
                url = story.get("url", "")
                
                # 检查是否 AI 相关
                content = f"{title} {text} {url.lower()}"
                is_ai = any(kw in content for kw in AI_KEYWORDS)
                
                if is_ai:
                    items.append({
                        "source": "hackernews",
                        "type": "news",
                        "title": story.get("title", ""),
                        "url": url or f"https://news.ycombinator.com/item?id={story_id}",
                        "hn_url": f"https://news.ycombinator.com/item?id={story_id}",
                        "score": score,
                        "comments": story.get("descendants", 0),
                        "author": story.get("by", ""),
                        "time": story.get("time", 0),
                    })
                
                time.sleep(0.1)  # Rate limit
                
            except Exception as e:
                continue
        
    except Exception as e:
        print(f"    ⚠️ Error fetching HN: {e}")
    
    # 按 score 排序
    items.sort(key=lambda x: x.get("score", 0), reverse=True)
    print(f"  ✅ Hacker News: {len(items)} AI-related posts (score > 100)")
    return items


def fetch_github_trending() -> list:
    """从 GitHub Trending 页面抓取 ML/AI 类项目"""
    print("  Fetching GitHub Trending...")
    projects = []
    
    # 尝试多个语言
    for lang in ["python", ""]:
        try:
            url = "https://github.com/trending"
            if lang:
                url += f"/{lang}"
            url += "?since=daily"
            
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html",
            })
            with urllib.request.urlopen(req, timeout=15) as resp:
                html = resp.read().decode("utf-8")
            
            # 简单解析 trending 页面
            # 查找 repo 链接模式: /user/repo
            repo_pattern = re.compile(
                r'<h2 class="h3[^"]*">\s*<a href="(/[^"]+)"',
                re.DOTALL
            )
            # 也尝试另一种模式
            if not repo_pattern.findall(html):
                repo_pattern = re.compile(
                    r'href="(/[^/]+/[^"]+)"[^>]*class="[^"]*"[^>]*>\s*\n\s*<span[^>]*>[^<]*</span>\s*\n\s*/\s*\n\s*<span[^>]*>([^<]*)</span>',
                    re.DOTALL
                )
            
            # 更通用的方法：用 article 标签提取
            article_pattern = re.compile(
                r'<article class="Box-row[^"]*">(.+?)</article>',
                re.DOTALL
            )
            
            for article_match in article_pattern.finditer(html):
                article = article_match.group(1)
                
                # 提取 repo 路径
                repo_match = re.search(r'href="(/[^/]+/[^"]+)"', article)
                if not repo_match:
                    continue
                repo_path = repo_match.group(1).strip()
                repo_name = repo_path.lstrip("/")
                
                # 提取描述
                desc_match = re.search(r'<p class="[^"]*col-9[^"]*"[^>]*>\s*(.+?)\s*</p>', article, re.DOTALL)
                description = ""
                if desc_match:
                    description = re.sub(r'<[^>]+>', '', desc_match.group(1)).strip()
                
                # 提取 stars
                stars_match = re.search(r'(\d[\d,]*)\s*stars?\s*today', article, re.IGNORECASE)
                stars_today = 0
                if stars_match:
                    stars_today = int(stars_match.group(1).replace(",", ""))
                
                total_stars_match = re.search(r'class="[^"]*Link--muted[^"]*"[^>]*href="[^"]*stargazers[^"]*"[^>]*>\s*(?:<[^>]*>\s*)*\s*([\d,]+)', article, re.DOTALL)
                total_stars = 0
                if total_stars_match:
                    total_stars = int(total_stars_match.group(1).replace(",", ""))
                
                # 提取语言
                lang_match = re.search(r'<span itemprop="programmingLanguage">([^<]+)</span>', article)
                language = lang_match.group(1).strip() if lang_match else ""
                
                # 检查是否 AI/ML 相关
                content = f"{repo_name} {description}".lower()
                is_ai = any(kw in content for kw in AI_KEYWORDS)
                
                if is_ai:
                    projects.append({
                        "source": "github_trending",
                        "type": "project",
                        "title": repo_name,
                        "url": f"https://github.com{repo_path}",
                        "description": description,
                        "language": language,
                        "stars_today": stars_today,
                        "total_stars": total_stars,
                    })
            
            time.sleep(1)
            
        except Exception as e:
            print(f"    ⚠️ Error fetching GitHub trending ({lang or 'all'}): {e}")
            continue
    
    # 去重
    seen = set()
    unique = []
    for p in projects:
        if p["title"] not in seen:
            seen.add(p["title"])
            unique.append(p)
    
    # 按 stars_today 排序
    unique.sort(key=lambda x: x.get("stars_today", 0), reverse=True)
    print(f"  ✅ GitHub Trending: {len(unique)} AI/ML projects")
    return unique


def main():
    parser = argparse.ArgumentParser(description="Fetch AI research papers, news, and projects")
    parser.add_argument("date", nargs="?", default=datetime.now().strftime("%Y-%m-%d"),
                        help="Date string YYYY-MM-DD (default: today)")
    parser.add_argument("--max-per-cat", type=int, default=20,
                        help="Max papers per arXiv category (default: 20)")
    parser.add_argument("--no-pdf", action="store_true",
                        help="Skip downloading PDFs")
    args = parser.parse_args()

    date_str = args.date
    print(f"📡 Fetching AI research data for {date_str}...")
    
    result = {
        "date": date_str,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "papers": [],
        "news": [],
        "projects": [],
    }
    
    # 1. arXiv
    result["papers"] = fetch_arxiv(date_str, max_per_cat=args.max_per_cat, skip_pdf=args.no_pdf)
    
    # 2. Hacker News
    result["news"] = fetch_hackernews()
    
    # 3. GitHub Trending
    result["projects"] = fetch_github_trending()
    
    # 保存
    output_path = DATA_DIR / f"{date_str}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    total = len(result["papers"]) + len(result["news"]) + len(result["projects"])
    print(f"\n✅ Done! Saved {total} items to {output_path}")
    print(f"   - Papers: {len(result['papers'])}")
    print(f"   - News: {len(result['news'])}")
    print(f"   - Projects: {len(result['projects'])}")


if __name__ == "__main__":
    main()
