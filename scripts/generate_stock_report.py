#!/usr/bin/env python3
"""
巴菲特价值投资选股系统 — Hugo 报告生成器
读取筛选结果 JSON，生成 Hugo Markdown 报告。
"""

import json
import os
import sys
import glob
from datetime import datetime
from pathlib import Path

BLOG_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = BLOG_ROOT / "data" / "stocks"
CONTENT_DIR = BLOG_ROOT / "content" / "stock-picks"
CONTENT_DIR.mkdir(parents=True, exist_ok=True)


def format_number(n, decimals=2):
    """格式化数字"""
    if n is None:
        return "N/A"
    if isinstance(n, str):
        return n
    if abs(n) >= 1e12:
        return f"{n / 1e12:.{decimals}f}万亿"
    if abs(n) >= 1e9:
        return f"{n / 1e9:.{decimals}f}B"
    if abs(n) >= 1e6:
        return f"{n / 1e6:.{decimals}f}M"
    return f"{n:,.{decimals}f}"


def format_pct(n, default="N/A"):
    """格式化百分比"""
    if n is None:
        return default
    return f"{n:.1%}"


def find_historical_mentions(ticker, current_file):
    """检查历史报告中是否出现过该 ticker"""
    mentions = []
    for json_file in sorted(DATA_DIR.glob("*.json")):
        if json_file.name == current_file:
            continue
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            for stock in data.get("all_scored", []):
                if stock.get("ticker") == ticker:
                    mentions.append({
                        "week": f"{data.get('year')}-W{data.get('week', 0):02d}",
                        "score": stock.get("scores", {}).get("total", 0),
                        "price": stock.get("current_price", 0),
                        "safety_margin": stock.get("safety_margin"),
                    })
        except Exception:
            continue
    return mentions


def get_moat_analysis(stock):
    """生成护城河分析文字"""
    sector = stock.get("sector", "")
    industry = stock.get("industry", "")
    avg_roe = stock.get("avg_roe", 0)
    scores = stock.get("scores", {})

    analysis = []

    moat_score = scores.get("moat", 0)
    if moat_score >= 16:
        analysis.append("**护城河评级：宽阔（Wide）**")
        analysis.append("公司具有显著的竞争优势，表现为稳定的高毛利率和持续的高 ROE。")
    elif moat_score >= 12:
        analysis.append("**护城河评级：较宽（Moderate-Wide）**")
        analysis.append("公司具有一定的竞争优势，毛利率和 ROE 表现良好。")
    elif moat_score >= 8:
        analysis.append("**护城河评级：中等（Moderate）**")
        analysis.append("公司有一些竞争壁垒，但可能面临行业竞争压力。")
    else:
        analysis.append("**护城河评级：较窄（Narrow）**")
        analysis.append("公司的竞争优势较弱，需关注行业竞争态势。")

    if avg_roe > 0.20:
        analysis.append(f"- 资本回报率出色（5年平均 ROE: {avg_roe:.1%}），说明公司能持续为股东创造高回报")
    elif avg_roe > 0.15:
        analysis.append(f"- 资本回报率良好（5年平均 ROE: {avg_roe:.1%}），符合巴菲特的最低要求")

    analysis.append(f"- 所属行业: {sector} / {industry}")

    return "\n".join(analysis)


def get_risk_analysis(stock):
    """生成风险分析"""
    risks = []
    debt_ebitda = stock.get("debt_ebitda")
    safety_margin = stock.get("safety_margin")
    pe = stock.get("pe_ratio")

    if debt_ebitda and debt_ebitda > 2:
        risks.append(f"⚠️ 债务水平偏高（债务/EBITDA: {debt_ebitda:.1f}），经济下行时可能面临压力")
    if safety_margin and safety_margin < 0.30:
        risks.append(f"⚠️ 安全边际偏低（{safety_margin:.1%}），估值下行空间有限")
    if pe and pe > 25:
        risks.append(f"⚠️ 市盈率较高（P/E: {pe:.1f}），市场对未来增长预期可能过高")

    # 通用风险
    risks.append("- 宏观经济衰退可能影响公司业绩")
    risks.append("- 行业竞争格局变化可能侵蚀利润率")
    risks.append("- 本估值模型依赖历史数据和假设参数，实际情况可能偏离")

    return "\n".join(risks)


def get_catalyst_analysis(stock):
    """生成潜在催化剂分析"""
    catalysts = []
    safety_margin = stock.get("safety_margin")
    dividend_yield = stock.get("dividend_yield", 0)

    if safety_margin and safety_margin > 0.30:
        catalysts.append("- 📈 当前估值具有较大安全边际，市场情绪修复可带来估值回归")
    if dividend_yield and dividend_yield > 0.02:
        catalysts.append(f"- 💰 股息收益率 {dividend_yield:.1%}，提供稳定现金回报")

    catalysts.append("- 📊 持续的盈利增长和自由现金流改善")
    catalysts.append("- 🔄 股票回购计划减少流通股，提升每股收益")
    catalysts.append("- 🌍 市场份额扩张或新业务增长点")

    return "\n".join(catalysts)


def generate_report(json_path):
    """生成 Hugo Markdown 报告"""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    year = data.get("year", 2024)
    week = data.get("week", 1)
    generated_at = data.get("generated_at", "")
    top_picks = data.get("top_picks", [])
    all_scored = data.get("all_scored", [])
    params = data.get("parameters", {})
    json_filename = Path(json_path).name

    if not top_picks:
        print("⚠️ 没有找到 Top Picks 数据")
        return

    # 生成日期
    try:
        gen_date = datetime.fromisoformat(generated_at)
        date_str = gen_date.strftime("%Y-%m-%d")
        date_display = gen_date.strftime("%Y年%m月%d日")
    except Exception:
        date_str = datetime.now().strftime("%Y-%m-%d")
        date_display = datetime.now().strftime("%Y年%m月%d日")

    # Hugo front matter
    report_filename = f"{year}-W{week:02d}.md"
    report_path = CONTENT_DIR / report_filename

    # 构建 Top 5 摘要
    top5_summary = []
    for i, s in enumerate(top_picks[:5], 1):
        score = s.get("scores", {}).get("total", 0)
        top5_summary.append(f"{i}. **{s['ticker']}** ({s.get('name', '')}) — {score:.0f}分")

    lines = []
    lines.append("---")
    lines.append(f'title: "📊 巴菲特选股周报 {year} 第{week}周"')
    lines.append(f"date: {date_str}")
    lines.append(f'summary: "基于巴菲特价值投资8维度评分框架的每周选股报告 | Top 5: {", ".join(s["ticker"] for s in top_picks[:5])}"')
    lines.append("tags:")
    lines.append("  - 价值投资")
    lines.append("  - 巴菲特")
    lines.append("  - 选股")
    lines.append("  - DCF估值")
    lines.append("categories:")
    lines.append("  - Stock Picks")
    lines.append("ShowToc: true")
    lines.append("TocOpen: true")
    lines.append("---")
    lines.append("")
    lines.append(f"> 📅 生成日期: {date_display}")
    lines.append(f"> 📈 初始筛选池: {data.get('total_screened', 0)} 只股票")
    lines.append(f"> ✅ 第一轮通过: {data.get('passed_first_round', 0)} 只")
    lines.append(f"> ✅ 第二轮通过: {data.get('passed_second_round', 0)} 只")
    lines.append("")
    lines.append("## 📋 本周 Top 5")
    lines.append("")
    for item in top5_summary:
        lines.append(item)
    lines.append("")

    # 评分概览表
    lines.append("## 🏆 评分总览")
    lines.append("")
    lines.append("| 排名 | 股票 | 市场 | 总分 | 护城河 | 安全边际 | 盈利 | ROE | 管理层 | 财务 | 可理解 | 情绪 |")
    lines.append("|:----:|:-----|:----:|:----:|:------:|:--------:|:----:|:---:|:------:|:----:|:------:|:----:|")
    for i, s in enumerate(all_scored[:20], 1):
        sc = s.get("scores", {})
        lines.append(
            f"| {i} | **{s['ticker']}** | {s['market']} | **{sc.get('total', 0):.0f}** | "
            f"{sc.get('moat', 0):.0f}/20 | {sc.get('safety_margin', 0):.0f}/20 | "
            f"{sc.get('earnings_quality', 0):.0f}/15 | {sc.get('roe_capital', 0):.0f}/15 | "
            f"{sc.get('management', 0):.0f}/10 | {sc.get('financial_health', 0):.0f}/10 | "
            f"{sc.get('understandability', 0):.0f}/5 | {sc.get('sentiment_discount', 0):.0f}/5 |"
        )
    lines.append("")

    # 每只 Top 5 股票的详细分析
    lines.append("---")
    lines.append("")
    lines.append("## 📈 详细分析")
    lines.append("")

    for i, stock in enumerate(top_picks[:5], 1):
        ticker = stock["ticker"]
        name = stock.get("name", "")
        scores = stock.get("scores", {})
        total = scores.get("total", 0)

        lines.append(f"### #{i} {ticker} — {name}")
        lines.append("")

        # 检查历史出现
        history = find_historical_mentions(ticker, json_filename)
        if history:
            lines.append(f"🔄 **历史追踪：** 该股票在之前的报告中也被选入：")
            for h in history:
                sm_str = format_pct(h.get("safety_margin"))
                lines.append(f"- {h['week']}: 评分 {h['score']:.0f}分, 价格 {h['price']}, 安全边际 {sm_str}")
            lines.append("")

        # 关键数据表
        lines.append("#### 📊 关键数据")
        lines.append("")
        lines.append("| 指标 | 数值 |")
        lines.append("|:-----|:-----|")
        lines.append(f"| 市场/行业 | {stock['market']} / {stock.get('sector', '')} |")
        lines.append(f"| 当前价格 | {stock.get('current_price', 'N/A')} {stock.get('currency', '')} |")
        lines.append(f"| 市值 | {format_number(stock.get('market_cap', 0))} |")
        lines.append(f"| P/E | {format_number(stock.get('pe_ratio'), 1) if stock.get('pe_ratio') else 'N/A'} |")
        lines.append(f"| P/B | {format_number(stock.get('pb_ratio'), 2) if stock.get('pb_ratio') else 'N/A'} |")
        lines.append(f"| 5年平均 ROE | {format_pct(stock.get('avg_roe'))} |")
        lines.append(f"| 债务/EBITDA | {format_number(stock.get('debt_ebitda'), 1) if stock.get('debt_ebitda') else 'N/A'} |")
        lines.append(f"| 内部人持股 | {format_pct(stock.get('insider_pct'))} |")
        lines.append(f"| 股息率 | {format_pct(stock.get('dividend_yield'))} |")
        lines.append(f"| 52周范围 | {stock.get('fifty_two_week_low', 'N/A')} - {stock.get('fifty_two_week_high', 'N/A')} |")
        lines.append(f"| **综合评分** | **{total:.0f}/100** |")
        lines.append("")

        # DCF 估值详情
        dcf = stock.get("dcf_details")
        if dcf:
            lines.append("#### 💰 DCF 估值")
            lines.append("")
            lines.append("| 参数 | 值 |")
            lines.append("|:-----|:---|")
            lines.append(f"| 最近一年 FCF | {format_number(dcf.get('latest_fcf', 0))} |")
            lines.append(f"| 预期增长率 | {format_pct(dcf.get('growth_rate'))} |")
            lines.append(f"| WACC | {format_pct(dcf.get('wacc'))} |")
            lines.append(f"| 终端增长率 | {format_pct(dcf.get('terminal_growth'))} |")
            lines.append(f"| Beta | {dcf.get('beta', 'N/A')} |")
            lines.append(f"| **内在价值/股** | **{format_number(dcf.get('intrinsic_per_share', 0))} {stock.get('currency', '')}** |")
            sm = stock.get("safety_margin")
            lines.append(f"| **安全边际** | **{format_pct(sm)}** |")
            lines.append("")

        # 投资论点
        lines.append("#### 🎯 投资论点")
        lines.append("")
        lines.append(get_moat_analysis(stock))
        lines.append("")

        # Owner Earnings
        oe = stock.get("owner_earnings")
        if oe:
            lines.append(f"- Owner Earnings: {format_number(oe)}（巴菲特偏好的自由现金流指标）")
            lines.append("")

        # 风险
        lines.append("#### ⚠️ 风险因素")
        lines.append("")
        lines.append(get_risk_analysis(stock))
        lines.append("")

        # 催化剂
        lines.append("#### 🚀 潜在催化剂")
        lines.append("")
        lines.append(get_catalyst_analysis(stock))
        lines.append("")
        lines.append("---")
        lines.append("")

    # 方法论说明
    lines.append("## 📚 方法论")
    lines.append("")
    lines.append("本报告采用巴菲特价值投资8维度评分框架：")
    lines.append("")
    lines.append("1. **护城河** (20分): 毛利率稳定性、ROE持续性")
    lines.append("2. **安全边际** (20分): DCF估值、Owner Earnings")
    lines.append("3. **盈利质量** (15分): 连续盈利、增长稳定性、现金流匹配")
    lines.append("4. **ROE与资本效率** (15分): ROE水平、ROIC vs WACC")
    lines.append("5. **管理层质量** (10分): 内部人持股、回购、股息")
    lines.append("6. **财务健康度** (10分): 杠杆率、流动性、利息覆盖")
    lines.append("7. **可理解性** (5分): 商业模式复杂度")
    lines.append("8. **市场情绪折价** (5分): 52周价格位置")
    lines.append("")
    lines.append("**筛选流程：**")
    lines.append(f"- 初始池: {data.get('total_screened', 0)} 只股票（覆盖美国、香港、A股、日本、欧洲）")
    lines.append(f"- 第一轮量化初筛 → {data.get('passed_first_round', 0)} 只")
    lines.append(f"- 第二轮深度分析（DCF + 护城河）→ {data.get('passed_second_round', 0)} 只")
    lines.append("- 第三轮综合排名 → Top 5")
    lines.append("")
    lines.append("**DCF 假设参数：**")
    lines.append(f"- 无风险利率: {format_pct(params.get('risk_free_rate', 0.045))}")
    lines.append(f"- 市场风险溢价: {format_pct(params.get('market_risk_premium', 0.055))}")
    lines.append(f"- 终端增长率: {format_pct(params.get('terminal_growth_rate', 0.03))}")
    lines.append(f"- 增长率上限: {format_pct(params.get('max_growth_rate', 0.15))}")
    lines.append(f"- DCF 预测年数: {params.get('dcf_years', 10)} 年")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("*⚠️ 免责声明：本报告由自动化系统生成，基于公开数据和量化模型，仅供研究参考，不构成投资建议。投资有风险，入市需谨慎。过往表现不预示未来结果。*")

    # 写入文件
    content = "\n".join(lines)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"✅ 报告已生成: {report_path}")
    print(f"   Top 5: {', '.join(s['ticker'] for s in top_picks[:5])}")
    return report_path


def main():
    """主入口：找到最新的 JSON 文件并生成报告"""
    # 支持命令行指定文件
    if len(sys.argv) > 1:
        json_path = sys.argv[1]
    else:
        # 找最新的 JSON 文件
        json_files = sorted(DATA_DIR.glob("*.json"), reverse=True)
        if not json_files:
            print("❌ 没有找到筛选结果 JSON 文件")
            print(f"   请先运行 stock_screener.py")
            sys.exit(1)
        json_path = str(json_files[0])

    print(f"📄 读取数据文件: {json_path}")
    generate_report(json_path)


if __name__ == "__main__":
    main()
