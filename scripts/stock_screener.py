#!/usr/bin/env python3
"""
巴菲特价值投资选股系统 — 自动化筛选脚本
使用 yfinance 获取公开财务数据，按8维度评分框架筛选股票。
"""

import json
import os
import sys
import time
import logging
import math
from datetime import datetime, date
from pathlib import Path
from collections import defaultdict

import yfinance as yf
import numpy as np

# ────────────────────────── 配置 ──────────────────────────

BLOG_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = BLOG_ROOT / "data" / "stocks"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# 速率控制
REQUEST_DELAY = 0.8  # 每个 ticker 之间的延迟（秒）
BATCH_DELAY = 5.0    # 每批次之间的延迟

# DCF 参数
RISK_FREE_RATE = 0.045     # 无风险利率
MARKET_RISK_PREMIUM = 0.055  # 市场风险溢价
TERMINAL_GROWTH_RATE = 0.03  # 终端增长率
DEFAULT_TAX_RATE = 0.21      # 默认税率
MAX_GROWTH_RATE = 0.15       # 增长率上限
DCF_YEARS = 10               # DCF 预测年数

# 筛选阈值
MIN_MARKET_CAP = 1e9   # 10亿美元
MAX_PB = 3.0
MIN_ROE_5Y = 0.15
MIN_PROFIT_YEARS = 5
MAX_DEBT_EBITDA = 4.0
MIN_SAFETY_MARGIN = 0.20  # 第二轮要求安全边际 ≥ 20%

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("stock_screener")

# ────────────────────────── 指数成分股 ──────────────────────────

# S&P 500 代表性样本 + NASDAQ 100 + 其他市场
# 实际使用中，我们获取尽可能多的 ticker

def get_sp500_tickers():
    """获取 S&P 500 成分股列表"""
    try:
        import urllib.request
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        html = urllib.request.urlopen(req, timeout=15).read().decode()
        tickers = []
        # 简单解析表格中的 ticker
        import re
        # Find table rows with ticker symbols
        rows = re.findall(r'<td[^>]*><a[^>]*>([A-Z.]+)</a>', html)
        tickers = [t.replace(".", "-") for t in rows if len(t) <= 5]
        if len(tickers) > 100:
            log.info(f"从 Wikipedia 获取到 {len(tickers)} 个 S&P 500 成分股")
            return tickers
    except Exception as e:
        log.warning(f"无法从 Wikipedia 获取 S&P 500 列表: {e}")

    # 备用列表 - S&P 500 核心股票
    return [
        "AAPL", "MSFT", "AMZN", "GOOGL", "META", "BRK-B", "JNJ", "V", "MA",
        "PG", "UNH", "HD", "JPM", "NVDA", "DIS", "PYPL", "ADBE", "NFLX",
        "CMCSA", "PFE", "TMO", "ABT", "CSCO", "AVGO", "ACN", "PEP", "KO",
        "COST", "WMT", "MRK", "CVX", "XOM", "ABBV", "LLY", "MCD", "NKE",
        "DHR", "NEE", "BMY", "LIN", "LOW", "UNP", "TXN", "MDT", "PM",
        "AMGN", "HON", "IBM", "QCOM", "CAT", "GS", "BLK", "AXP", "ISRG",
        "DE", "MMM", "GE", "BA", "RTX", "LMT", "SYK", "GILD", "MDLZ",
        "ADP", "SPGI", "CB", "TJX", "BKNG", "SCHW", "CI", "MO", "SO",
        "DUK", "MMC", "PGR", "SHW", "CL", "APD", "ITW", "EMR", "ECL",
        "HUM", "AON", "REGN", "FIS", "FISV", "NSC", "USB", "TGT", "WM",
        "ORLY", "AZO", "SRE", "PNC", "TROW", "AFL", "PAYX", "MSI", "FAST",
        "ROP", "CTAS", "VRSK", "CPRT", "KLAC", "MCHP", "IDXX", "ODFL",
        "BRO", "WST", "TDY", "POOL", "JBHT", "STE", "TTC", "RMD", "HEI",
        "GWW", "CLX", "CHD", "HRL", "K", "SJM", "CAG", "GIS", "MKC",
        "TSN", "HSY", "MNST", "STZ", "BF-B", "TAP", "SAM",
        # 更多金融
        "BAC", "C", "WFC", "MS", "TFC", "KEY", "RF", "CFG", "HBAN", "MTB",
        # 更多工业
        "UPS", "FDX", "CSX", "WM", "RSG", "PCAR", "CMI", "PH", "DOV",
        # 更多消费
        "SBUX", "YUM", "DPZ", "CMG", "ROST", "DG", "DLTR", "BBY",
        # 更多科技
        "CRM", "NOW", "SNOW", "PANW", "CRWD", "ZS", "DDOG", "NET",
        "INTC", "AMD", "MU", "LRCX", "AMAT", "SNPS", "CDNS",
        # 更多医疗
        "ELV", "HCA", "MCK", "CAH", "ZTS", "VRTX", "BIIB", "MRNA",
    ]


def get_hk_tickers():
    """恒生指数核心成分股"""
    return [
        "0005.HK", "0011.HK", "0388.HK", "0700.HK", "9988.HK",
        "1299.HK", "0941.HK", "0883.HK", "0016.HK", "0002.HK",
        "0003.HK", "0006.HK", "0012.HK", "0017.HK", "0027.HK",
        "0066.HK", "0101.HK", "0175.HK", "0241.HK", "0267.HK",
        "0288.HK", "0386.HK", "0669.HK", "0688.HK", "0823.HK",
        "0857.HK", "0939.HK", "0960.HK", "0968.HK", "1038.HK",
        "1044.HK", "1093.HK", "1109.HK", "1113.HK", "1177.HK",
        "1211.HK", "1398.HK", "1810.HK", "1876.HK", "1928.HK",
        "1997.HK", "2007.HK", "2018.HK", "2020.HK", "2269.HK",
        "2313.HK", "2318.HK", "2319.HK", "2331.HK", "2382.HK",
        "2388.HK", "2628.HK", "3328.HK", "3690.HK", "3968.HK",
        "6098.HK", "6862.HK", "9618.HK", "9626.HK", "9698.HK",
        "9888.HK", "9961.HK", "9999.HK",
    ]


def get_a_share_tickers():
    """沪深300核心成分股（部分代表）"""
    return [
        # 上交所 (.SS)
        "600519.SS", "601318.SS", "600036.SS", "600276.SS", "601166.SS",
        "600900.SS", "600030.SS", "600887.SS", "601888.SS", "603259.SS",
        "600809.SS", "601012.SS", "600309.SS", "601899.SS", "600585.SS",
        "600031.SS", "601398.SS", "601939.SS", "600000.SS", "600104.SS",
        "601668.SS", "600690.SS", "600050.SS", "601288.SS", "600048.SS",
        "601628.SS", "600406.SS", "601601.SS", "600196.SS", "600436.SS",
        # 深交所 (.SZ)
        "000858.SZ", "000333.SZ", "000651.SZ", "002714.SZ", "000568.SZ",
        "002415.SZ", "000661.SZ", "002352.SZ", "000002.SZ", "002304.SZ",
        "000725.SZ", "002594.SZ", "300750.SZ", "002475.SZ", "000063.SZ",
        "002027.SZ", "000776.SZ", "002032.SZ", "300015.SZ", "300059.SZ",
    ]


def get_japan_tickers():
    """日本市场核心成分股"""
    return [
        "7203.T", "6758.T", "6861.T", "8306.T", "9984.T",
        "6501.T", "7267.T", "4502.T", "8035.T", "9433.T",
        "6902.T", "7741.T", "4063.T", "6954.T", "4568.T",
        "6367.T", "7974.T", "8058.T", "9432.T", "4519.T",
        "6594.T", "3382.T", "2914.T", "8001.T", "9020.T",
    ]


def get_europe_tickers():
    """欧洲市场核心成分股"""
    return [
        # LSE
        "SHEL.L", "AZN.L", "ULVR.L", "HSBA.L", "DGE.L",
        "RIO.L", "BP.L", "GSK.L", "BATS.L", "LSEG.L",
        # Euronext (Paris)
        "MC.PA", "OR.PA", "SAN.PA", "AI.PA", "SU.PA",
        "BN.PA", "AIR.PA", "DG.PA", "CS.PA", "RI.PA",
        # Frankfurt
        "SAP.DE", "SIE.DE", "ALV.DE", "DTE.DE", "BAS.DE",
        "BAYN.DE", "MBG.DE", "BMW.DE", "ADS.DE", "MUV2.DE",
    ]


def get_all_tickers():
    """获取所有市场的 ticker"""
    fast_mode = os.environ.get("STOCK_FAST_MODE", "").lower() in ("1", "true", "yes")

    all_tickers = []
    all_tickers.extend(get_sp500_tickers())
    if not fast_mode:
        all_tickers.extend(get_hk_tickers())
        all_tickers.extend(get_a_share_tickers())
        all_tickers.extend(get_japan_tickers())
        all_tickers.extend(get_europe_tickers())
    # 去重
    seen = set()
    unique = []
    for t in all_tickers:
        if t not in seen:
            seen.add(t)
            unique.append(t)

    if fast_mode and len(unique) > 80:
        log.info(f"快速模式: 从 {len(unique)} 个缩减到前 80 个核心 ticker")
        unique = unique[:80]

    log.info(f"总计 {len(unique)} 个 ticker 待筛选")
    return unique


# ────────────────────────── 数据获取工具 ──────────────────────────

def safe_get(d, *keys, default=None):
    """安全地从嵌套字典获取值"""
    current = d
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key, default)
        else:
            return default
        if current is None:
            return default
    return current


def get_financials(ticker_obj):
    """获取并整理财务数据"""
    data = {}
    info = {}
    try:
        info = ticker_obj.info or {}
    except Exception:
        pass

    data["info"] = info
    data["market_cap"] = info.get("marketCap", 0) or 0
    data["currency"] = info.get("currency", "USD")
    data["sector"] = info.get("sector", "Unknown")
    data["industry"] = info.get("industry", "Unknown")
    data["name"] = info.get("shortName") or info.get("longName", "")
    data["pe_ratio"] = info.get("trailingPE") or info.get("forwardPE")
    data["pb_ratio"] = info.get("priceToBook")
    data["beta"] = info.get("beta", 1.0)
    data["current_price"] = info.get("currentPrice") or info.get("regularMarketPrice", 0)
    data["fifty_two_week_high"] = info.get("fiftyTwoWeekHigh", 0)
    data["fifty_two_week_low"] = info.get("fiftyTwoWeekLow", 0)
    data["insider_pct"] = info.get("heldPercentInsiders", 0) or 0
    data["dividend_yield"] = info.get("dividendYield", 0) or 0
    data["payout_ratio"] = info.get("payoutRatio", 0) or 0

    # 获取年度财务报表
    try:
        income = ticker_obj.financials
        if income is not None and not income.empty:
            data["income_stmt"] = income
        else:
            data["income_stmt"] = None
    except Exception:
        data["income_stmt"] = None

    try:
        balance = ticker_obj.balance_sheet
        if balance is not None and not balance.empty:
            data["balance_sheet"] = balance
        else:
            data["balance_sheet"] = None
    except Exception:
        data["balance_sheet"] = None

    try:
        cashflow = ticker_obj.cashflow
        if cashflow is not None and not cashflow.empty:
            data["cashflow"] = cashflow
        else:
            data["cashflow"] = None
    except Exception:
        data["cashflow"] = None

    return data


def extract_annual_values(df, row_names, years=5):
    """从财务报表中提取年度数据"""
    if df is None or df.empty:
        return []
    values = []
    for name in row_names:
        if name in df.index:
            row = df.loc[name]
            vals = [v for v in row.values[:years] if v is not None and not (isinstance(v, float) and math.isnan(v))]
            if vals:
                return [float(v) for v in vals]
    return values


# ────────────────────────── 第一轮：量化初筛 ──────────────────────────

def first_round_screen(ticker_str):
    """
    第一轮量化初筛。
    返回 (pass, data_dict) — pass 为 True 则通过初筛。
    """
    try:
        ticker = yf.Ticker(ticker_str)
        data = get_financials(ticker)

        # 市值检查
        market_cap = data["market_cap"]
        if not market_cap or market_cap < MIN_MARKET_CAP:
            return False, {"reason": f"市值不足 ({market_cap:.0f})" if market_cap else "无市值数据"}

        # P/B 检查
        pb = data.get("pb_ratio")
        if pb is not None and pb > MAX_PB:
            return False, {"reason": f"P/B ({pb:.1f}) > {MAX_PB}"}

        # P/E 检查 — 排除亏损
        pe = data.get("pe_ratio")
        if pe is not None and pe < 0:
            return False, {"reason": f"P/E 为负 ({pe:.1f})"}

        # ROE 检查
        income = data.get("income_stmt")
        balance = data.get("balance_sheet")
        roe_values = []
        if income is not None and balance is not None:
            net_incomes = extract_annual_values(income, ["Net Income", "Net Income Common Stockholders"])
            equities = extract_annual_values(balance, ["Stockholders Equity", "Total Stockholder Equity",
                                                        "Stockholders' Equity", "Common Stock Equity"])
            if net_incomes and equities:
                for ni, eq in zip(net_incomes, equities):
                    if eq and eq > 0:
                        roe_values.append(ni / eq)

        avg_roe = np.mean(roe_values) if roe_values else 0
        if avg_roe < MIN_ROE_5Y and len(roe_values) >= 2:
            return False, {"reason": f"平均ROE ({avg_roe:.1%}) < {MIN_ROE_5Y:.0%}"}

        # 连续盈利检查
        net_incomes = extract_annual_values(
            income, ["Net Income", "Net Income Common Stockholders"]
        ) if income is not None else []
        profitable_years = sum(1 for ni in net_incomes if ni > 0)
        if len(net_incomes) >= MIN_PROFIT_YEARS and profitable_years < MIN_PROFIT_YEARS:
            return False, {"reason": f"连续盈利不足 ({profitable_years}年)"}

        # 债务/EBITDA 检查
        ebitda_vals = extract_annual_values(income, ["EBITDA", "Normalized EBITDA"]) if income is not None else []
        total_debt = 0
        if balance is not None:
            debt_vals = extract_annual_values(balance, ["Total Debt", "Long Term Debt", "Long Term Debt And Capital Lease Obligation"])
            if debt_vals:
                total_debt = debt_vals[0]
        latest_ebitda = ebitda_vals[0] if ebitda_vals else 0
        if latest_ebitda > 0 and total_debt > 0:
            debt_ebitda = total_debt / latest_ebitda
            if debt_ebitda > MAX_DEBT_EBITDA:
                return False, {"reason": f"债务/EBITDA ({debt_ebitda:.1f}) > {MAX_DEBT_EBITDA}"}
        else:
            debt_ebitda = None

        # 通过初筛
        data["roe_values"] = roe_values
        data["avg_roe"] = avg_roe
        data["net_incomes"] = net_incomes
        data["profitable_years"] = profitable_years
        data["ebitda_vals"] = ebitda_vals
        data["total_debt"] = total_debt
        data["debt_ebitda"] = debt_ebitda
        data["ticker_obj"] = ticker
        return True, data

    except Exception as e:
        return False, {"reason": f"数据获取失败: {str(e)[:100]}"}


# ────────────────────────── 第二轮：深度分析 ──────────────────────────

def calculate_dcf(data):
    """计算 DCF 内在价值"""
    cashflow = data.get("cashflow")
    if cashflow is None:
        return None, None

    # 获取自由现金流
    op_cf = extract_annual_values(cashflow, ["Operating Cash Flow", "Total Cash From Operating Activities"])
    capex = extract_annual_values(cashflow, ["Capital Expenditure", "Capital Expenditures"])

    if not op_cf:
        return None, None

    # 计算历史 FCF
    fcf_history = []
    for i in range(min(len(op_cf), len(capex) if capex else len(op_cf))):
        if capex and i < len(capex):
            fcf = op_cf[i] + capex[i]  # capex 通常为负数
        else:
            fcf = op_cf[i] * 0.8  # 粗略估计
        fcf_history.append(fcf)

    if not fcf_history or fcf_history[0] <= 0:
        return None, None

    latest_fcf = fcf_history[0]

    # 估算增长率
    if len(fcf_history) >= 3 and fcf_history[-1] > 0:
        years = len(fcf_history) - 1
        growth_rate = (fcf_history[0] / fcf_history[-1]) ** (1 / years) - 1
        growth_rate = max(-0.05, min(growth_rate, MAX_GROWTH_RATE))
    else:
        growth_rate = 0.05  # 默认 5%

    # WACC 计算
    beta = data.get("beta", 1.0) or 1.0
    beta = max(0.5, min(beta, 2.5))  # 限制 beta 范围
    cost_of_equity = RISK_FREE_RATE + beta * MARKET_RISK_PREMIUM

    # 简化 WACC（假设以股权为主）
    market_cap = data.get("market_cap", 0)
    total_debt = data.get("total_debt", 0)
    total_value = market_cap + total_debt if market_cap else 1
    equity_weight = market_cap / total_value if total_value > 0 else 0.8
    debt_weight = 1 - equity_weight
    cost_of_debt = 0.05  # 假设债务成本 5%
    wacc = equity_weight * cost_of_equity + debt_weight * cost_of_debt * (1 - DEFAULT_TAX_RATE)
    wacc = max(0.06, min(wacc, 0.20))  # 限制 WACC 范围

    # DCF 计算
    dcf_value = 0
    projected_fcf = latest_fcf
    for year in range(1, DCF_YEARS + 1):
        if year <= 5:
            g = growth_rate
        else:
            # 从第6年开始衰减到终端增长率
            fade = (year - 5) / 5
            g = growth_rate * (1 - fade) + TERMINAL_GROWTH_RATE * fade
        projected_fcf *= (1 + g)
        dcf_value += projected_fcf / (1 + wacc) ** year

    # 终值
    terminal_value = projected_fcf * (1 + TERMINAL_GROWTH_RATE) / (wacc - TERMINAL_GROWTH_RATE)
    terminal_value_pv = terminal_value / (1 + wacc) ** DCF_YEARS
    dcf_value += terminal_value_pv

    # 计算每股内在价值
    shares = data.get("info", {}).get("sharesOutstanding", 0)
    if not shares or shares <= 0:
        return None, None

    intrinsic_per_share = dcf_value / shares

    dcf_details = {
        "latest_fcf": latest_fcf,
        "growth_rate": growth_rate,
        "wacc": wacc,
        "terminal_growth": TERMINAL_GROWTH_RATE,
        "dcf_total": dcf_value,
        "terminal_value_pv": terminal_value_pv,
        "intrinsic_per_share": intrinsic_per_share,
        "shares": shares,
        "beta": beta,
    }

    return intrinsic_per_share, dcf_details


def calculate_owner_earnings(data):
    """计算 Owner Earnings（巴菲特偏好指标）"""
    income = data.get("income_stmt")
    cashflow = data.get("cashflow")
    if income is None or cashflow is None:
        return None

    net_income = extract_annual_values(income, ["Net Income", "Net Income Common Stockholders"])
    depreciation = extract_annual_values(cashflow, ["Depreciation And Amortization", "Depreciation"])
    capex = extract_annual_values(cashflow, ["Capital Expenditure", "Capital Expenditures"])

    if not net_income:
        return None

    ni = net_income[0]
    dep = depreciation[0] if depreciation else 0
    cx = abs(capex[0]) if capex else 0
    maintenance_capex = cx * 0.7  # 估算维护性资本支出

    owner_earnings = ni + dep - maintenance_capex
    return owner_earnings


def second_round_analysis(ticker_str, data):
    """第二轮深度分析"""
    result = {"ticker": ticker_str, "passed": False}

    # DCF 估值
    intrinsic_value, dcf_details = calculate_dcf(data)
    current_price = data.get("current_price", 0)

    if intrinsic_value and intrinsic_value > 0 and current_price and current_price > 0:
        safety_margin = (intrinsic_value - current_price) / intrinsic_value
        result["intrinsic_value"] = round(intrinsic_value, 2)
        result["current_price"] = round(current_price, 2)
        result["safety_margin"] = round(safety_margin, 4)
        result["dcf_details"] = dcf_details

        if safety_margin < MIN_SAFETY_MARGIN:
            result["reason"] = f"安全边际不足 ({safety_margin:.1%} < {MIN_SAFETY_MARGIN:.0%})"
            # 仍然保留数据，但标记为未通过（如果安全边际接近就放宽标准）
            if safety_margin < 0.10:
                return result
    else:
        result["intrinsic_value"] = None
        result["safety_margin"] = None
        result["dcf_details"] = None
        # DCF 计算失败不一定排除，给予中性分

    # Owner Earnings
    owner_earnings = calculate_owner_earnings(data)
    result["owner_earnings"] = owner_earnings

    result["passed"] = True
    return result


# ────────────────────────── 第三轮：综合评分 ──────────────────────────

def get_industry_category(sector):
    """将行业分类映射到可理解性评分"""
    simple_industries = {
        "Consumer Defensive": 5, "Consumer Cyclical": 4,
        "Industrials": 4, "Basic Materials": 4,
        "Financial Services": 4, "Utilities": 5,
        "Real Estate": 4, "Energy": 4,
        "Communication Services": 3, "Healthcare": 3,
        "Technology": 3,
    }
    return simple_industries.get(sector, 3)


def score_moat(data):
    """维度1: 护城河评分 (满分20)"""
    score = 0

    # 毛利率稳定性
    income = data.get("income_stmt")
    if income is not None:
        revenues = extract_annual_values(income, ["Total Revenue", "Revenue"])
        cogs = extract_annual_values(income, ["Cost Of Revenue", "Cost Of Goods Sold"])
        if revenues and cogs and len(revenues) >= 3:
            gross_margins = []
            for rev, cost in zip(revenues, cogs):
                if rev and rev > 0:
                    gross_margins.append((rev - cost) / rev)
            if gross_margins:
                avg_gm = np.mean(gross_margins)
                std_gm = np.std(gross_margins) * 100  # 转为百分点
                # 毛利率稳定性
                if std_gm < 3: score += 8
                elif std_gm < 5: score += 6
                elif std_gm < 8: score += 4
                else: score += 2
                # 毛利率水平
                if avg_gm > 0.60: score += 6
                elif avg_gm > 0.40: score += 5
                elif avg_gm > 0.30: score += 4
                elif avg_gm > 0.20: score += 3
                else: score += 1
            else:
                score += 4  # 无数据给基础分
        else:
            score += 4
    else:
        score += 4

    # ROE 持续性
    roe_values = data.get("roe_values", [])
    years_above_15 = sum(1 for r in roe_values if r > 0.15)
    if years_above_15 >= 5: score += 6
    elif years_above_15 >= 4: score += 5
    elif years_above_15 >= 3: score += 4
    elif years_above_15 >= 2: score += 3
    else: score += 1

    return min(score, 20)


def score_safety_margin(safety_margin):
    """维度2: 安全边际评分 (满分20)"""
    if safety_margin is None:
        return 8  # 无法计算给中性分
    if safety_margin >= 0.50: return 20
    if safety_margin >= 0.40: return 16
    if safety_margin >= 0.30: return 12
    if safety_margin >= 0.20: return 8
    if safety_margin >= 0.10: return 4
    return 0


def score_earnings_quality(data):
    """维度3: 盈利质量与稳定性 (满分15)"""
    score = 0

    # 连续盈利年数
    profitable_years = data.get("profitable_years", 0)
    net_incomes = data.get("net_incomes", [])
    if profitable_years >= 10 or (profitable_years >= 5 and len(net_incomes) <= 5):
        score += 5
    elif profitable_years >= 7: score += 4
    elif profitable_years >= 5: score += 3
    else: score += 1

    # 盈利增长稳定性
    if len(net_incomes) >= 3:
        growth_rates = []
        for i in range(len(net_incomes) - 1):
            if net_incomes[i + 1] and net_incomes[i + 1] > 0:
                growth_rates.append((net_incomes[i] - net_incomes[i + 1]) / abs(net_incomes[i + 1]))
        if growth_rates:
            mean_gr = np.mean(growth_rates)
            std_gr = np.std(growth_rates)
            stability = 1 - (std_gr / abs(mean_gr)) if abs(mean_gr) > 0.01 else 0.5
            if stability > 0.7: score += 5
            elif stability > 0.5: score += 4
            elif stability > 0.3: score += 3
            else: score += 1
        else:
            score += 3
    else:
        score += 3

    # 现金流匹配
    cashflow = data.get("cashflow")
    income = data.get("income_stmt")
    if cashflow is not None and income is not None:
        op_cfs = extract_annual_values(cashflow, ["Operating Cash Flow", "Total Cash From Operating Activities"])
        net_inc = extract_annual_values(income, ["Net Income", "Net Income Common Stockholders"])
        if op_cfs and net_inc:
            ratios = []
            for cf, ni in zip(op_cfs, net_inc):
                if ni and ni > 0:
                    ratios.append(cf / ni)
            if ratios:
                avg_ratio = np.mean(ratios)
                if avg_ratio > 1.2: score += 5
                elif avg_ratio > 1.0: score += 4
                elif avg_ratio > 0.8: score += 3
                else: score += 1
            else:
                score += 3
        else:
            score += 3
    else:
        score += 3

    return min(score, 15)


def score_roe_capital(data):
    """维度4: ROE与资本效率 (满分15)"""
    score = 0

    # ROE 水平
    avg_roe = data.get("avg_roe", 0)
    if avg_roe > 0.25: score += 8
    elif avg_roe > 0.20: score += 7
    elif avg_roe > 0.15: score += 6
    elif avg_roe > 0.10: score += 4
    else: score += 2

    # ROIC vs WACC（简化估算）
    income = data.get("income_stmt")
    balance = data.get("balance_sheet")
    if income is not None and balance is not None:
        ebit_vals = extract_annual_values(income, ["EBIT", "Operating Income"])
        total_assets = extract_annual_values(balance, ["Total Assets"])
        current_liab = extract_annual_values(balance, ["Current Liabilities", "Total Current Liabilities"])

        if ebit_vals and total_assets:
            ebit = ebit_vals[0]
            nopat = ebit * (1 - DEFAULT_TAX_RATE)
            ta = total_assets[0]
            cl = current_liab[0] if current_liab else 0
            invested_capital = ta - cl * 0.5  # 粗略估算
            if invested_capital > 0:
                roic = nopat / invested_capital
                beta = data.get("beta", 1.0) or 1.0
                wacc = RISK_FREE_RATE + beta * MARKET_RISK_PREMIUM * 0.7  # 近似
                spread = roic - wacc
                if spread > 0.10: score += 7
                elif spread > 0.05: score += 6
                elif spread > 0.02: score += 5
                elif spread > 0: score += 3
                else: score += 0
            else:
                score += 3
        else:
            score += 3
    else:
        score += 3

    return min(score, 15)


def score_management(data):
    """维度5: 管理层质量 (满分10)"""
    score = 0

    # 管理层持股
    insider_pct = data.get("insider_pct", 0) or 0
    if insider_pct > 0.10: score += 4
    elif insider_pct > 0.05: score += 3
    elif insider_pct > 0.01: score += 2
    else: score += 1

    # 股票回购（通过股本变化推断）
    cashflow = data.get("cashflow")
    if cashflow is not None:
        buyback = extract_annual_values(cashflow, [
            "Repurchase Of Capital Stock", "Common Stock Repurchased",
            "Repurchase Of Stock", "Issuance Of Capital Stock"
        ])
        if buyback:
            buyback_years = sum(1 for b in buyback if b and b < 0)  # 回购为负值
            if buyback_years >= 3: score += 3
            elif buyback_years >= 1: score += 2
            else: score += 1
        else:
            score += 1
    else:
        score += 1

    # 股息持续性
    dividend_yield = data.get("dividend_yield", 0) or 0
    payout_ratio = data.get("payout_ratio", 0) or 0
    if dividend_yield > 0 and payout_ratio > 0 and payout_ratio < 0.8:
        score += 3
    elif dividend_yield > 0:
        score += 2
    else:
        score += 1

    return min(score, 10)


def score_financial_health(data):
    """维度6: 财务健康度 (满分10)"""
    score = 0

    # 债务/EBITDA
    debt_ebitda = data.get("debt_ebitda")
    if debt_ebitda is not None:
        if debt_ebitda < 1: score += 4
        elif debt_ebitda < 2: score += 3
        elif debt_ebitda < 3: score += 2.5
        elif debt_ebitda < 4: score += 2
        else: score += 0
    else:
        score += 2  # 无数据给基础分

    # 流动比率
    balance = data.get("balance_sheet")
    if balance is not None:
        current_assets = extract_annual_values(balance, ["Current Assets", "Total Current Assets"])
        current_liab = extract_annual_values(balance, ["Current Liabilities", "Total Current Liabilities"])
        if current_assets and current_liab and current_liab[0] > 0:
            current_ratio = current_assets[0] / current_liab[0]
            if current_ratio > 2.0: score += 3
            elif current_ratio > 1.5: score += 2.5
            elif current_ratio > 1.0: score += 2
            else: score += 1
        else:
            score += 2
    else:
        score += 2

    # 利息覆盖率
    income = data.get("income_stmt")
    if income is not None:
        ebit_vals = extract_annual_values(income, ["EBIT", "Operating Income"])
        interest = extract_annual_values(income, ["Interest Expense", "Interest Expense Non Operating"])
        if ebit_vals and interest and interest[0] and abs(interest[0]) > 0:
            coverage = ebit_vals[0] / abs(interest[0])
            if coverage > 10: score += 3
            elif coverage > 5: score += 2.5
            elif coverage > 3: score += 2
            else: score += 1
        else:
            score += 2  # 可能无债务
    else:
        score += 2

    return min(score, 10)


def score_understandability(data):
    """维度7: 可理解性 (满分5)"""
    sector = data.get("sector", "Unknown")
    return get_industry_category(sector)


def score_sentiment_discount(data):
    """维度8: 市场情绪折价 (满分5)"""
    high = data.get("fifty_two_week_high", 0) or 0
    low = data.get("fifty_two_week_low", 0) or 0
    current = data.get("current_price", 0) or 0

    if high > low and low > 0 and current > 0:
        position = (current - low) / (high - low)
        if position < 0.20: return 5
        if position < 0.30: return 4
        if position < 0.40: return 3
        if position < 0.60: return 2
        return 1
    return 2  # 无数据给基础分


def compute_total_score(data, analysis_result):
    """计算8维度总分"""
    safety_margin = analysis_result.get("safety_margin")

    scores = {
        "moat": score_moat(data),
        "safety_margin": score_safety_margin(safety_margin),
        "earnings_quality": score_earnings_quality(data),
        "roe_capital": score_roe_capital(data),
        "management": score_management(data),
        "financial_health": score_financial_health(data),
        "understandability": score_understandability(data),
        "sentiment_discount": score_sentiment_discount(data),
    }
    scores["total"] = sum(scores.values())
    return scores


# ────────────────────────── 主流程 ──────────────────────────

def get_market_label(ticker_str):
    """根据 ticker 后缀确定市场"""
    if ticker_str.endswith(".HK"):
        return "香港"
    elif ticker_str.endswith(".SS"):
        return "上交所"
    elif ticker_str.endswith(".SZ"):
        return "深交所"
    elif ticker_str.endswith(".T"):
        return "东京"
    elif ticker_str.endswith((".L", ".PA", ".DE")):
        return "欧洲"
    else:
        return "美国"


def run_screening():
    """执行完整的三轮筛选"""
    all_tickers = get_all_tickers()
    total = len(all_tickers)

    # ── 第一轮 ──
    log.info("=" * 60)
    log.info("第一轮：量化初筛")
    log.info("=" * 60)

    passed_first = {}
    failed_count = 0
    batch_size = 20

    for i, ticker_str in enumerate(all_tickers):
        if (i + 1) % 50 == 0:
            log.info(f"进度: {i + 1}/{total} ({len(passed_first)} 通过)")

        if i > 0 and i % batch_size == 0:
            time.sleep(BATCH_DELAY)

        passed, data = first_round_screen(ticker_str)
        if passed:
            passed_first[ticker_str] = data
            log.info(f"  ✓ {ticker_str} ({data.get('name', '')[:30]}) — ROE: {data.get('avg_roe', 0):.1%}, P/B: {data.get('pb_ratio', 'N/A')}")
        else:
            failed_count += 1

        time.sleep(REQUEST_DELAY)

    log.info(f"\n第一轮结果: {len(passed_first)} 通过 / {failed_count} 未通过")

    # ── 第二轮 ──
    log.info("\n" + "=" * 60)
    log.info("第二轮：深度分析（DCF估值 + Owner Earnings）")
    log.info("=" * 60)

    passed_second = {}
    for i, (ticker_str, data) in enumerate(passed_first.items()):
        if (i + 1) % 10 == 0:
            log.info(f"深度分析进度: {i + 1}/{len(passed_first)}")

        analysis = second_round_analysis(ticker_str, data)
        if analysis["passed"]:
            passed_second[ticker_str] = {"data": data, "analysis": analysis}
            sm = analysis.get("safety_margin")
            sm_str = f"{sm:.1%}" if sm is not None else "N/A"
            log.info(f"  ✓ {ticker_str} — 安全边际: {sm_str}, 内在价值: {analysis.get('intrinsic_value', 'N/A')}")
        else:
            log.info(f"  ✗ {ticker_str} — {analysis.get('reason', '未通过')}")

        time.sleep(0.5)

    log.info(f"\n第二轮结果: {len(passed_second)} 通过")

    # ── 第三轮 ──
    log.info("\n" + "=" * 60)
    log.info("第三轮：综合评分排名")
    log.info("=" * 60)

    scored_stocks = []
    for ticker_str, item in passed_second.items():
        scores = compute_total_score(item["data"], item["analysis"])
        entry = {
            "ticker": ticker_str,
            "name": item["data"].get("name", ""),
            "market": get_market_label(ticker_str),
            "sector": item["data"].get("sector", "Unknown"),
            "industry": item["data"].get("industry", "Unknown"),
            "currency": item["data"].get("currency", "USD"),
            "market_cap": item["data"].get("market_cap", 0),
            "current_price": item["data"].get("current_price", 0),
            "pe_ratio": item["data"].get("pe_ratio"),
            "pb_ratio": item["data"].get("pb_ratio"),
            "avg_roe": item["data"].get("avg_roe", 0),
            "debt_ebitda": item["data"].get("debt_ebitda"),
            "intrinsic_value": item["analysis"].get("intrinsic_value"),
            "safety_margin": item["analysis"].get("safety_margin"),
            "owner_earnings": item["analysis"].get("owner_earnings"),
            "insider_pct": item["data"].get("insider_pct", 0),
            "dividend_yield": item["data"].get("dividend_yield", 0),
            "fifty_two_week_high": item["data"].get("fifty_two_week_high", 0),
            "fifty_two_week_low": item["data"].get("fifty_two_week_low", 0),
            "scores": {k: round(v, 1) for k, v in scores.items()},
            "dcf_details": None,  # will add cleaned version
        }

        # 清理 DCF 详情（确保 JSON 可序列化）
        dcf = item["analysis"].get("dcf_details")
        if dcf:
            entry["dcf_details"] = {
                "latest_fcf": round(dcf.get("latest_fcf", 0), 0),
                "growth_rate": round(dcf.get("growth_rate", 0), 4),
                "wacc": round(dcf.get("wacc", 0), 4),
                "terminal_growth": dcf.get("terminal_growth", TERMINAL_GROWTH_RATE),
                "intrinsic_per_share": round(dcf.get("intrinsic_per_share", 0), 2),
                "beta": round(dcf.get("beta", 1.0), 2),
            }

        scored_stocks.append(entry)

        log.info(f"  {ticker_str}: 总分 {scores['total']:.0f} "
                 f"(护城河:{scores['moat']:.0f} 安全边际:{scores['safety_margin']:.0f} "
                 f"盈利:{scores['earnings_quality']:.0f} ROE:{scores['roe_capital']:.0f} "
                 f"管理层:{scores['management']:.0f} 财务:{scores['financial_health']:.0f} "
                 f"可理解:{scores['understandability']:.0f} 情绪:{scores['sentiment_discount']:.0f})")

    # 排序
    scored_stocks.sort(key=lambda x: x["scores"]["total"], reverse=True)

    # 输出结果
    now = datetime.now()
    week_num = now.isocalendar()[1]
    year = now.year
    filename = f"{year}-W{week_num:02d}.json"
    filepath = DATA_DIR / filename

    output = {
        "generated_at": now.isoformat(),
        "year": year,
        "week": week_num,
        "total_screened": total,
        "passed_first_round": len(passed_first),
        "passed_second_round": len(passed_second),
        "top_picks": scored_stocks[:5],
        "all_scored": scored_stocks[:20],  # 保留 Top 20
        "parameters": {
            "risk_free_rate": RISK_FREE_RATE,
            "market_risk_premium": MARKET_RISK_PREMIUM,
            "terminal_growth_rate": TERMINAL_GROWTH_RATE,
            "max_growth_rate": MAX_GROWTH_RATE,
            "dcf_years": DCF_YEARS,
            "min_market_cap": MIN_MARKET_CAP,
            "max_pb": MAX_PB,
            "min_roe_5y": MIN_ROE_5Y,
            "min_profit_years": MIN_PROFIT_YEARS,
            "max_debt_ebitda": MAX_DEBT_EBITDA,
            "min_safety_margin": MIN_SAFETY_MARGIN,
        }
    }

    # 确保所有值都可序列化
    def clean_for_json(obj):
        if isinstance(obj, dict):
            return {k: clean_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_for_json(v) for v in obj]
        elif isinstance(obj, (np.integer,)):
            return int(obj)
        elif isinstance(obj, (np.floating,)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj

    output = clean_for_json(output)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(f"\n结果已保存到: {filepath}")
    log.info(f"\n{'=' * 60}")
    log.info("🏆 Top 5 选股结果")
    log.info("=" * 60)
    for i, stock in enumerate(scored_stocks[:5], 1):
        log.info(f"\n#{i} {stock['ticker']} ({stock['name']})")
        log.info(f"   市场: {stock['market']} | 行业: {stock['sector']}")
        log.info(f"   总分: {stock['scores']['total']:.0f}/100")
        log.info(f"   现价: {stock['current_price']} {stock['currency']} | 内在价值: {stock.get('intrinsic_value', 'N/A')}")
        sm = stock.get('safety_margin')
        log.info(f"   安全边际: {sm:.1%}" if sm else "   安全边际: N/A")
        log.info(f"   P/E: {stock.get('pe_ratio', 'N/A')} | P/B: {stock.get('pb_ratio', 'N/A')} | ROE: {stock.get('avg_roe', 0):.1%}")

    return filepath, output


if __name__ == "__main__":
    filepath, results = run_screening()
    print(f"\n✅ 筛选完成，结果文件: {filepath}")
