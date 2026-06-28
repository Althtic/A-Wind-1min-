from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from config import BacktestConfig


def _to_builtin(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    return value


def load_signal_data(path: Path) -> pd.DataFrame:
    data = pd.read_csv(path)
    data["Time"] = pd.to_datetime(data["Time"], errors="coerce")
    data["Date"] = pd.to_datetime(data["Date"], errors="coerce").dt.normalize()
    data = data.dropna(subset=["Time", "Date", "Bond_Code"])
    return data.sort_values(["Time", "Bond_Code"]).reset_index(drop=True)


def add_execution_price(data: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    data = data.copy()
    close = data["Bond_Close"].astype(float)
    data["exec_price"] = close

    if config.price_mode == "vwap":
        volume = data["Bond_Volume"].astype(float)
        amount = data["Bond_Amount"].astype(float)
        denom = volume * config.volume_multiplier
        vwap = amount / denom.replace(0, np.nan)
        valid_vwap = vwap.notna() & np.isfinite(vwap) & (vwap > 0)
        valid_vwap &= close.notna() & (close > 0)
        # Guard against unit mistakes or bad amount data.
        valid_vwap &= (vwap / close).between(0.5, 1.5)
        data.loc[valid_vwap, "exec_price"] = vwap.loc[valid_vwap]
    elif config.price_mode != "close":
        raise ValueError(f"Unsupported price_mode: {config.price_mode}")

    return data


def _position_market_value(position: dict, price_map: dict[str, float]) -> float:
    total = 0.0
    for bond_code, qty in position.items():
        price = price_map.get(bond_code)
        if price is not None and np.isfinite(price):
            total += qty * price
    return total


def _max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    running_max = equity.cummax()
    drawdown = equity / running_max - 1
    return float(drawdown.min())


def _drawdown_period(equity_curve: pd.DataFrame) -> dict:
    if equity_curve.empty:
        return {
            "max_drawdown": 0.0,
            "drawdown_start": pd.NaT,
            "drawdown_trough": pd.NaT,
            "drawdown_recovery": pd.NaT,
        }

    curve = equity_curve.sort_values("Time").reset_index(drop=True)
    equity = curve["total_equity"]
    running_max = equity.cummax()
    drawdown = equity / running_max - 1
    trough_idx = int(drawdown.idxmin())
    peak_idx = int(equity.loc[:trough_idx].idxmax())
    peak_value = float(equity.iloc[peak_idx])

    recovery = curve.loc[trough_idx + 1:][equity.loc[trough_idx + 1:] >= peak_value]
    recovery_time = recovery["Time"].iloc[0] if not recovery.empty else pd.NaT

    return {
        "max_drawdown": float(drawdown.iloc[trough_idx]),
        "drawdown_start": curve["Time"].iloc[peak_idx],
        "drawdown_trough": curve["Time"].iloc[trough_idx],
        "drawdown_recovery": recovery_time,
    }


def _build_trade_pairs(executions: list[dict]) -> pd.DataFrame:
    open_trades: dict[str, list[dict]] = {}
    pairs = []

    for event in executions:
        bond_code = event["Bond_Code"]
        if event["side"] == "BUY":
            open_trades.setdefault(bond_code, []).append(event)
            continue

        buys = open_trades.get(bond_code, [])
        if not buys:
            continue
        buy = buys.pop(0)
        buy_cash = buy["cash_amount"]
        sell_cash = event["cash_amount"]
        buy_fee = buy["fee"]
        sell_fee = event["fee"]
        pnl = sell_cash - sell_fee - buy_cash - buy_fee
        invested = buy_cash + buy_fee
        pairs.append({
            "Bond_Code": bond_code,
            "buy_time": buy["Time"],
            "sell_time": event["Time"],
            "buy_price": buy["price"],
            "sell_price": event["price"],
            "quantity": buy["quantity"],
            "buy_fee": buy_fee,
            "sell_fee": sell_fee,
            "gross_pnl": sell_cash - buy_cash,
            "net_pnl": pnl,
            "return_rate": pnl / invested if invested > 0 else np.nan,
            "holding_minutes": (event["Time"] - buy["Time"]).total_seconds() / 60,
            "sell_reason": event.get("reason", ""),
        })

    return pd.DataFrame(pairs)


def _daily_summary(equity_curve: pd.DataFrame, trades: pd.DataFrame) -> pd.DataFrame:
    if equity_curve.empty:
        return pd.DataFrame()

    daily = (
        equity_curve
        .sort_values("Time")
        .groupby("Date", as_index=False)
        .agg(
            start_equity=("total_equity", "first"),
            end_equity=("total_equity", "last"),
            min_equity=("total_equity", "min"),
            max_equity=("total_equity", "max"),
        )
    )
    daily["daily_return"] = daily["end_equity"] / daily["start_equity"] - 1

    if not trades.empty:
        trade_stats = trades.copy()
        trade_stats["Date"] = pd.to_datetime(trade_stats["sell_time"]).dt.normalize()
        grouped = trade_stats.groupby("Date").agg(
            trades=("net_pnl", "count"),
            winning_trades=("net_pnl", lambda x: int((x > 0).sum())),
            net_pnl=("net_pnl", "sum"),
        ).reset_index()
        daily = daily.merge(grouped, on="Date", how="left")
    else:
        daily["trades"] = 0
        daily["winning_trades"] = 0
        daily["net_pnl"] = 0.0

    for col in ["trades", "winning_trades", "net_pnl"]:
        daily[col] = daily[col].fillna(0)
    daily["win_rate"] = np.where(daily["trades"] > 0, daily["winning_trades"] / daily["trades"], np.nan)
    return daily


def _performance_summary(
    config: BacktestConfig,
    equity_curve: pd.DataFrame,
    trades: pd.DataFrame,
    skipped_orders: list[dict],
) -> dict:
    initial_cash = config.initial_cash
    final_equity = float(equity_curve["total_equity"].iloc[-1]) if not equity_curve.empty else initial_cash
    total_return = final_equity / initial_cash - 1
    total_trades = int(len(trades))
    wins = trades[trades["net_pnl"] > 0] if not trades.empty else pd.DataFrame()
    losses = trades[trades["net_pnl"] < 0] if not trades.empty else pd.DataFrame()

    avg_profit = float(wins["net_pnl"].mean()) if not wins.empty else 0.0
    avg_loss = float(losses["net_pnl"].mean()) if not losses.empty else 0.0
    profit_loss_ratio = avg_profit / abs(avg_loss) if avg_loss < 0 else np.nan

    if equity_curve.empty:
        annual_return = 0.0
        annual_volatility = 0.0
    else:
        days = max(1, equity_curve["Date"].nunique())
        annual_return = (1 + total_return) ** (config.annual_trading_days / days) - 1
        daily_equity = equity_curve.sort_values("Time").groupby("Date")["total_equity"].last()
        daily_returns = daily_equity.pct_change().dropna()
        annual_volatility = (
            float(daily_returns.std(ddof=1) * np.sqrt(config.annual_trading_days))
            if len(daily_returns) > 1
            else 0.0
        )

    drawdown_info = _drawdown_period(equity_curve)
    max_drawdown = drawdown_info["max_drawdown"]
    calmar_ratio = annual_return / abs(max_drawdown) if max_drawdown < 0 else np.nan
    sharpe_ratio = annual_return / annual_volatility if annual_volatility > 0 else np.nan

    summary = {
        "initial_cash": initial_cash,
        "final_equity": final_equity,
        "total_return": total_return,
        "annual_return": annual_return,
        "annual_volatility": annual_volatility,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown": max_drawdown,
        "max_drawdown_start": drawdown_info["drawdown_start"],
        "max_drawdown_trough": drawdown_info["drawdown_trough"],
        "max_drawdown_recovery": drawdown_info["drawdown_recovery"],
        "calmar_ratio": calmar_ratio,
        "total_trades": total_trades,
        "winning_trades": int(len(wins)),
        "losing_trades": int(len(losses)),
        "win_rate": float(len(wins) / total_trades) if total_trades else np.nan,
        "average_profit": avg_profit,
        "average_loss": avg_loss,
        "profit_loss_ratio": profit_loss_ratio,
        "average_trade_return": float(trades["return_rate"].mean()) if total_trades else np.nan,
        "average_holding_minutes": float(trades["holding_minutes"].mean()) if total_trades else np.nan,
        "skipped_orders": int(len(skipped_orders)),
        "price_mode": config.price_mode,
        "buy_fee_rate": config.buy_fee_rate,
        "sell_fee_rate": config.sell_fee_rate,
        "slippage_rate": config.slippage_rate,
        "order_cash": config.order_cash,
        "max_single_bond_weight": config.max_single_bond_weight,
    }
    return {k: _to_builtin(v) for k, v in summary.items()}


def _plot_equity_curve(equity_curve: pd.DataFrame, output_path: Path, summary: dict) -> None:
    if equity_curve.empty:
        return
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        return

    curve = equity_curve.sort_values("Time").copy()
    curve["net_value"] = curve["total_equity"] / curve["total_equity"].iloc[0]
    drawdown_info = _drawdown_period(curve)
    daily_curve = (
        curve
        .sort_values("Time")
        .groupby("Date", as_index=False)
        .agg(Time=("Time", "last"), total_equity=("total_equity", "last"))
    )
    daily_curve["drawdown"] = 1 - daily_curve["total_equity"] / daily_curve["total_equity"].cummax()

    width, height = 1400, 820
    margin_left, margin_right = 110, 88
    top, chart_bottom, panel_top, panel_bottom = 72, 560, 610, 760
    chart_width = width - margin_left - margin_right
    bg = "white"
    axis = "#444444"
    grid = "#dddddd"
    blue = "#1f77b4"
    red = "#d62728"
    bar_red = "#e15759"
    green = "#2ca02c"
    orange = "#ff7f0e"
    panel_bg = "#f7f7f7"

    image = Image.new("RGB", (width, height), bg)
    draw = ImageDraw.Draw(image)

    def load_font(size: int):
        font_candidates = [
            "/System/Library/Fonts/Hiragino Sans GB.ttc",
            "/System/Library/Fonts/STHeiti Medium.ttc",
            "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        ]
        for font_path in font_candidates:
            if Path(font_path).exists():
                return ImageFont.truetype(font_path, size=size)
        return ImageFont.load_default()

    title_font = load_font(24)
    axis_font = load_font(15)
    label_font = load_font(15)
    stat_label_font = load_font(17)
    stat_value_font = load_font(20)
    marker_font = load_font(15)

    def y_scale(values: pd.Series, y_top: int, y_bottom: int, pad_ratio: float = 0.08):
        min_v = float(values.min())
        max_v = float(values.max())
        if min_v == max_v:
            min_v -= 0.01
            max_v += 0.01
        pad = (max_v - min_v) * pad_ratio
        min_v -= pad
        max_v += pad

        def scale(v: float) -> int:
            return int(y_bottom - (float(v) - min_v) / (max_v - min_v) * (y_bottom - y_top))

        return scale, min_v, max_v

    n = len(curve)
    xs = np.linspace(margin_left, margin_left + chart_width, n)
    net_scale, net_min, net_max = y_scale(curve["net_value"], top, chart_bottom)
    max_daily_drawdown = max(0.0, float(daily_curve["drawdown"].max()))
    drawdown_axis_max = max(max_daily_drawdown * 1.12, 0.01)

    def drawdown_scale(value: float) -> int:
        return int(chart_bottom - float(value) / drawdown_axis_max * (chart_bottom - top))

    draw.text((margin_left, 24), "可转债策略回测 - 净值曲线", fill="#222222", font=title_font)

    tick_values = np.linspace(net_min, net_max, 6)
    for tick in tick_values:
        y = net_scale(tick)
        draw.line((margin_left, y, width - margin_right, y), fill=grid)
        draw.text((18, y - 9), f"{tick:.4f}", fill=axis, font=axis_font)

    draw.rectangle((margin_left, top, width - margin_right, chart_bottom), outline=axis)
    draw.rectangle((margin_left, panel_top, width - margin_right, panel_bottom), outline="#cccccc", fill=panel_bg)
    draw.text((20, top - 30), "净值", fill=axis, font=axis_font)

    right_axis_x = width - margin_right
    dd_tick_values = np.linspace(0, drawdown_axis_max, 6)
    for tick in dd_tick_values:
        y = drawdown_scale(tick)
        draw.line((right_axis_x, y, right_axis_x + 6, y), fill=axis)
        draw.text((right_axis_x + 10, y - 9), f"{tick:.1%}", fill=red, font=axis_font)
    draw.text((right_axis_x - 20, top - 30), "日度回撤", fill=red, font=axis_font)

    daily_xs = np.linspace(margin_left, margin_left + chart_width, len(daily_curve))
    zero_y = drawdown_scale(0)
    dd_points = [(int(x), drawdown_scale(v)) for x, v in zip(daily_xs, daily_curve["drawdown"])]
    if len(dd_points) > 1:
        area_points = [(dd_points[0][0], zero_y)] + dd_points + [(dd_points[-1][0], zero_y)]
        draw.polygon(area_points, fill="#f9d6d5")
        draw.line(dd_points, fill=bar_red, width=3)
        draw.line((margin_left, zero_y, width - margin_right, zero_y), fill="#cc7777")

    net_points = [(int(x), net_scale(v)) for x, v in zip(xs, curve["net_value"])]
    if len(net_points) > 1:
        draw.line(net_points, fill=blue, width=3)

    time_to_x = {time: int(x) for time, x in zip(curve["Time"], xs)}
    time_to_net_value = dict(zip(curve["Time"], curve["net_value"]))

    marker_specs = [
        ("回撤起点", drawdown_info["drawdown_start"], orange),
        ("回撤低点", drawdown_info["drawdown_trough"], red),
        ("修复点", drawdown_info["drawdown_recovery"], green),
    ]
    for label, time_value, color in marker_specs:
        if pd.isna(time_value) or time_value not in time_to_x:
            continue
        x = time_to_x[time_value]
        y = net_scale(time_to_net_value[time_value])
        draw.ellipse((x - 6, y - 6, x + 6, y + 6), fill=color, outline="white", width=2)
        label_y = y - 30 if label != "回撤低点" else y + 10
        label_text = f"{label} {pd.Timestamp(time_value).strftime('%Y-%m-%d %H:%M')}"
        draw.text((x + 8, label_y), label_text, fill=color, font=marker_font)

    draw.line((margin_left, chart_bottom + 1, width - margin_right, chart_bottom + 1), fill=axis)

    date_ticks = np.linspace(0, len(curve) - 1, 6).round().astype(int)
    last_label_right = -10_000
    for idx in date_ticks:
        tick_time = curve["Time"].iloc[idx]
        x = int(xs[idx])
        draw.line((x, chart_bottom, x, chart_bottom + 6), fill=axis)
        label = pd.Timestamp(tick_time).strftime("%Y-%m-%d")
        approx_width = 92
        label_x = max(margin_left, min(x - approx_width // 2, width - margin_right - approx_width))
        if label_x <= last_label_right + 8:
            continue
        draw.text((label_x, chart_bottom + 14), label, fill=axis, font=label_font)
        last_label_right = label_x + approx_width

    def fmt_pct(value: float) -> str:
        return "n/a" if pd.isna(value) else f"{value:.2%}"

    def fmt_pct_abs(value: float) -> str:
        return "n/a" if pd.isna(value) else f"{abs(value):.2%}"

    def fmt_num(value: float) -> str:
        return "n/a" if pd.isna(value) else f"{value:.2f}"

    recovery_text = (
        pd.Timestamp(drawdown_info["drawdown_recovery"]).strftime("%Y-%m-%d %H:%M")
        if not pd.isna(drawdown_info["drawdown_recovery"])
        else "未修复"
    )
    stats = [
        ("累计收益率", fmt_pct(summary.get("total_return"))),
        ("年化收益率", fmt_pct(summary.get("annual_return"))),
        ("年化波动率", fmt_pct(summary.get("annual_volatility"))),
        ("最大回撤", fmt_pct_abs(summary.get("max_drawdown"))),
        ("卡玛比率", fmt_num(summary.get("calmar_ratio"))),
        ("夏普比率", fmt_num(summary.get("sharpe_ratio"))),
        ("胜率", fmt_pct(summary.get("win_rate"))),
        ("回撤修复", recovery_text),
    ]
    x_positions = [margin_left + 28, margin_left + 350, margin_left + 672, margin_left + 994]
    y_positions = [panel_top + 28, panel_top + 82]
    for i, (name, value) in enumerate(stats):
        x = x_positions[i % 4]
        y = y_positions[i // 4]
        draw.text((x, y), name, fill="#666666", font=stat_label_font)
        draw.text((x, y + 26), value, fill="#222222", font=stat_value_font)

    image.save(output_path)


def run_backtest(config: BacktestConfig) -> dict:
    data = add_execution_price(load_signal_data(config.signal_path), config)
    config.output_dir.mkdir(parents=True, exist_ok=True)

    cash = float(config.initial_cash)
    positions: dict[str, float] = {}
    latest_prices: dict[str, float] = {}
    executions: list[dict] = []
    skipped_orders: list[dict] = []
    equity_rows: list[dict] = []

    for time, frame in data.groupby("Time", sort=True):
        frame = frame.sort_values(["rank_percentile_premium_rate", "Bond_Code"], na_position="last")

        for row in frame.itertuples(index=False):
            price = getattr(row, "exec_price")
            close = getattr(row, "Bond_Close")
            bond_code = getattr(row, "Bond_Code")
            if pd.notna(price) and np.isfinite(price) and price > 0:
                latest_prices[bond_code] = float(price)
            elif pd.notna(close) and np.isfinite(close) and close > 0:
                latest_prices[bond_code] = float(close)

        for row in frame.itertuples(index=False):
            if getattr(row, "signal_sell") != 1:
                continue
            bond_code = getattr(row, "Bond_Code")
            quantity = positions.get(bond_code, 0.0)
            if quantity <= 0:
                continue
            price = getattr(row, "exec_price")
            if pd.isna(price) or not np.isfinite(price) or price <= 0:
                skipped_orders.append({"Time": time, "Bond_Code": bond_code, "side": "SELL", "reason": "missing_price"})
                continue
            fill_price = float(price) * (1 - config.slippage_rate)
            cash_amount = quantity * fill_price
            fee = cash_amount * config.sell_fee_rate
            cash += cash_amount - fee
            positions.pop(bond_code, None)
            executions.append({
                "Time": time,
                "Date": getattr(row, "Date"),
                "Bond_Code": bond_code,
                "side": "SELL",
                "price": fill_price,
                "quantity": quantity,
                "cash_amount": cash_amount,
                "fee": fee,
                "cash_after": cash,
                "reason": "signal_sell",
            })

        current_equity = cash + _position_market_value(positions, latest_prices)
        buy_frame = frame[frame["signal_buy"] == 1].sort_values(
            ["rank_percentile_premium_rate", "Bond_Code"],
            na_position="last",
        )

        for row in buy_frame.itertuples(index=False):
            bond_code = getattr(row, "Bond_Code")
            if positions.get(bond_code, 0.0) > 0:
                skipped_orders.append({"Time": time, "Bond_Code": bond_code, "side": "BUY", "reason": "already_holding"})
                continue

            price = getattr(row, "exec_price")
            if pd.isna(price) or not np.isfinite(price) or price <= 0:
                skipped_orders.append({"Time": time, "Bond_Code": bond_code, "side": "BUY", "reason": "missing_price"})
                continue

            fill_price = float(price) * (1 + config.slippage_rate)
            target_cash = config.order_cash
            if config.max_single_bond_weight is not None:
                max_position_value = current_equity * config.max_single_bond_weight
                target_cash = min(target_cash, max_position_value)

            if target_cash <= 0:
                skipped_orders.append({"Time": time, "Bond_Code": bond_code, "side": "BUY", "reason": "insufficient_cash"})
                continue

            fee = target_cash * config.buy_fee_rate
            total_cost = target_cash + fee
            if total_cost > cash + 1e-8:
                skipped_orders.append({"Time": time, "Bond_Code": bond_code, "side": "BUY", "reason": "insufficient_cash"})
                continue

            quantity = target_cash / fill_price
            cash -= total_cost
            positions[bond_code] = quantity
            latest_prices[bond_code] = fill_price
            executions.append({
                "Time": time,
                "Date": getattr(row, "Date"),
                "Bond_Code": bond_code,
                "side": "BUY",
                "price": fill_price,
                "quantity": quantity,
                "cash_amount": target_cash,
                "fee": fee,
                "cash_after": cash,
                "reason": "signal_buy",
            })
            current_equity = cash + _position_market_value(positions, latest_prices)

        market_value = _position_market_value(positions, latest_prices)
        equity_rows.append({
            "Time": time,
            "Date": pd.Timestamp(time).normalize(),
            "cash": cash,
            "market_value": market_value,
            "total_equity": cash + market_value,
            "position_count": len(positions),
        })

    executions_df = pd.DataFrame(executions)
    trades_df = _build_trade_pairs(executions)
    equity_curve = pd.DataFrame(equity_rows)
    daily = _daily_summary(equity_curve, trades_df)
    summary = _performance_summary(config, equity_curve, trades_df, skipped_orders)

    executions_df.to_csv(config.output_dir / "executions.csv", index=False, encoding="utf-8-sig")
    trades_df.to_csv(config.output_dir / "trades.csv", index=False, encoding="utf-8-sig")
    equity_curve.to_csv(config.output_dir / "equity_curve.csv", index=False, encoding="utf-8-sig")
    daily.to_csv(config.output_dir / "daily_summary.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame([summary]).to_csv(config.output_dir / "performance_summary.csv", index=False, encoding="utf-8-sig")
    with (config.output_dir / "performance_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2, allow_nan=True)
    skipped_columns = ["Time", "Bond_Code", "side", "reason"]
    pd.DataFrame(skipped_orders, columns=skipped_columns).to_csv(
        config.output_dir / "skipped_orders.csv",
        index=False,
        encoding="utf-8-sig",
    )
    _plot_equity_curve(equity_curve, config.output_dir / "equity_curve.png", summary)

    return summary
