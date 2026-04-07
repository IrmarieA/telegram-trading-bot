"""SQLite trade journal (async via aiosqlite)."""

from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiosqlite

DB_PATH = Path(os.getenv("SQLITE_DB_PATH", str(Path(__file__).resolve().parent.parent / "forex_bot.sqlite3"))).resolve()


async def init_db() -> None:
    """Create the trades table if it doesn't exist."""

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS user_settings (
                user_id INTEGER PRIMARY KEY,
                alerts_on INTEGER DEFAULT 0,
                account_size REAL DEFAULT NULL,
                timezone TEXT DEFAULT 'America/New_York',
                onboarded INTEGER DEFAULT 0
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                pair TEXT,
                direction TEXT,
                entry REAL,
                stop_loss REAL,
                target REAL,
                pips_sl REAL,
                pips_tp REAL,
                risk_reward TEXT,
                timestamp TEXT,
                result TEXT DEFAULT NULL
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS open_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                pair TEXT,
                direction TEXT,
                entry REAL,
                stop_loss REAL,
                target REAL,
                pips_sl REAL,
                pips_tp REAL,
                risk_reward TEXT,
                timestamp TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS closed_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                pair TEXT,
                direction TEXT,
                entry REAL,
                stop_loss REAL,
                target REAL,
                exit_price REAL,
                pips_result REAL,
                result TEXT,
                ai_bias TEXT,
                timestamp_open TEXT,
                timestamp_close TEXT,
                risk_reward TEXT
            )
            """
        )
        await db.commit()

    # Older DBs: add columns safely if they already exist.
    async with aiosqlite.connect(DB_PATH) as db:
        for _sql in (
            "ALTER TABLE open_trades ADD COLUMN reason TEXT DEFAULT NULL",
            "ALTER TABLE closed_trades ADD COLUMN reason TEXT DEFAULT NULL",
        ):
            try:
                await db.execute(_sql)
            except Exception:
                pass  # column already exists
        try:
            await db.execute("ALTER TABLE closed_trades ADD COLUMN risk_reward TEXT")
        except Exception:
            pass
        await db.commit()


async def save_user_setting(user_id: int, key: str, value: Any) -> None:
    """
    Updates a single column in user_settings for this user.
    Creates the row first if it doesn't exist (INSERT OR IGNORE).
    """

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO user_settings (user_id) VALUES (?)", (user_id,))
        await db.execute(f"UPDATE user_settings SET {key} = ? WHERE user_id = ?", (value, user_id))
        await db.commit()


async def get_user_setting(user_id: int, key: str) -> Any:
    """
    Returns the value of a single column for this user.
    Returns None if user not found.
    """

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(f"SELECT {key} FROM user_settings WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
    if not row:
        return None
    return row[0]


async def save_open_trade(trade_dict: Dict[str, Any]) -> int:
    """Inserts into open_trades, returns the new row id."""

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            INSERT INTO open_trades (
                user_id, pair, direction,
                entry, stop_loss, target,
                pips_sl, pips_tp, risk_reward,
                timestamp,
                reason
            )
            VALUES (?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?)
            """,
            (
                trade_dict["user_id"],
                trade_dict["pair"],
                trade_dict["direction"],
                trade_dict["entry"],
                trade_dict["stop_loss"],
                trade_dict["target"],
                trade_dict["pips_sl"],
                trade_dict["pips_tp"],
                trade_dict["risk_reward"],
                trade_dict["timestamp"],
                trade_dict.get("reason"),
            ),
        )
        await db.commit()
        return int(cursor.lastrowid)


async def get_open_trades(user_id: int) -> List[Dict[str, Any]]:
    """Returns all open trades for this user as list of dicts (newest first)."""

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id, user_id, pair, direction,
                entry, stop_loss, target,
                pips_sl, pips_tp, risk_reward,
                timestamp, reason
            FROM open_trades
            WHERE user_id = ?
            ORDER BY timestamp DESC, id DESC
            """,
            (user_id,),
        )
        rows = await cursor.fetchall()

    cols = [
        "id",
        "user_id",
        "pair",
        "direction",
        "entry",
        "stop_loss",
        "target",
        "pips_sl",
        "pips_tp",
        "risk_reward",
        "timestamp",
        "reason",
    ]
    return [dict(zip(cols, row)) for row in rows]


def _pips_from_entry_exit(pair: str, direction: str, entry: float, exit_price: float) -> float:
    if pair == "EURUSD":
        mult = 10000.0
    else:
        mult = 10.0
    if direction == "BUY":
        return (exit_price - entry) * mult
    return (entry - exit_price) * mult


async def close_trade(trade_id: int, exit_price: float, result: str, ai_bias: str) -> Dict[str, Any]:
    """
    Moves trade from open_trades to closed_trades and returns the closed trade dict.
    Deletes from open_trades after inserting to closed_trades.
    """

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id, user_id, pair, direction,
                entry, stop_loss, target,
                risk_reward,
                timestamp,
                reason
            FROM open_trades
            WHERE id = ?
            """,
            (trade_id,),
        )
        row = await cursor.fetchone()
        if not row:
            raise ValueError("Trade not found")

        (
            _id,
            user_id,
            pair,
            direction,
            entry,
            stop_loss,
            target,
            risk_reward,
            timestamp_open,
            reason,
        ) = row

        pips_result = _pips_from_entry_exit(str(pair), str(direction), float(entry), float(exit_price))
        timestamp_close = os.getenv("NOW_ISO") or ""  # fallback placeholder if needed
        if not timestamp_close:
            # keep import footprint minimal; use ISO timestamp here
            import datetime as _dt

            timestamp_close = _dt.datetime.now().isoformat()

        await db.execute(
            """
            INSERT INTO closed_trades (
                user_id, pair, direction,
                entry, stop_loss, target,
                exit_price, pips_result, result, ai_bias,
                timestamp_open, timestamp_close,
                risk_reward,
                reason
            )
            VALUES (?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?, ?)
            """,
            (
                user_id,
                pair,
                direction,
                entry,
                stop_loss,
                target,
                exit_price,
                pips_result,
                result,
                ai_bias,
                timestamp_open,
                timestamp_close,
                risk_reward,
                reason,
            ),
        )
        await db.execute("DELETE FROM open_trades WHERE id = ?", (trade_id,))
        await db.commit()

    return {
        "user_id": user_id,
        "pair": pair,
        "direction": direction,
        "entry": float(entry),
        "stop_loss": float(stop_loss),
        "target": float(target),
        "exit_price": float(exit_price),
        "pips_result": float(pips_result),
        "result": result,
        "ai_bias": ai_bias,
        "timestamp_open": timestamp_open,
        "timestamp_close": timestamp_close,
        "risk_reward": risk_reward,
        "reason": reason,
    }


async def get_closed_trades(user_id: int) -> List[Dict[str, Any]]:
    """Returns all closed trades for this user as list of dicts (newest first)."""

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id, user_id, pair, direction,
                entry, stop_loss, target,
                exit_price, pips_result, result, ai_bias,
                timestamp_open, timestamp_close,
                risk_reward,
                reason
            FROM closed_trades
            WHERE user_id = ?
            ORDER BY timestamp_close DESC, id DESC
            """,
            (user_id,),
        )
        rows = await cursor.fetchall()

    cols = [
        "id",
        "user_id",
        "pair",
        "direction",
        "entry",
        "stop_loss",
        "target",
        "exit_price",
        "pips_result",
        "result",
        "ai_bias",
        "timestamp_open",
        "timestamp_close",
        "risk_reward",
        "reason",
    ]
    return [dict(zip(cols, row)) for row in rows]


def _edge_is_winner(result: Optional[str]) -> bool:
    if result is None:
        return False
    r = str(result).strip()
    return r in ("Winner", "WIN")


async def get_edge_report(user_id: int) -> Optional[Dict[str, Dict[str, Any]]]:
    """
    Groups closed_trades by reason. Each value has:
      trade_count, winners, win_rate (1 decimal %), avg_pips

    Dict keys are reasons, ordered by win_rate descending.
    Returns None if the user has no closed trades.
    """

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT reason, result, pips_result
            FROM closed_trades
            WHERE user_id = ?
            """,
            (user_id,),
        )
        rows = await cursor.fetchall()

    if not rows:
        return None

    buckets: Dict[str, List[Tuple[Optional[str], Optional[float]]]] = defaultdict(list)
    for reason, result, pips_result in rows:
        key = str(reason).strip() if reason is not None and str(reason).strip() else "(No reason)"
        try:
            p = float(pips_result) if pips_result is not None else 0.0
        except (TypeError, ValueError):
            p = 0.0
        buckets[key].append((result, p))

    metrics: List[tuple[str, Dict[str, Any]]] = []
    for reason_key, items in buckets.items():
        trade_count = len(items)
        winners = sum(1 for res, _ in items if _edge_is_winner(res))
        win_rate = round((winners / trade_count) * 100.0, 1) if trade_count else 0.0
        avg_pips = round(sum(p for _, p in items) / trade_count, 2) if trade_count else 0.0
        metrics.append(
            (
                reason_key,
                {
                    "trade_count": trade_count,
                    "winners": winners,
                    "win_rate": win_rate,
                    "avg_pips": avg_pips,
                },
            )
        )

    metrics.sort(key=lambda x: x[1]["win_rate"], reverse=True)
    return {k: v for k, v in metrics}


def _normalize_closed_result(result: Optional[str]) -> str:
    """Map legacy codes to canonical labels used in stats."""

    if result is None:
        return ""
    r = str(result).strip()
    if r in ("WIN", "Winner"):
        return "Winner"
    if r in ("LOSS", "Loser"):
        return "Loser"
    if r in ("BE", "Breakeven"):
        return "Breakeven"
    return r


async def get_stats(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Stats from ``closed_trades`` plus open count from ``open_trades``.

    Returns None only when the user has zero closed rows and zero open rows.

    ``total_trades`` is the count of closed rows only.
    ``win_rate`` = winners / total_trades * 100 (1 decimal), using result
    ``Winner`` / ``Loser`` / ``Breakeven`` (legacy WIN/LOSS/BE accepted).
    """

    async with aiosqlite.connect(DB_PATH) as db:
        cur_open = await db.execute(
            "SELECT COUNT(*) FROM open_trades WHERE user_id = ?",
            (user_id,),
        )
        open_row = await cur_open.fetchone()
        open_count = int(open_row[0]) if open_row else 0

        cur_closed = await db.execute(
            """
            SELECT pips_result, result, risk_reward, pair
            FROM closed_trades
            WHERE user_id = ?
            """,
            (user_id,),
        )
        closed_rows = await cur_closed.fetchall()

    total_trades = len(closed_rows)
    if total_trades == 0 and open_count == 0:
        return None

    winners = 0
    losers = 0
    breakeven = 0
    pips_list: List[float] = []
    pair_list: List[str] = []
    rr_vals: List[float] = []
    best_pair: Optional[str] = None
    worst_pair: Optional[str] = None
    best_pips: Optional[float] = None
    worst_pips: Optional[float] = None

    for pips_result, result, risk_reward, pair in closed_rows:
        try:
            p = float(pips_result) if pips_result is not None else 0.0
        except (TypeError, ValueError):
            p = 0.0
        pips_list.append(p)
        pair_list.append(str(pair) if pair is not None else "")

        norm = _normalize_closed_result(str(result) if result is not None else "")
        if norm == "Winner":
            winners += 1
        elif norm == "Loser":
            losers += 1
        elif norm == "Breakeven":
            breakeven += 1

        if isinstance(risk_reward, str) and ":" in risk_reward:
            try:
                rr_vals.append(float(risk_reward.split(":", 1)[1]))
            except Exception:
                pass

    if pips_list:
        best_pips = max(pips_list)
        worst_pips = min(pips_list)
        best_idx = pips_list.index(best_pips)
        worst_idx = pips_list.index(worst_pips)
        best_pair = pair_list[best_idx] or None
        worst_pair = pair_list[worst_idx] or None

    win_rate = round((winners / total_trades) * 100.0, 1) if total_trades else 0.0
    avg_rr = (sum(rr_vals) / len(rr_vals)) if rr_vals else None

    return {
        "total_trades": total_trades,
        "winners": winners,
        "losers": losers,
        "breakeven": breakeven,
        "open_count": open_count,
        "win_rate": win_rate,
        "best_trade": best_pips,
        "worst_trade": worst_pips,
        "best_pair": best_pair,
        "worst_pair": worst_pair,
        "avg_rr": avg_rr,
    }


async def save_trade(trade_dict: Dict[str, Any]) -> None:
    """
    Save a logged trade.

    Expects keys:
      user_id, pair, direction, entry, stop_loss, target,
      pips_sl, pips_tp, risk_reward, timestamp
    """
    # New canonical storage is open_trades.
    await save_open_trade(trade_dict)

    # Backward compatibility: best-effort insert into legacy `trades` table.
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO trades (
                    user_id, pair, direction,
                    entry, stop_loss, target,
                    pips_sl, pips_tp, risk_reward,
                    timestamp,
                    result
                )
                VALUES (?, ?, ?,
                        ?, ?, ?,
                        ?, ?, ?,
                        ?, NULL)
                """,
                (
                    trade_dict["user_id"],
                    trade_dict["pair"],
                    trade_dict["direction"],
                    trade_dict["entry"],
                    trade_dict["stop_loss"],
                    trade_dict["target"],
                    trade_dict["pips_sl"],
                    trade_dict["pips_tp"],
                    trade_dict["risk_reward"],
                    trade_dict["timestamp"],
                ),
            )
            await db.commit()
    except Exception:
        # Never crash the bot for legacy compatibility writes.
        pass


async def get_trades(user_id: int) -> List[Dict[str, Any]]:
    """
    Backward-compatible trade fetch.

    Prefer the new `open_trades` table; if it's empty, fall back to legacy `trades`.
    """

    open_rows = await get_open_trades(user_id)
    if open_rows:
        # Keep legacy shape by adding `result` as None.
        return [{**t, "result": None} for t in open_rows]

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id, user_id, pair, direction,
                entry, stop_loss, target,
                pips_sl, pips_tp, risk_reward,
                timestamp, result
            FROM trades
            WHERE user_id = ?
            ORDER BY timestamp DESC, id DESC
            """,
            (user_id,),
        )
        rows = await cursor.fetchall()

    cols = [
        "id",
        "user_id",
        "pair",
        "direction",
        "entry",
        "stop_loss",
        "target",
        "pips_sl",
        "pips_tp",
        "risk_reward",
        "timestamp",
        "result",
    ]
    return [dict(zip(cols, row)) for row in rows]
