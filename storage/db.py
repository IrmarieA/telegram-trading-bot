"""SQLite trade journal (async via aiosqlite)."""

from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite

DB_PATH = Path(os.getenv("SQLITE_DB_PATH", str(Path(__file__).resolve().parent.parent / "forex_bot.sqlite3"))).resolve()


async def _table_exists(db: aiosqlite.Connection, table_name: str) -> bool:
    cursor = await db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    )
    return await cursor.fetchone() is not None


async def _column_names(db: aiosqlite.Connection, table_name: str) -> set[str]:
    if not await _table_exists(db, table_name):
        return set()
    cursor = await db.execute(f"PRAGMA table_info({table_name})")
    rows = await cursor.fetchall()
    return {str(row[1]) for row in rows}


async def _add_column_if_missing(db: aiosqlite.Connection, table_name: str, column_sql: str) -> None:
    column_name = column_sql.split()[0]
    if column_name in await _column_names(db, table_name):
        return
    await db.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")


async def init_db() -> None:
    """Create and migrate the single canonical trade journal table."""

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
                result TEXT DEFAULT NULL,
                reason TEXT DEFAULT NULL,
                status TEXT DEFAULT 'OPEN',
                source TEXT DEFAULT 'MANUAL',
                open_time TEXT DEFAULT NULL,
                close_time TEXT DEFAULT NULL,
                trade_duration TEXT DEFAULT NULL,
                exit_price REAL DEFAULT NULL,
                pips_result REAL DEFAULT NULL,
                ai_bias TEXT DEFAULT NULL
            )
            """
        )
        for column_sql in (
            "reason TEXT DEFAULT NULL",
            "status TEXT DEFAULT 'OPEN'",
            "source TEXT DEFAULT 'MANUAL'",
            "open_time TEXT DEFAULT NULL",
            "close_time TEXT DEFAULT NULL",
            "trade_duration TEXT DEFAULT NULL",
            "exit_price REAL DEFAULT NULL",
            "pips_result REAL DEFAULT NULL",
            "ai_bias TEXT DEFAULT NULL",
        ):
            await _add_column_if_missing(db, "trades", column_sql)

        # Normalize old rows into the expanded journal shape.
        await db.execute("UPDATE trades SET open_time = COALESCE(open_time, timestamp)")
        await db.execute("UPDATE trades SET source = COALESCE(NULLIF(source, ''), 'MANUAL')")
        await db.execute(
            """
            UPDATE trades
            SET status = CASE
                WHEN result IS NULL OR TRIM(result) = '' THEN 'OPEN'
                ELSE 'CLOSED'
            END
            WHERE status IS NULL OR TRIM(status) = ''
            """
        )
        await db.commit()
        await _migrate_legacy_trade_tables(db)


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


def _now_iso() -> str:
    override = os.getenv("NOW_ISO")
    if override:
        return override
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _format_duration(open_time: Any, close_time: Any) -> Optional[str]:
    start = _parse_iso_datetime(open_time)
    end = _parse_iso_datetime(close_time)
    if start is None or end is None:
        return None
    if start.tzinfo is None and end.tzinfo is not None:
        start = start.replace(tzinfo=end.tzinfo)
    elif end.tzinfo is None and start.tzinfo is not None:
        end = end.replace(tzinfo=start.tzinfo)
    seconds = int((end - start).total_seconds())
    if seconds < 0:
        seconds = 0
    minutes, sec = divmod(seconds, 60)
    hours, minute = divmod(minutes, 60)
    days, hour = divmod(hours, 24)
    if days:
        return f"{days}d {hour}h {minute}m"
    if hours:
        return f"{hours}h {minute}m"
    if minutes:
        return f"{minutes}m"
    return f"{sec}s"


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_pair(pair: Any) -> str:
    text = str(pair or "").strip().upper()
    return text.replace("/", "").replace(" ", "")


def _normalize_direction(direction: Any) -> str:
    text = str(direction or "").strip().upper()
    if text in ("LONG", "BULL", "BULLISH"):
        return "BUY"
    if text in ("SHORT", "BEAR", "BEARISH"):
        return "SELL"
    return text


def _normalize_source(source: Any) -> str:
    text = str(source or "").strip().upper()
    return text if text in ("AUTO", "MANUAL") else "MANUAL"


def _normalize_closed_result(result: Optional[str]) -> str:
    """Map legacy and automatic result codes to canonical labels used in stats."""

    if result is None:
        return ""
    r = str(result).strip().upper()
    if r in ("WIN", "WINNER"):
        return "Winner"
    if r in ("LOSS", "LOSER"):
        return "Loser"
    if r in ("BE", "BREAKEVEN"):
        return "Breakeven"
    return str(result).strip()


def _edge_is_winner(result: Optional[str]) -> bool:
    return _normalize_closed_result(result) == "Winner"


def _is_open_status(status: Any) -> bool:
    return str(status or "").strip().upper() == "OPEN"


def _is_closed_result(result: Any) -> bool:
    return _normalize_closed_result(str(result) if result is not None else "") in (
        "Winner",
        "Loser",
        "Breakeven",
    )


def compute_trade_pips(
    pair: str,
    direction: str,
    entry: float,
    stop_loss: float,
    target: float,
) -> Tuple[float, float]:
    pip_size = 0.1 if _normalize_pair(pair) == "XAUUSD" else 0.0001
    direction_norm = _normalize_direction(direction)
    if direction_norm == "BUY":
        return (stop_loss - entry) / pip_size, (target - entry) / pip_size
    return (entry - stop_loss) / pip_size, (entry - target) / pip_size


def _risk_reward_from_pips(pips_sl: float, pips_tp: float) -> str:
    risk = abs(pips_sl)
    reward = abs(pips_tp)
    ratio = (reward / risk) if risk > 0 else 0.0
    return f"1:{ratio:.1f}"


def _pips_from_entry_exit(pair: str, direction: str, entry: float, exit_price: float) -> float:
    mult = 10000.0 if _normalize_pair(pair) == "EURUSD" else 10.0
    if _normalize_direction(direction) == "BUY":
        return (exit_price - entry) * mult
    return (entry - exit_price) * mult


def _row_dict(cols: List[str], row: tuple[Any, ...]) -> Dict[str, Any]:
    data = dict(zip(cols, row))
    if "open_time" in data:
        data["timestamp"] = data.get("open_time") or data.get("timestamp")
    return data


async def _fetch_existing_rows(
    db: aiosqlite.Connection,
    table_name: str,
    wanted_cols: List[str],
) -> List[Dict[str, Any]]:
    existing_cols = await _column_names(db, table_name)
    if not existing_cols:
        return []
    selected = [col for col in wanted_cols if col in existing_cols]
    if not selected:
        return []
    cursor = await db.execute(f"SELECT {', '.join(selected)} FROM {table_name}")
    rows = await cursor.fetchall()
    return [dict(zip(selected, row)) for row in rows]


async def _find_matching_trade_id(
    db: aiosqlite.Connection,
    row: Dict[str, Any],
    *,
    open_time_key: str = "timestamp",
) -> Optional[int]:
    cursor = await db.execute(
        """
        SELECT id
        FROM trades
        WHERE COALESCE(CAST(user_id AS TEXT), '') = ?
          AND COALESCE(pair, '') = ?
          AND COALESCE(direction, '') = ?
          AND COALESCE(CAST(entry AS TEXT), '') = ?
          AND COALESCE(CAST(stop_loss AS TEXT), '') = ?
          AND COALESCE(CAST(target AS TEXT), '') = ?
          AND COALESCE(open_time, timestamp, '') = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (
            "" if row.get("user_id") is None else str(row.get("user_id")),
            str(row.get("pair") or ""),
            str(row.get("direction") or ""),
            "" if row.get("entry") is None else str(row.get("entry")),
            "" if row.get("stop_loss") is None else str(row.get("stop_loss")),
            "" if row.get("target") is None else str(row.get("target")),
            str(row.get(open_time_key) or ""),
        ),
    )
    found = await cursor.fetchone()
    return int(found[0]) if found else None


async def _migrate_legacy_trade_tables(db: aiosqlite.Connection) -> None:
    """Fold legacy open/closed tables into the expanded trades journal."""

    open_rows = await _fetch_existing_rows(
        db,
        "open_trades",
        [
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
        ],
    )
    for row in open_rows:
        trade_id = await _find_matching_trade_id(db, row, open_time_key="timestamp")
        if trade_id is not None:
            await db.execute(
                """
                UPDATE trades
                SET status = 'OPEN',
                    open_time = COALESCE(open_time, ?),
                    reason = COALESCE(reason, ?),
                    source = COALESCE(NULLIF(source, ''), 'MANUAL')
                WHERE id = ?
                """,
                (row.get("timestamp"), row.get("reason"), trade_id),
            )
            continue
        await _insert_trade_with_connection(db, {**row, "source": "MANUAL", "status": "OPEN"})

    closed_rows = await _fetch_existing_rows(
        db,
        "closed_trades",
        [
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
        ],
    )
    for row in closed_rows:
        trade_id = await _find_matching_trade_id(db, row, open_time_key="timestamp_open")
        open_time = row.get("timestamp_open")
        close_time = row.get("timestamp_close")
        duration = _format_duration(open_time, close_time)
        payload = {
            **row,
            "timestamp": open_time,
            "open_time": open_time,
            "close_time": close_time,
            "trade_duration": duration,
            "source": "MANUAL",
            "status": "CLOSED",
        }
        if trade_id is None:
            await _insert_trade_with_connection(db, payload)
            continue
        await db.execute(
            """
            UPDATE trades
            SET status = 'CLOSED',
                result = ?,
                exit_price = ?,
                pips_result = ?,
                ai_bias = ?,
                close_time = ?,
                trade_duration = ?,
                reason = COALESCE(reason, ?),
                risk_reward = COALESCE(risk_reward, ?),
                source = COALESCE(NULLIF(source, ''), 'MANUAL')
            WHERE id = ?
            """,
            (
                row.get("result"),
                row.get("exit_price"),
                row.get("pips_result"),
                row.get("ai_bias"),
                close_time,
                duration,
                row.get("reason"),
                row.get("risk_reward"),
                trade_id,
            ),
        )
    await db.commit()


async def _insert_trade_with_connection(
    db: aiosqlite.Connection,
    trade_dict: Dict[str, Any],
) -> int:
    pair = _normalize_pair(trade_dict.get("pair"))
    direction = _normalize_direction(trade_dict.get("direction"))
    entry = _as_float(trade_dict.get("entry"))
    stop_loss = _as_float(trade_dict.get("stop_loss"))
    target = _as_float(trade_dict.get("target"))

    if "pips_sl" in trade_dict and "pips_tp" in trade_dict:
        pips_sl = _as_float(trade_dict.get("pips_sl"))
        pips_tp = _as_float(trade_dict.get("pips_tp"))
    else:
        pips_sl, pips_tp = compute_trade_pips(pair, direction, entry, stop_loss, target)

    risk_reward = str(trade_dict.get("risk_reward") or _risk_reward_from_pips(pips_sl, pips_tp))
    open_time = str(trade_dict.get("open_time") or trade_dict.get("timestamp") or _now_iso())
    timestamp = str(trade_dict.get("timestamp") or open_time)
    result = trade_dict.get("result")
    status = str(trade_dict.get("status") or ("CLOSED" if result else "OPEN")).upper()
    source = _normalize_source(trade_dict.get("source"))
    close_time = trade_dict.get("close_time")
    duration = trade_dict.get("trade_duration") or (
        _format_duration(open_time, close_time) if close_time else None
    )

    cursor = await db.execute(
        """
        INSERT INTO trades (
            user_id, pair, direction,
            entry, stop_loss, target,
            pips_sl, pips_tp, risk_reward,
            timestamp, result, reason,
            status, source, open_time, close_time,
            trade_duration, exit_price, pips_result, ai_bias
        )
        VALUES (?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?)
        """,
        (
            trade_dict.get("user_id"),
            pair,
            direction,
            entry,
            stop_loss,
            target,
            pips_sl,
            pips_tp,
            risk_reward,
            timestamp,
            result,
            trade_dict.get("reason"),
            status,
            source,
            open_time,
            close_time,
            duration,
            trade_dict.get("exit_price"),
            trade_dict.get("pips_result"),
            trade_dict.get("ai_bias"),
        ),
    )
    return int(cursor.lastrowid)


async def save_journal_trade(trade_dict: Dict[str, Any], *, source: str = "MANUAL") -> int:
    """Insert a trade into the canonical journal and return its row id."""

    async with aiosqlite.connect(DB_PATH) as db:
        row_id = await _insert_trade_with_connection(
            db,
            {**trade_dict, "source": source, "status": trade_dict.get("status") or "OPEN"},
        )
        await db.commit()
        return row_id


async def save_open_trade(trade_dict: Dict[str, Any]) -> int:
    """Backward-compatible alias for inserting an open manual journal trade."""

    return await save_journal_trade(trade_dict, source=str(trade_dict.get("source") or "MANUAL"))


async def save_auto_trade(trade_dict: Dict[str, Any]) -> int:
    """Insert an automatically tracked TradingView trade into the journal."""

    return await save_journal_trade({**trade_dict, "status": "OPEN"}, source="AUTO")


async def save_trade(trade_dict: Dict[str, Any]) -> None:
    """Save a manually logged trade in the canonical journal."""

    await save_journal_trade({**trade_dict, "status": "OPEN"}, source="MANUAL")


_OPEN_TRADE_COLS = [
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
    "status",
    "source",
    "open_time",
]


async def get_open_trades(user_id: int) -> List[Dict[str, Any]]:
    """Returns all open trades for this user from the journal (newest first)."""

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id, user_id, pair, direction,
                entry, stop_loss, target,
                pips_sl, pips_tp, risk_reward,
                timestamp, reason, status, source, open_time
            FROM trades
            WHERE user_id = ?
              AND UPPER(COALESCE(status, 'OPEN')) = 'OPEN'
            ORDER BY COALESCE(open_time, timestamp) DESC, id DESC
            """,
            (user_id,),
        )
        rows = await cursor.fetchall()
    return [_row_dict(_OPEN_TRADE_COLS, row) for row in rows]


async def get_all_open_trades() -> List[Dict[str, Any]]:
    """Returns every open trade in the journal for the automatic monitor."""

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id, user_id, pair, direction,
                entry, stop_loss, target,
                pips_sl, pips_tp, risk_reward,
                timestamp, reason, status, source, open_time
            FROM trades
            WHERE UPPER(COALESCE(status, 'OPEN')) = 'OPEN'
            ORDER BY COALESCE(open_time, timestamp) ASC, id ASC
            """
        )
        rows = await cursor.fetchall()
    return [_row_dict(_OPEN_TRADE_COLS, row) for row in rows]


async def close_trade(trade_id: int, exit_price: float, result: str, ai_bias: str) -> Dict[str, Any]:
    """Close a journal trade by updating the original trades row."""

    close_time = _now_iso()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id, user_id, pair, direction,
                entry, stop_loss, target,
                risk_reward, COALESCE(open_time, timestamp),
                reason, source
            FROM trades
            WHERE id = ?
              AND UPPER(COALESCE(status, 'OPEN')) = 'OPEN'
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
            source,
        ) = row

        pips_result = _pips_from_entry_exit(str(pair), str(direction), float(entry), float(exit_price))
        duration = _format_duration(timestamp_open, close_time)
        await db.execute(
            """
            UPDATE trades
            SET status = 'CLOSED',
                close_time = ?,
                trade_duration = ?,
                exit_price = ?,
                pips_result = ?,
                result = ?,
                ai_bias = ?
            WHERE id = ?
            """,
            (close_time, duration, exit_price, pips_result, result, ai_bias, trade_id),
        )
        await db.commit()

    return {
        "id": trade_id,
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
        "timestamp_close": close_time,
        "trade_duration": duration,
        "risk_reward": risk_reward,
        "reason": reason,
        "source": source,
    }


async def get_closed_trades(user_id: int) -> List[Dict[str, Any]]:
    """Returns closed journal trades for this user as list of dicts (newest first)."""

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
        "source",
        "trade_duration",
    ]
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id, user_id, pair, direction,
                entry, stop_loss, target,
                exit_price, pips_result, result, ai_bias,
                COALESCE(open_time, timestamp) AS timestamp_open,
                close_time AS timestamp_close,
                risk_reward, reason, source, trade_duration
            FROM trades
            WHERE user_id = ?
              AND UPPER(COALESCE(status, '')) != 'OPEN'
            ORDER BY close_time DESC, id DESC
            """,
            (user_id,),
        )
        rows = await cursor.fetchall()
    return [dict(zip(cols, row)) for row in rows]


async def get_edge_report(user_id: int) -> Optional[Dict[str, Dict[str, Any]]]:
    """
    Groups closed journal trades by reason. Kept for compatibility with future reports.
    """

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT reason, result, pips_result
            FROM trades
            WHERE user_id = ?
              AND UPPER(COALESCE(status, '')) != 'OPEN'
            """,
            (user_id,),
        )
        rows = await cursor.fetchall()

    if not rows:
        return None

    buckets: Dict[str, List[Tuple[Optional[str], Optional[float]]]] = defaultdict(list)
    for reason, result, pips_result in rows:
        key = str(reason).strip() if reason is not None and str(reason).strip() else "(No reason)"
        buckets[key].append((result, _as_float(pips_result)))

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


async def get_stats(user_id: int) -> Optional[Dict[str, Any]]:
    """Return My Stats directly from the canonical journal table."""

    async with aiosqlite.connect(DB_PATH) as db:
        cur_total = await db.execute("SELECT COUNT(*) FROM trades WHERE user_id = ?", (user_id,))
        total_row = await cur_total.fetchone()
        total_trades = int(total_row[0]) if total_row else 0

        cur_rows = await db.execute(
            """
            SELECT status, result
            FROM trades
            WHERE user_id = ?
            """,
            (user_id,),
        )
        rows = await cur_rows.fetchall()

    if total_trades == 0:
        return None

    open_count = 0
    winners = 0
    losers = 0
    for status, result in rows:
        if _is_open_status(status):
            open_count += 1
            continue
        norm = _normalize_closed_result(result)
        if norm == "Winner":
            winners += 1
        elif norm == "Loser":
            losers += 1

    decided = winners + losers
    win_rate = round((winners / decided) * 100.0, 1) if decided else 0.0
    return {
        "total_trades": total_trades,
        "winners": winners,
        "losers": losers,
        "open_count": open_count,
        "win_rate": win_rate,
    }


def _dt_in_tz(value: Any, tz: Any) -> Optional[datetime]:
    dt = _parse_iso_datetime(value)
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


async def get_daily_report_stats(
    user_id: int,
    *,
    report_date: Any,
    tz: Any,
) -> Dict[str, Any]:
    """Return the daily report data from the journal for one NY calendar day."""

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id, pair, direction, status, result,
                COALESCE(open_time, timestamp) AS open_time,
                close_time
            FROM trades
            WHERE user_id = ?
            ORDER BY close_time ASC, id ASC
            """,
            (user_id,),
        )
        rows = await cursor.fetchall()

    completed: List[Dict[str, Any]] = []
    open_count = 0
    for row in rows:
        trade = {
            "id": row[0],
            "pair": row[1],
            "direction": row[2],
            "status": row[3],
            "result": row[4],
            "open_time": row[5],
            "close_time": row[6],
        }
        if _is_open_status(trade["status"]):
            open_count += 1
            continue
        close_dt = _dt_in_tz(trade["close_time"], tz)
        if close_dt is None or close_dt.date() != report_date:
            continue
        norm = _normalize_closed_result(trade["result"])
        if norm in ("Winner", "Loser"):
            trade["normalized_result"] = norm
            completed.append(trade)

    wins = sum(1 for t in completed if t["normalized_result"] == "Winner")
    losses = sum(1 for t in completed if t["normalized_result"] == "Loser")
    decided = wins + losses
    win_rate = round((wins / decided) * 100.0, 1) if decided else 0.0
    return {
        "trades": decided,
        "wins": wins,
        "losses": losses,
        "open": open_count,
        "win_rate": win_rate,
        "completed": completed,
    }


async def get_trades(user_id: int) -> List[Dict[str, Any]]:
    """Backward-compatible fetch of all journal trades for a user."""

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
        "reason",
        "status",
        "source",
        "open_time",
        "close_time",
        "trade_duration",
    ]
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT
                id, user_id, pair, direction,
                entry, stop_loss, target,
                pips_sl, pips_tp, risk_reward,
                timestamp, result, reason, status, source,
                open_time, close_time, trade_duration
            FROM trades
            WHERE user_id = ?
            ORDER BY COALESCE(open_time, timestamp) DESC, id DESC
            """,
            (user_id,),
        )
        rows = await cursor.fetchall()
    return [dict(zip(cols, row)) for row in rows]
