from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.io.database import get_engine


logger = logging.getLogger(__name__)

SECTION_ORDER = (
    "General",
    "Highlights",
    "Valuation",
    "ShareStats",
    "SharesStats",
    "Technicals",
    "AnalystRatings",
    "SplitsDividends",
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate market_metrics_slim to market_metrics.")
    parser.add_argument("--batch-size", type=int, default=500)
    return parser.parse_args(argv)


def _section_order_sql() -> str:
    clauses = " ".join(
        f"WHEN '{section}' THEN {index}"
        for index, section in enumerate(SECTION_ORDER, start=1)
    )
    return f"CASE section {clauses} ELSE 999 END"


def _ensure_source_exists(conn, table: str) -> None:
    result = conn.execute(text("SELECT to_regclass(:table)"), {"table": table}).scalar()
    if result is None:
        raise RuntimeError(f"Source table '{table}' not found")


def _load_metric_types(conn, table: str) -> dict[str, str]:
    rows = conn.execute(
        text(
            f"""
            SELECT metric, value_type, COUNT(*) AS count
            FROM {table}
            GROUP BY metric, value_type
            ORDER BY metric
            """
        )
    ).fetchall()
    if not rows:
        raise RuntimeError(f"No metrics found in '{table}'")
    collected: dict[str, set[str]] = {}
    for metric, value_type, _ in rows:
        if not isinstance(metric, str):
            continue
        metric_key = metric.strip()
        if not metric_key:
            continue
        type_value = str(value_type).strip().lower() if value_type is not None else "text"
        normalized = "float" if type_value == "float" else "text"
        collected.setdefault(metric_key, set()).add(normalized)
    resolved: dict[str, str] = {}
    conflicts: dict[str, set[str]] = {}
    for metric, types in collected.items():
        if len(types) == 1:
            resolved[metric] = next(iter(types))
        else:
            resolved[metric] = "text"
            conflicts[metric] = types
    if conflicts:
        logger.info(
            "Metric type conflicts resolved to text (sample): %s",
            sorted(conflicts.items())[:25],
        )
    return resolved


def _market_metrics_table_sql(table: str, metric_types: Mapping[str, str]) -> str:
    column_sql: list[str] = []
    for metric, metric_type in metric_types.items():
        if metric_type == "float":
            sql_type = "DOUBLE PRECISION"
        else:
            sql_type = "TEXT"
        column_sql.append(f'    {_quote_identifier(metric)} {sql_type} NULL')
    columns = ",\n".join(column_sql)
    return f"""
    CREATE TABLE IF NOT EXISTS {table} (
        symbol TEXT NOT NULL,
        retrieval_date TIMESTAMPTZ NOT NULL,
{columns},
        PRIMARY KEY (symbol, retrieval_date)
    );
    CREATE INDEX IF NOT EXISTS IX_market_metrics_symbol
        ON {table} (symbol, retrieval_date);
    """


def _ensure_target_exists(conn, table: str, metric_types: Mapping[str, str]) -> None:
    result = conn.execute(text("SELECT to_regclass(:table)"), {"table": table}).scalar()
    if result is not None:
        return
    logger.info("Creating target table '%s'", table)
    ddl = _market_metrics_table_sql(table, metric_types)
    for statement in (stmt.strip() for stmt in ddl.split(";")):
        if statement:
            conn.exec_driver_sql(statement)
    verified = conn.execute(text("SELECT to_regclass(:table)"), {"table": table}).scalar()
    if verified is None:
        logger.warning("Target table '%s' still not visible after DDL", table)


def _log_connection_context(conn, label: str) -> None:
    row = conn.execute(text("SELECT current_database(), current_schema()")).fetchone()
    if row is None:
        return
    database, schema = row
    logger.info("%s database=%s schema=%s", label, database, schema)


def _select_source_rows(table: str) -> text:
    return text(
        f"""
        SELECT
            symbol,
            retrieval_date,
            section,
            metric,
            value_float,
            value_text,
            value_type
        FROM {table}
        ORDER BY symbol, retrieval_date, {_section_order_sql()}, metric
        """
    )


def _insert_statement(table: str, metric_columns: list[str]) -> text:
    columns = ["symbol", "retrieval_date", *metric_columns]
    param_map = {column: _metric_param_name(column) for column in columns}
    update_columns = ", ".join(
        f"{_quote_identifier(column)} = COALESCE(EXCLUDED.{_quote_identifier(column)}, "
        f"{table}.{_quote_identifier(column)})"
        for column in metric_columns
    )
    return text(
        f"""
        INSERT INTO {table} (
            {", ".join(_quote_identifier(column) for column in columns)}
        )
        VALUES (
            {", ".join(f":{param_map[column]}" for column in columns)}
        )
        ON CONFLICT (symbol, retrieval_date) DO UPDATE SET
            {update_columns}
        """
    )


def _row_params(row: Mapping[str, object], metric_columns: list[str]) -> dict[str, object]:
    columns = ["symbol", "retrieval_date", *metric_columns]
    param_map = {column: _metric_param_name(column) for column in columns}
    return {param_map[column]: row.get(column) for column in columns}


def _flush_rows(
    conn,
    insert_sql: text,
    rows: list[dict[str, object]],
    metric_columns: list[str],
) -> int:
    if not rows:
        return 0
    conn.execute(insert_sql, [_row_params(row, metric_columns) for row in rows])
    return len(rows)


def _normalize_source_value(
    value_type: object,
    value_float: object,
    value_text: object,
) -> object | None:
    normalized = str(value_type).strip().lower() if value_type is not None else ""
    if normalized == "float":
        return value_float if value_float is not None else value_text
    if normalized == "text":
        return value_text if value_text is not None else value_float
    return value_text if value_text is not None else value_float


def _convert_metric_value(metric_type: str, raw_value: object) -> object | None:
    if metric_type == "float":
        return _to_float(raw_value)
    if raw_value is None:
        return None
    if isinstance(raw_value, str):
        stripped = raw_value.strip()
        return stripped if stripped else None
    return str(raw_value)


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _metric_param_name(column: str) -> str:
    safe = "".join(char if char.isalnum() else "_" for char in column)
    if not safe or safe[0].isdigit():
        safe = f"m_{safe}"
    return f"p_{safe}"


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def _build_row(
    symbol: str,
    retrieval_date: datetime,
    metrics: Mapping[str, object],
    metric_columns: list[str],
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "retrieval_date": retrieval_date,
        **{metric: metrics.get(metric) for metric in metric_columns},
    }


def _iter_rows(
    source_rows: Iterable[Mapping[str, object]],
    unknown_metrics: Counter[str],
    collisions: Counter[str],
    metric_columns: list[str],
    metric_types: Mapping[str, str],
) -> Iterable[dict[str, object]]:
    current_key: tuple[str, datetime] | None = None
    metrics: dict[str, object] = {}

    def flush() -> dict[str, object] | None:
        nonlocal metrics, current_key
        if current_key is None:
            return None
        symbol, retrieval_date = current_key
        row = _build_row(symbol, retrieval_date, metrics, metric_columns)
        metrics = {}
        return row

    for row in source_rows:
        symbol = row.get("symbol")
        retrieval_date = row.get("retrieval_date")
        if not isinstance(symbol, str) or not isinstance(retrieval_date, datetime):
            continue
        key = (symbol, retrieval_date)
        if current_key is None:
            current_key = key
        if key != current_key:
            flushed = flush()
            if flushed is not None:
                yield flushed
            current_key = key
        metric = row.get("metric")
        if not isinstance(metric, str):
            continue
        if metric not in metric_types:
            unknown_metrics[metric] += 1
            continue
        raw_value = _normalize_source_value(
            row.get("value_type"),
            row.get("value_float"),
            row.get("value_text"),
        )
        converted = _convert_metric_value(metric_types[metric], raw_value)
        if metric in metrics:
            if metrics[metric] is not None and converted is not None:
                collisions[metric] += 1
                continue
            if converted is None:
                continue
        metrics[metric] = converted

    flushed = flush()
    if flushed is not None:
        yield flushed


def migrate(batch_size: int) -> None:
    database_url = os.getenv("HARBOUR_BRIDGE_DB_URL")
    if not database_url:
        raise RuntimeError("HARBOUR_BRIDGE_DB_URL is not set")
    engine = get_engine(database_url)
    source_table = "market_metrics_slim"
    target_table = "market_metrics"
    total_rows = 0
    total_inserted = 0
    unknown_metrics: Counter[str] = Counter()
    collisions: Counter[str] = Counter()
    buffer: list[dict[str, object]] = []

    with engine.connect().execution_options(stream_results=True) as read_conn:
        _ensure_source_exists(read_conn, source_table)
        metric_types = _load_metric_types(read_conn, source_table)
        metric_columns = list(metric_types.keys())
        insert_sql = _insert_statement(target_table, metric_columns)
        result = read_conn.execute(_select_source_rows(source_table))
        rows = result.mappings()
        with engine.begin() as write_conn:
            _log_connection_context(read_conn, "Read")
            _log_connection_context(write_conn, "Write")
            _ensure_target_exists(write_conn, target_table, metric_types)
            for item in _iter_rows(rows, unknown_metrics, collisions, metric_columns, metric_types):
                total_rows += 1
                buffer.append(item)
                if len(buffer) >= batch_size:
                    total_inserted += _flush_rows(write_conn, insert_sql, buffer, metric_columns)
                    buffer.clear()
            if buffer:
                total_inserted += _flush_rows(write_conn, insert_sql, buffer, metric_columns)
                buffer.clear()

    logger.info(
        "Migrated market_metrics rows: source=%d inserted=%d",
        total_rows,
        total_inserted,
    )
    if unknown_metrics:
        logger.info(
            "Skipped metrics not in schema (sample): %s",
            unknown_metrics.most_common(10),
        )
    if collisions:
        logger.info(
            "Metric collisions encountered (sample): %s",
            collisions.most_common(10),
        )


def main() -> None:
    args = _parse_args(sys.argv[1:])
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    migrate(batch_size=args.batch_size)


if __name__ == "__main__":
    main()
