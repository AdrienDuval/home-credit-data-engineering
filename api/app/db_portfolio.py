"""Fetch portfolio summary for dashboard charts (datamart_portfolio_summary)."""
import os
from typing import Any, Generator, List

import psycopg2
from psycopg2.extras import RealDictCursor

# Reuse same connection logic as database.py
def _get_connection_params():
    from app.config import DATABASE_URL
    if DATABASE_URL and DATABASE_URL.startswith("postgresql://"):
        return {"dsn": DATABASE_URL}
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "dbname": os.environ.get("POSTGRES_DB", "home_credit"),
        "user": os.environ.get("POSTGRES_USER", "home_credit_user"),
        "password": os.environ.get("POSTGRES_PASSWORD", "home_credit_pwd"),
    }


def fetch_portfolio_summary() -> List[dict]:
    """Return all rows from datamart.datamart_portfolio_summary for dashboard charts."""
    conn = psycopg2.connect(**_get_connection_params())
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM datamart.datamart_portfolio_summary ORDER BY risk_segment")
        rows = cur.fetchall()
        cur.close()
        return [dict(r) for r in rows]
    finally:
        conn.close()
