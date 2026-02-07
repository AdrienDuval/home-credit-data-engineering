"""PostgreSQL connection for querying datamarts."""
import os
from contextlib import contextmanager
from typing import Any, Generator, List, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

from app.config import DATABASE_URL


def get_connection_params():
    if DATABASE_URL and DATABASE_URL.startswith("postgresql://"):
        return {"dsn": DATABASE_URL}
    return {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        "dbname": os.environ.get("POSTGRES_DB", "home_credit"),
        "user": os.environ.get("POSTGRES_USER", "home_credit_user"),
        "password": os.environ.get("POSTGRES_PASSWORD", "home_credit_pwd"),
    }


@contextmanager
def get_cursor(dict_cursor: bool = True) -> Generator[Any, None, None]:
    conn = psycopg2.connect(**get_connection_params())
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor if dict_cursor else None)
        try:
            yield cur
            conn.commit()
        finally:
            cur.close()
    finally:
        conn.close()


def _build_client_risk_where(
    risk_segment: Optional[str] = None,
    client_id: Optional[int] = None,
    min_income: Optional[float] = None,
    max_income: Optional[float] = None,
    min_credit_exposure: Optional[float] = None,
    max_credit_exposure: Optional[float] = None,
) -> tuple[str, list]:
    """Build WHERE clause and params for datamart_client_risk. Column names lowercase (PostgreSQL)."""
    conditions = []
    params: list = []
    if risk_segment is not None and risk_segment.strip():
        conditions.append("risk_segment = %s")
        params.append(risk_segment.strip())
    if client_id is not None:
        conditions.append('"SK_ID_CURR" = %s')
        params.append(client_id)
    if min_income is not None:
        conditions.append("(income IS NULL OR income >= %s)")
        params.append(min_income)
    if max_income is not None:
        conditions.append("(income IS NULL OR income <= %s)")
        params.append(max_income)
    if min_credit_exposure is not None:
        conditions.append("(credit_exposure IS NULL OR credit_exposure >= %s)")
        params.append(min_credit_exposure)
    if max_credit_exposure is not None:
        conditions.append("(credit_exposure IS NULL OR credit_exposure <= %s)")
        params.append(max_credit_exposure)
    where = " AND ".join(conditions) if conditions else "1=1"
    return where, params


def fetch_client_risk_page(
    page: int = 1,
    page_size: int = 50,
    risk_segment: Optional[str] = None,
    client_id: Optional[int] = None,
    min_income: Optional[float] = None,
    max_income: Optional[float] = None,
    min_credit_exposure: Optional[float] = None,
    max_credit_exposure: Optional[float] = None,
) -> tuple[List[dict], int]:
    offset = (page - 1) * page_size
    if offset < 0:
        offset = 0
    if page_size < 1 or page_size > 1000:
        page_size = 50
    where, where_params = _build_client_risk_where(
        risk_segment=risk_segment,
        client_id=client_id,
        min_income=min_income,
        max_income=max_income,
        min_credit_exposure=min_credit_exposure,
        max_credit_exposure=max_credit_exposure,
    )
    with get_cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) AS c FROM datamart.datamart_client_risk WHERE {where}",
            where_params,
        )
        total = cur.fetchone()["c"]
        order_col = '"SK_ID_CURR"'  # Spark/JDBC often creates quoted uppercase column names
        cur.execute(
            f"SELECT * FROM datamart.datamart_client_risk WHERE {where} ORDER BY {order_col} LIMIT %s OFFSET %s",
            where_params + [page_size, offset],
        )
        rows = cur.fetchall()
    return [dict(r) for r in rows], total


def fetch_client_by_id(sk_id_curr: int) -> Optional[dict]:
    """Return one client row by SK_ID_CURR or None if not found.
    Tries quoted uppercase first (Spark/JDBC often creates "SK_ID_CURR"), then lowercase.
    """
    with get_cursor() as cur:
        row = None
        try:
            cur.execute(
                'SELECT * FROM datamart.datamart_client_risk WHERE "SK_ID_CURR" = %s',
                (sk_id_curr,),
            )
            row = cur.fetchone()
        except Exception:
            pass
        if row is None:
            try:
                cur.execute(
                    "SELECT * FROM datamart.datamart_client_risk WHERE sk_id_curr = %s",
                    (sk_id_curr,),
                )
                row = cur.fetchone()
            except Exception:
                pass
    return dict(row) if row else None


def fetch_bureau_by_client(sk_id_curr: int) -> List[dict]:
    """Return all bureau credits for one client. Empty if table missing or no rows."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "SELECT * FROM datamart.datamart_client_bureau WHERE sk_id_curr = %s ORDER BY sk_id_bureau",
                (sk_id_curr,),
            )
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []


def fetch_previous_apps_by_client(sk_id_curr: int) -> List[dict]:
    """Return all previous applications for one client. Empty if table missing or no rows."""
    try:
        with get_cursor() as cur:
            cur.execute(
                "SELECT * FROM datamart.datamart_client_previous_apps WHERE sk_id_curr = %s ORDER BY sk_id_prev",
                (sk_id_curr,),
            )
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    except Exception:
        return []
