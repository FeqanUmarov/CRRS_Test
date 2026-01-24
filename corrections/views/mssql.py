import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from django.conf import settings

logger = logging.getLogger(__name__)

ALLOWED_INFO_FIELDS = {
    "ORG_ID",
    "RE_TYPE_ID",
    "RE_CATEGORY_ID",
    "RE_ADDRESS",
    "RE_FACTUAL_USE",
    "LAND_AREA_D",
    "LAND_AREA_F",
    "TOTAL_AREA_D",
    "TOTAL_AREA_F",
    "MAIN_AREA_D",
    "MAIN_AREA_F",
    "AUX_AREA_D",
    "AUX_AREA_F",
    "ROOM_COUNT_D",
    "ROOM_COUNT_F",
    "ILLEGAL_BUILDS",
    "NOTES",
    "CONCLUSION",
    "OPINION",
    "REQUEST_NUMBER",
}

try:
    import pyodbc

    PYODBC_AVAILABLE = True
except Exception:
    pyodbc = None
    PYODBC_AVAILABLE = False


def _jsonify_values(row: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in row.items():
        if isinstance(v, (datetime, date, Decimal)):
            out[k] = str(v)
        else:
            out[k] = v
    return out


def _filter_request_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    """DB-dən gələn sətirdən ancaq icazəli sütunları (case-insensitive) saxla."""
    if not row:
        return {}
    out = {}
    for k, v in row.items():
        if k is None:
            continue
        ku = str(k).upper()
        if ku in ALLOWED_INFO_FIELDS:
            out[ku] = v
    return out


def _as_bool(v):
    return str(v).strip().lower() in ("1", "true", "yes", "on")


def _odbc_escape(val: Optional[str]) -> str:
    v = "" if val is None else str(val).strip()
    if any(ch in v for ch in (";", "{", "}", " ")):
        v = v.replace("}", "}}")
        return "{" + v + "}"
    return v


def _mssql_connect():
    driver = (getattr(settings, "MSSQL_DRIVER", None) or "ODBC Driver 18 for SQL Server").strip()
    host = (getattr(settings, "MSSQL_HOST", "") or "").strip()
    port = int(getattr(settings, "MSSQL_PORT", 1433))
    db = (getattr(settings, "MSSQL_NAME", "") or "").strip()
    user = (getattr(settings, "MSSQL_USER", "") or "").strip()
    pwd = getattr(settings, "MSSQL_PASSWORD", "")
    enc = "yes" if _as_bool(getattr(settings, "MSSQL_ENCRYPT", True)) else "no"
    trust = "yes" if _as_bool(getattr(settings, "MSSQL_TRUST_CERT", False)) else "no"
    login_timeout = int(getattr(settings, "MSSQL_TIMEOUT", 5))

    if driver.startswith("{") and driver.endswith("}"):
        driver = driver[1:-1]
    driver = f"{{{driver}}}"

    # host\instance varsa port əlavə etmə, yoxdursa host,port
    server_part = host if ("\\" in host) else f"{host},{port}"

    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={server_part};"
        f"DATABASE={_odbc_escape(db)};"
        f"UID={_odbc_escape(user)};PWD={_odbc_escape(pwd)};"
        f"Encrypt={enc};TrustServerCertificate={trust};"
    )
    return pyodbc.connect(conn_str, timeout=login_timeout)


def _mssql_fetch_request(row_id: int) -> Optional[Dict[str, Any]]:
    try:
        with _mssql_connect() as cn:
            cur = cn.cursor()
            cur.execute(
                """
                SELECT UPPER(COLUMN_NAME)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'TBL_REQUEST_REG'
            """,
                (getattr(settings, "MSSQL_SCHEMA", "dbo"),),
            )
            cols = {r[0] for r in cur.fetchall()}
            idcol = "ROW_ID" if "ROW_ID" in cols else ("ROWID" if "ROWID" in cols else ("ID" if "ID" in cols else None))
            if not idcol:
                print("MSSQL: ID sütunu tapılmadı. Mövcud sütunlar:", cols)
                return None

            schema = getattr(settings, "MSSQL_SCHEMA", "dbo")
            sql = f"SELECT TOP 1 * FROM {schema}.TBL_REQUEST_REG WHERE {idcol} = ?"
            cur.execute(sql, (int(row_id),))
            row = cur.fetchone()
            if not row:
                print(f"MSSQL: Sətir tapılmadı ({idcol}={row_id})")
                return None

            colnames = [d[0] for d in cur.description]
            data = {colnames[i]: row[i] for i in range(len(colnames))}

            try:
                org_id = data.get("ORG_ID")
                if org_id is not None:
                    try:
                        cur.execute(
                            f"SELECT TOP 1 ORG_NAME_SHORT FROM {schema}.TBL_ORGS WHERE ORG_ID = ?",
                            (int(org_id),),
                        )
                        r = cur.fetchone()
                        if r and r[0]:
                            data["ORG_ID"] = r[0]
                    except Exception:
                        pass

                re_type_id = data.get("RE_TYPE_ID")
                if re_type_id is not None:
                    try:
                        cur.execute(
                            f"SELECT TOP 1 RE_TYPE_NAME FROM {schema}.DIC_RE_TYPES WHERE RE_TYPE_ID = ?",
                            (int(re_type_id),),
                        )
                        r = cur.fetchone()
                        if r and r[0]:
                            data["RE_TYPE_ID"] = r[0]
                    except Exception:
                        pass

                re_cat_id = data.get("RE_CATEGORY_ID")
                if re_cat_id is not None:
                    try:
                        cur.execute(
                            f"SELECT TOP 1 RE_CATEGORY_NAME FROM {schema}.DIC_RE_CATEGORIES WHERE RE_CATEGORY_ID = ?",
                            (int(re_cat_id),),
                        )
                        r = cur.fetchone()
                        if r and r[0]:
                            data["RE_CATEGORY_ID"] = r[0]
                    except Exception:
                        pass
            except Exception:
                pass

            return _jsonify_values(data)
    except Exception as e:
        print("MSSQL error:", e)
        return None


def _mssql_set_objectid(row_id: int, gis_id: int) -> bool:
    """
    TBL_REQUEST_REG cədvəlində OBJECTID sütununu güncəlləyir:
      OBJECTID = gis_id  WHERE <ID kolonu> = row_id
    ROW_ID / ROWID / ID sütunlarından hansının mövcud olduğunu avtomatik müəyyən edir.
    """
    try:
        with _mssql_connect() as cn:
            cur = cn.cursor()
            schema = getattr(settings, "MSSQL_SCHEMA", "dbo")

            # Mövcud sütunları oxu (ID kolonunu və OBJECTID-ni tapmaq üçün)
            cur.execute(
                """
                SELECT UPPER(COLUMN_NAME)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'TBL_REQUEST_REG'
            """,
                (schema,),
            )
            cols = {r[0] for r in cur.fetchall()}

            idcol = "ROW_ID" if "ROW_ID" in cols else ("ROWID" if "ROWID" in cols else ("ID" if "ID" in cols else None))
            if not idcol:
                raise RuntimeError("TBL_REQUEST_REG üçün ID kolonu (ROW_ID/ROWID/ID) tapılmadı.")
            if "OBJECTID" not in cols:
                raise RuntimeError("TBL_REQUEST_REG cədvəlində OBJECTID kolonu tapılmadı.")

            sql = f"UPDATE {schema}.TBL_REQUEST_REG SET OBJECTID = ? WHERE {idcol} = ?"
            cur.execute(sql, (int(gis_id), int(row_id)))
            cn.commit()
            return True
    except Exception as e:
        logger.error("MSSQL OBJECTID update failed: %s", e)
        return False


def _mssql_clear_objectid(row_id: int) -> bool:
    """
    TBL_REQUEST_REG cədvəlində OBJECTID sütununu NULL edir:
      OBJECTID = NULL WHERE <ID kolonu> = row_id
    ROW_ID / ROWID / ID sütunlarından hansının mövcud olduğunu avtomatik müəyyən edir.
    """
    try:
        with _mssql_connect() as cn:
            cur = cn.cursor()
            schema = getattr(settings, "MSSQL_SCHEMA", "dbo")

            # Sütunları yoxla
            cur.execute(
                """
                SELECT UPPER(COLUMN_NAME)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'TBL_REQUEST_REG'
            """,
                (schema,),
            )
            cols = {r[0] for r in cur.fetchall()}

            idcol = "ROW_ID" if "ROW_ID" in cols else ("ROWID" if "ROWID" in cols else ("ID" if "ID" in cols else None))
            if not idcol:
                raise RuntimeError("TBL_REQUEST_REG üçün ID kolonu (ROW_ID/ROWID/ID) tapılmadı.")
            if "OBJECTID" not in cols:
                raise RuntimeError("TBL_REQUEST_REG cədvəlində OBJECTID kolonu tapılmadı.")

            sql = f"UPDATE {schema}.TBL_REQUEST_REG SET OBJECTID = NULL WHERE {idcol} = ?"
            cur.execute(sql, (int(row_id),))
            cn.commit()
            return True
    except Exception as e:
        logger.error("MSSQL OBJECTID clear failed: %s", e)
        return False


# --- GIS edit icazəsi: STATUS_ID yalnız 2 və 99 olduqda ---

def _get_status_id_from_row(row: Optional[Dict[str, Any]]) -> Optional[int]:
    if not row:
        return None
    for k, v in row.items():
        if str(k).upper() == "STATUS_ID":
            try:
                return int(v)
            except Exception:
                return None
    return None


def _is_edit_allowed_for_fk(meta_id: int) -> Tuple[bool, Optional[int]]:
    details = _mssql_fetch_request(int(meta_id))
    sid = _get_status_id_from_row(details)
    return (sid in (2, 99)), sid