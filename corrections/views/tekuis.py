import json
import os
import zlib
from typing import List, Optional

import oracledb
from django.conf import settings
from django.db import connection, transaction
from django.http import HttpResponseBadRequest, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET
from shapely import wkt as shapely_wkt
from shapely.geometry import mapping, shape as shapely_shape

from .attach import _find_attach_file, _geojson_from_csvtxt_file, _geojson_from_zip_file, _smb_net_use
from .auth import _redeem_ticket, _unauthorized, require_valid_ticket
from .geo_utils import _canonize_crs_value, _clean_wkt_text, _flatten_geoms, _payload_to_wkt_list
from corrections.tekuis_validation import ignore_gap, validate_tekuis

TEKUIS_ATTRS = (
    "ID",
    "LAND_CATEGORY2ENUM",
    "LAND_CATEGORY_ENUM",
    "NAME",
    "OWNER_TYPE_ENUM",
    "SUVARILMA_NOVU_ENUM",
    "EMLAK_NOVU_ENUM",
    "OLD_LAND_CATEGORY2ENUM",
    "TERRITORY_NAME",
    "RAYON_ADI",
    "IED_ADI",
    "BELEDIYE_ADI",
    "LAND_CATEGORY3ENUM",
    "LAND_CATEGORY4ENUM",
    "AREA_HA",
)


def _tekuis_props_from_row(vals):
    """Oracle-dan oxunan dəyərləri sütun adları ilə properties-ə çevirir."""
    return {k: v for k, v in zip(TEKUIS_ATTRS, vals)}


def _oracle_connect():
    host = os.getenv("ORA_HOST", "alldb-scan.emlak.gov.az")
    port = int(os.getenv("ORA_PORT", "1521"))
    service = os.getenv("ORA_SERVICE", "tekuisdb")
    user = os.getenv("ORA_USER")
    password = os.getenv("ORA_PASSWORD")

    dsn = oracledb.makedsn(host, port, service_name=service)

    # Bəzi oracledb versiyalarında 'encoding' dəstəklənmir → geriyə uyğun bağla
    try:
        return oracledb.connect(user=user, password=password, dsn=dsn, encoding="UTF-8", nencoding="UTF-8")
    except TypeError:
        return oracledb.connect(user=user, password=password, dsn=dsn)


def _has_active_tekuis(meta_id: int) -> bool:
    try:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                  FROM tekuis_parcel
                 WHERE meta_id = %s
                   AND COALESCE(status, 1) = 1
                 LIMIT 1
            """,
                [int(meta_id)],
            )
            return cur.fetchone() is not None
    except Exception:
        return False


@require_GET
def tekuis_parcels_by_bbox(request):
    try:
        minx = float(request.GET.get("minx"))
        miny = float(request.GET.get("miny"))
        maxx = float(request.GET.get("maxx"))
        maxy = float(request.GET.get("maxy"))
    except Exception:
        return HttpResponseBadRequest("minx/miny/maxx/maxy tələb olunur və ədədi olmalıdır.")

    schema = getattr(settings, "TEKUIS_SCHEMA", os.getenv("TEKUIS_SCHEMA", "BTG_MIS"))
    table = getattr(settings, "TEKUIS_TABLE", os.getenv("TEKUIS_TABLE", "M_G_PARSEL"))

    sql = f"""
        SELECT sde.st_astext(t.SHAPE) AS wkt,
               t.ID, t.LAND_CATEGORY2ENUM, t.LAND_CATEGORY_ENUM, t.NAME, t.OWNER_TYPE_ENUM,
               t.SUVARILMA_NOVU_ENUM, t.EMLAK_NOVU_ENUM, t.OLD_LAND_CATEGORY2ENUM,
               t.TERRITORY_NAME, t.RAYON_ADI, t.IED_ADI, t.BELEDIYE_ADI,t.LAND_CATEGORY3ENUM,t.LAND_CATEGORY4ENUM, t.AREA_HA
          FROM {schema}.{table} t
         WHERE t.SHAPE.MINX <= :maxx AND t.SHAPE.MAXX >= :minx
           AND t.SHAPE.MINY <= :maxy AND t.SHAPE.MAXY >= :miny
    """

    params = dict(minx=minx, miny=miny, maxx=maxx, maxy=maxy)

    features, skipped = [], 0
    with _oracle_connect() as cn:
        with cn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur:
                wkt_lob, *attr_vals = row
                raw = wkt_lob.read() if hasattr(wkt_lob, "read") else wkt_lob
                wkt = _clean_wkt_text(raw)
                if not wkt:
                    skipped += 1
                    continue
                try:
                    geom = shapely_wkt.loads(wkt)
                except Exception:
                    skipped += 1
                    continue
                props = _tekuis_props_from_row(attr_vals)
                props["SOURCE"] = "TEKUIS"
                features.append({"type": "Feature", "geometry": mapping(geom), "properties": props})

    print(f"[TEKUIS][BBOX] returned={len(features)} skipped={skipped} extent=({minx},{miny},{maxx},{maxy})")
    return JsonResponse({"type": "FeatureCollection", "features": features}, safe=False)


@csrf_exempt
def tekuis_parcels_by_geom(request):
    if request.method != "POST":
        return HttpResponseBadRequest("POST gözlənirdi.")

    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return HttpResponseBadRequest("Yanlış JSON.")

    # İstifadəçi SRID verə bilər, amma aşağıda avtomatik korreksiya edəcəyik
    srid_in_payload = int(payload.get("srid") or os.getenv("TEKUIS_SRID", 4326))
    buf_m = float(payload.get("buffer_m") or 0.0)

    schema = os.getenv("TEKUIS_SCHEMA", "BTG_MIS")
    table = os.getenv("TEKUIS_TABLE", "M_G_PARSEL")
    table_srid = int(os.getenv("TEKUIS_TABLE_SRID", 4326))  # cədvəl SRID

    # --- TEKUİS atributları (SELECT və properties üçün eyni sıra)
    attrs_sql = ", ".join([f"t.{c}" for c in TEKUIS_ATTRS])

    # WKT siyahısını yığ
    wkt_list = _payload_to_wkt_list(payload)
    if not wkt_list:
        w_single = _clean_wkt_text(payload.get("wkt")) if payload.get("wkt") else None
        if not w_single:
            return HttpResponseBadRequest("wkt və ya geojson verilməlidir.")
        wkt_list = [w_single]

    import re
    from shapely import wkt as _wkt
    from shapely import wkb as _wkb

    NUM_RE = r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?"
    TYPE_RE = r"(?:POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)"

    def _normalize_wkt_remove_m_dims(w: str) -> str:
        s = w
        m_hdr = re.match(rf"^\s*(?:{TYPE_RE})\s+(ZM|M)\b", s, flags=re.I)
        if not m_hdr:
            return s
        dim = m_hdr.group(1).upper()
        if dim == "ZM":
            s = re.sub(rf"\b({TYPE_RE})\s+ZM\b", r"\1 Z", s, flags=re.I)
            s = re.sub(rf"({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})", r"\1 \2 \3", s)
        elif dim == "M":
            s = re.sub(rf"\b({TYPE_RE})\s+M\b", r"\1", s, flags=re.I)
            s = re.sub(rf"({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})", r"\1 \2", s)
        return s

    def _clip_tail(s: str) -> str:
        """WKTdən sonrakı zibili kəs (məs: 'POLYGON((...))49.80…' → 'POLYGON((...))')."""
        if s.upper().startswith("SRID=") and ";" in s:
            s = s.split(";", 1)[1].strip()
        depth = 0
        end = -1
        for i, ch in enumerate(s):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        return s[: end + 1].strip() if end >= 0 else s.strip()

    # Input sanitizasiya
    safe_wkts, bad_empty, bad_curved, bad_parse = [], 0, 0, 0
    for w in wkt_list:
        s = _clean_wkt_text(w or "")
        if not s:
            bad_empty += 1
            continue
        if re.search(r"\b(CURVEPOLYGON|CIRCULARSTRING|COMPOUNDCURVE|ELLIPTICARC|MULTICURVE|MULTISURFACE|GEOMETRYCOLLECTION)\b", s, flags=re.I):
            bad_curved += 1
            continue
        s = _clip_tail(_normalize_wkt_remove_m_dims(s))
        try:
            g = _wkt.loads(s)
            if g.is_empty:
                bad_empty += 1
                continue
            s = g.wkt  # kanonik 2D WKT
        except Exception:
            bad_parse += 1
            continue
        safe_wkts.append(s)

    if not safe_wkts:
        print(
            f"[TEKUIS][GEOM][input_sanitize] all invalid. empty={bad_empty}, curved={bad_curved}, parse={bad_parse}"
        )
        return JsonResponse({"type": "FeatureCollection", "features": []}, safe=False)

    # SRID auto-detekt: dərəcə aralığındadırsa 4326
    def _infer_srid(wkts: list[str], fallback: int) -> int:
        try:
            g0 = _wkt.loads(wkts[0])
            minx, miny, maxx, maxy = g0.bounds
            if (-180 <= minx <= 180 and -180 <= maxx <= 180 and -90 <= miny <= 90 and -90 <= maxy <= 90):
                return 4326
        except Exception:
            pass
        return fallback

    srid_in = _infer_srid(safe_wkts, srid_in_payload)

    # SQL generator (WKT yolu)
    def _make_sql_wkt(n_items: int) -> tuple[str, dict]:
        bind_names = [f"w{i}" for i in range(n_items)]
        g_raw_sql = " \nUNION ALL\n".join([f"  SELECT :{bn} AS wkt FROM dual" for bn in bind_names])
        sql = f"""
            WITH g_raw AS (
{g_raw_sql}
            ),
            g AS (
                SELECT CASE WHEN :bufm > 0 THEN
                    sde.st_transform(
                        sde.st_buffer(
                            sde.st_transform(sde.st_geomfromtext(wkt, :srid_in), 3857), :bufm
                        ),
                        :table_srid
                    )
                ELSE
                    sde.st_transform(sde.st_geomfromtext(wkt, :srid_in), :table_srid)
                END AS geom
                FROM g_raw
            ),
            ids AS (
                SELECT DISTINCT t.ROWID AS rid
                  FROM {schema}.{table} t, g
                 WHERE sde.st_envintersects(t.SHAPE, g.geom) = 1
                   AND sde.st_intersects(t.SHAPE, g.geom) = 1
            )
            SELECT t.ROWID AS rid,
                   sde.st_astext(t.SHAPE) AS wkt,
                   {attrs_sql}
              FROM {schema}.{table} t
              JOIN ids ON t.ROWID = ids.rid
        """
        params = {bn: safe_wkts[i] for i, bn in enumerate(bind_names)}
        params.update({"srid_in": int(srid_in), "bufm": float(buf_m), "table_srid": int(table_srid)})
        return sql, params

    # Per-row fallback (WKB yolu)
    def _make_sql_wkb() -> str:
        return f"""
            WITH g AS (
                SELECT CASE WHEN :bufm > 0 THEN
                    sde.st_transform(
                        sde.st_buffer(
                            sde.st_transform(sde.st_geomfromwkb(hextoraw(:wkb), :srid_in), 3857), :bufm
                        ),
                        :table_srid
                    )
                ELSE
                    sde.st_transform(sde.st_geomfromwkb(hextoraw(:wkb), :srid_in), :table_srid)
                END AS geom
                FROM dual
            ),
            ids AS (
                SELECT DISTINCT t.ROWID AS rid
                  FROM {schema}.{table} t, g
                 WHERE sde.st_envintersects(t.SHAPE, g.geom) = 1
                   AND sde.st_intersects(t.SHAPE, g.geom) = 1
            )
            SELECT t.ROWID AS rid,
                   sde.st_astext(t.SHAPE) AS wkt,
                   {attrs_sql}
              FROM {schema}.{table} t
              JOIN ids ON t.ROWID = ids.rid
        """

    features, seen_rids = [], set()
    out_skip_empty = out_skip_parse = out_skip_curved = out_tailfix = 0

    def _consume_cursor(cur):
        nonlocal out_skip_empty, out_skip_curved, out_skip_parse, out_tailfix
        for row in cur:
            # rid, wkt_lob, attr1, attr2, ...
            rid, wkt_lob, *attr_vals = row
            rid_key = str(rid) if rid is not None else None
            if rid_key and rid_key in seen_rids:
                continue

            raw = wkt_lob.read() if hasattr(wkt_lob, "read") else wkt_lob
            w = _clean_wkt_text(raw)
            if not w:
                out_skip_empty += 1
                continue
            if re.search(r"\b(CURVEPOLYGON|CIRCULARSTRING|COMPOUNDCURVE|ELLIPTICARC|MULTICURVE|MULTISURFACE)\b", w, flags=re.I):
                out_skip_curved += 1
                continue

            # tail kəs + M/ZM → 2D
            w2 = _clip_tail(w)
            if w2 != w:
                out_tailfix += 1
            m_hdr = re.match(rf"^\s*(?:{TYPE_RE})\s+(ZM|M)\b", w2, flags=re.I)
            if m_hdr:
                dim = m_hdr.group(1).upper()
                if dim == "ZM":
                    w2 = re.sub(rf"\b({TYPE_RE})\s+ZM\b", r"\1 Z", w2, flags=re.I)
                    w2 = re.sub(rf"({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})", r"\1 \2 \3", w2)
                else:
                    w2 = re.sub(rf"\b({TYPE_RE})\s+M\b", r"\1", w2, flags=re.I)
                    w2 = re.sub(rf"({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})", r"\1 \2", w2)
            try:
                geom = _wkt.loads(w2)
            except Exception:
                out_skip_parse += 1
                continue

            props = _tekuis_props_from_row(attr_vals)
            props["SOURCE"] = "TEKUIS"

            features.append({"type": "Feature", "geometry": mapping(geom), "properties": props})
            if rid_key:
                seen_rids.add(rid_key)

    with _oracle_connect() as cn:
        with cn.cursor() as cur:
            CHUNK = 200
            for start in range(0, len(safe_wkts), CHUNK):
                sub = safe_wkts[start : start + CHUNK]
                sql_wkt, params = _make_sql_wkt(len(sub))
                try:
                    try:
                        cur.setinputsizes(**{k: oracledb.DB_TYPE_CLOB for k in params if k.startswith("w")})
                    except Exception:
                        pass
                    cur.execute(sql_wkt, params)
                    _consume_cursor(cur)
                except oracledb.DatabaseError:
                    # Zəhərli WKT varsa — tək-tək yoxla; əvvəl WKT, sonra WKB fallback
                    for w in sub:
                        ok = False
                        try:
                            cur.execute(
                                f"""
                                WITH g AS (
                                    SELECT CASE WHEN :bufm > 0 THEN
                                        sde.st_transform(
                                            sde.st_buffer(
                                                sde.st_transform(sde.st_geomfromtext(:w, :srid_in), 3857), :bufm
                                            ),
                                            :table_srid
                                        )
                                    ELSE
                                        sde.st_transform(sde.st_geomfromtext(:w, :srid_in), :table_srid)
                                    END AS geom
                                    FROM dual
                                ),
                                ids AS (
                                    SELECT DISTINCT t.ROWID AS rid
                                      FROM {schema}.{table} t, g
                                     WHERE sde.st_envintersects(t.SHAPE, g.geom) = 1
                                       AND sde.st_intersects(t.SHAPE, g.geom) = 1
                                )
                                SELECT t.ROWID AS rid,
                                       sde.st_astext(t.SHAPE) AS wkt,
                                       {attrs_sql}
                                  FROM {schema}.{table} t
                                  JOIN ids ON t.ROWID = ids.rid
                                """,
                                {"w": w, "srid_in": int(srid_in), "bufm": float(buf_m), "table_srid": int(table_srid)},
                            )
                            _consume_cursor(cur)
                            ok = True
                        except Exception:
                            # WKB fallback
                            try:
                                g = _wkt.loads(w)  # artıq 2D və validdir
                                wkb_hex = _wkb.dumps(g, hex=True)  # 2D WKB (Shapely 2-də default 2D-dir)
                                cur.execute(
                                    _make_sql_wkb(),
                                    {"wkb": wkb_hex, "srid_in": int(srid_in), "bufm": float(buf_m), "table_srid": int(table_srid)},
                                )
                                _consume_cursor(cur)
                                ok = True
                            except Exception as e2:
                                head = (w[:220] + "…") if len(w) > 220 else w
                                print(
                                    "[TEKUIS][GEOM] skipped one WKT due to SDE error.\n"
                                    f"WKT head: {head}\nWKB fallback err: {str(e2)[:240]}"
                                )
                        if not ok:
                            continue

    print(
        f"[TEKUIS][GEOM] input_sanitized={len(safe_wkts)} dropped={bad_empty+bad_curved+bad_parse} "
        f"(empty={bad_empty}, curved={bad_curved}, parse={bad_parse}) srid_in={srid_in} table_srid={table_srid} buf_m={buf_m}"
    )
    print(
        f"[TEKUIS][GEOM] returned={len(features)} unique_rids={len(seen_rids)} "
        f"skipped_out={out_skip_empty+out_skip_curved+out_skip_parse} tailfix={out_tailfix}"
    )

    return JsonResponse({"type": "FeatureCollection", "features": features}, safe=False)


# --- YENİ: attach-lardan WKT toplamaq üçün köməkçi ---

def _collect_attach_wkts_for_meta(meta_id: int, req_crs: str = "auto") -> List[str]:
    """
    Verilmiş meta_id üçün aktiv attach fayllarını oxuyur,
    onların içindəki bütün geometriyaları ayrı-ayrılıqda WKT kimi qaytarır.
    """
    wkts: List[str] = []

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT attach_id, attach_name, coordinate_system
            FROM attach_file
            WHERE meta_id = %s
              AND COALESCE(status,1) = 1
            ORDER BY attach_id
        """,
            [meta_id],
        )
        rows = cur.fetchall()

    try:
        _smb_net_use()
    except Exception:
        pass

    for aid, name, coord_label in rows:
        p = _find_attach_file(meta_id, name)
        if not p:
            continue
        ext = p.suffix.lower()

        # Attach-ı GeoJSON-a çevir
        if ext == ".zip":
            fc = _geojson_from_zip_file(p)
        elif ext in {".csv", ".txt"}:
            db_code = _canonize_crs_value(coord_label) if coord_label else None
            choice = db_code or req_crs or "auto"
            fc = _geojson_from_csvtxt_file(p, crs_choice=choice)
        else:
            continue

        # Hər featurenin geometriyasını WKT kimi əlavə et
        for ftr in fc.get("features", []):
            try:
                g = shapely_shape(ftr.get("geometry"))
                for gg in _flatten_geoms(g):
                    if not gg.is_empty:
                        wkts.append(gg.wkt)
            except Exception:
                continue

    # Dublikatları bir az azaldaq (eyni nöqtə/geom təkrarı ola bilər)
    # Sadə yol: string set
    wkts = list(dict.fromkeys(wkts))
    return wkts


# --- YENİ: TEKUIS cavabını WKT-lərdən yığan köməkçi ---

def _tekuis_features_from_wkts(wkt_list: List[str], srid: int, buf_m: float, limit: Optional[int] = None) -> List[dict]:
    """
    Verilən WKT siyahısı əsasında Oracle/TEKUIS-dən parselləri çəkir.
    Shapely üçün WKT-lərdə M/ZM ölçüsünü normallaşdırır və mümkün "tail"ları kəsir.
    """
    import re

    features: List[dict] = []
    seen_rids = set()

    # Statistik sayğaclar
    skipped_empty = 0
    skipped_curved = 0
    skipped_parse = 0
    logged_parse_examples = 0

    # M/ZM ölçüsünü təmizləyən regex-lər
    NUM_RE = r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?"
    TYPE_RE = r"(?:POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON)"

    def _normalize_wkt_remove_m_dims(w: str) -> str:
        s = w
        m_hdr = re.match(rf"^\s*(?:{TYPE_RE})\s+(ZM|M)\b", s, flags=re.I)
        if not m_hdr:
            return s
        dim = m_hdr.group(1).upper()
        if dim == "ZM":
            s = re.sub(rf"\b({TYPE_RE})\s+ZM\b", r"\1 Z", s, flags=re.I)
            s = re.sub(rf"({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})", r"\1 \2 \3", s)
        elif dim == "M":
            s = re.sub(rf"\b({TYPE_RE})\s+M\b", r"\1", s, flags=re.I)
            s = re.sub(rf"({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})", r"\1 \2", s)
        return s

    # WKT bitdikdən sonrakı “tail”i kəsən funksiya
    def _clip_to_first_geometry(w: str) -> str:
        lvl = 0
        last = -1
        for i, ch in enumerate(w):
            if ch == "(":
                lvl += 1
            elif ch == ")":
                lvl -= 1
                if lvl == 0:
                    last = i
                    break
        return w[: last + 1] if last >= 0 else w

    schema = os.getenv("TEKUIS_SCHEMA", "BTG_MIS")
    table = os.getenv("TEKUIS_TABLE", "M_G_PARSEL")
    max_features = int(os.getenv("TEKUIS_MAX_FEATURES", "20000"))
    row_limit = int(limit or max_features)

    with _oracle_connect() as cn:
        with cn.cursor() as cur:
            CHUNK = 200
            for start in range(0, len(wkt_list), CHUNK):
                if row_limit is not None and row_limit <= 0:
                    break

                chunk = wkt_list[start : start + CHUNK]
                bind_names = [f"w{i}" for i in range(len(chunk))]
                try:
                    cur.setinputsizes(**{bn: oracledb.DB_TYPE_CLOB for bn in bind_names})
                except Exception:
                    pass

                g_raw_sql = " \nUNION ALL\n".join([f"  SELECT :{bn} AS wkt FROM dual" for bn in bind_names])

                sql = f"""
                    WITH g_raw AS (
{g_raw_sql}
                    ),
                    g AS (
                        SELECT CASE WHEN :bufm > 0 THEN
                            sde.st_transform(
                                sde.st_buffer(
                                    sde.st_transform(sde.st_geomfromtext(wkt, :srid), 3857), :bufm
                                ),
                            4326)
                        ELSE sde.st_geomfromtext(wkt, :srid) END AS geom
                        FROM g_raw
                    ),
                    ids AS (
                        SELECT DISTINCT t.ROWID AS rid
                          FROM {schema}.{table} t, g
                         WHERE sde.st_envintersects(t.SHAPE, g.geom) = 1
                           AND sde.st_intersects(t.SHAPE, g.geom) = 1
                    ),
                    lim AS (
                        SELECT rid FROM ids WHERE ROWNUM <= :row_limit
                    )
                    SELECT t.ROWID AS rid, sde.st_astext(t.SHAPE) AS wkt
                      FROM {schema}.{table} t
                      JOIN lim ON t.ROWID = lim.rid
                """

                params = {bn: w for bn, w in zip(bind_names, chunk)}
                params.update({"srid": int(srid), "bufm": float(buf_m), "row_limit": int(row_limit)})

                try:
                    cur.execute(sql, params)
                except Exception:
                    # Ehtiyat plan (envintersects olmadan)
                    cur.execute(
                        f"""
                        WITH g_raw AS (
{g_raw_sql}
                        ),
                        g AS (
                            SELECT CASE WHEN :bufm > 0 THEN
                                sde.st_transform(
                                    sde.st_buffer(
                                        sde.st_transform(sde.st_geomfromtext(wkt, :srid), 3857), :bufm
                                    ),
                                4326)
                            ELSE sde.st_geomfromtext(wkt, :srid) END AS geom
                            FROM g_raw
                        ),
                        ids AS (
                            SELECT DISTINCT t.ROWID AS rid
                              FROM {schema}.{table} t, g
                             WHERE sde.st_intersects(t.SHAPE, g.geom) = 1
                        ),
                        lim AS (
                            SELECT rid FROM ids WHERE ROWNUM <= :row_limit
                        )
                        SELECT t.ROWID AS rid, sde.st_astext(t.SHAPE) AS wkt
                          FROM {schema}.{table} t
                          JOIN lim ON t.ROWID = lim.rid
                        """,
                        params,
                    )

                for rid, wkt_lob in cur:
                    rid_key = str(rid) if rid is not None else None
                    if rid_key and rid_key in seen_rids:
                        continue

                    raw = wkt_lob.read() if hasattr(wkt_lob, "read") else wkt_lob
                    w = _clean_wkt_text(raw)
                    if not w:
                        skipped_empty += 1
                        continue
                    if re.search(
                        r"\b(CURVEPOLYGON|CIRCULARSTRING|COMPOUNDCURVE|ELLIPTICARC|MULTICURVE|MULTISURFACE)\b",
                        w,
                        flags=re.I,
                    ):
                        skipped_curved += 1
                        continue

                    # Tail kəs + M/ZM normallaşdır
                    w2 = _normalize_wkt_remove_m_dims(_clip_to_first_geometry(w))

                    try:
                        geom = shapely_wkt.loads(w2)
                    except Exception as e:
                        skipped_parse += 1
                        if logged_parse_examples < 3:
                            head = (w[:280] + "…") if len(w) > 280 else w
                            print(f"[TEKUIS][ATTACH][parse_error] sample WKT head:\n{head}\n---\n{e}\n")
                            logged_parse_examples += 1
                        continue

                    features.append({"type": "Feature", "geometry": mapping(geom), "properties": {}})
                    if rid_key:
                        seen_rids.add(rid_key)

                    if row_limit is not None:
                        row_limit -= 1
                        if row_limit <= 0:
                            break

    skipped_total = skipped_empty + skipped_curved + skipped_parse
    print(
        f"[TEKUIS][ATTACH] returned={len(features)} unique_rids={len(seen_rids)} "
        f"skipped_total={skipped_total} (empty={skipped_empty}, curved={skipped_curved}, parse={skipped_parse}) "
        f"srid={srid} buf_m={buf_m}"
    )
    return features


@require_GET
def tekuis_parcels_by_attach_ticket(request):
    """
    GET parametrlər:
      - ticket: məcburi
      - srid:   opsional (default .env TEKUIS_SRID və ya 4326)
      - buffer_m (və ya buf): opsional, nöqtələr üçün axtarış radiusu (metr)
      - limit:  opsional, qaytarılacaq maksimum parsel sayı (default .env TEKUIS_MAX_FEATURES)
    """
    ticket = (request.GET.get("ticket") or "").strip()
    if not ticket:
        return HttpResponseBadRequest("ticket tələb olunur.")

    srid = int(request.GET.get("srid") or os.getenv("TEKUIS_SRID", 4326))
    buf_m = float(request.GET.get("buffer_m") or request.GET.get("buf") or 5.0)
    limit = request.GET.get("limit")
    limit = int(limit) if (limit is not None and str(limit).strip().isdigit()) else None

    meta_id = _redeem_ticket(ticket)
    if meta_id is None:
        return _unauthorized()

    # Attach-lardan bütün geometriyaları topla
    wkt_list = _collect_attach_wkts_for_meta(meta_id, req_crs="auto")
    if not wkt_list:
        print(f"[TEKUIS][ATTACH] no geometries found for meta_id={meta_id}")
        return JsonResponse({"type": "FeatureCollection", "features": []}, safe=False)

    # TEKUIS parsellərini çək
    features = _tekuis_features_from_wkts(wkt_list, srid=srid, buf_m=buf_m, limit=limit)
    return JsonResponse({"type": "FeatureCollection", "features": features}, safe=False)


def _prop_ci(props: dict, key: str):
    """Case-insensitive + alt xəttsiz lookup."""
    if not props:
        return None
    # birbaşa
    if key in props:
        return props.get(key)
    # üst/alt
    up = key.upper()
    lo = key.lower()
    for k, v in props.items():
        kk = str(k)
        if kk == key or kk.upper() == up or kk.lower() == lo:
            return v
    # alt_xett / boşluq tolerantlığı
    knorm = "".join(ch for ch in key.lower() if ch.isalnum())
    for k, v in props.items():
        kn = "".join(ch for ch in str(k).lower() if ch.isalnum())
        if kn == knorm:
            return v
    return None


def _to_float_or_none(v):
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        return float(str(v).replace(",", "."))
    except Exception:
        return None


def _guess_tekuis_id(props: dict):
    """Mümkün ID-lərdən birini götürür."""
    cand_keys = ["tekuis_id", "TEKUIS_ID", "ID", "OBJECTID", "rid", "RID"]
    for k in cand_keys:
        val = _prop_ci(props, k)
        if val is None or str(val).strip() == "":
            continue
        try:
            return int(str(val).strip())
        except Exception:
            # bəzən ROWID string ola bilər → skip
            pass
    return None


def _insert_tekuis_parcel_rows(
    meta_id: int,
    ticket: str,
    features: list,
    replace: bool = True,
    user_id: Optional[int] = None,
    user_full_name: Optional[str] = None,
):
    saved = 0
    skipped = 0
    deactivated = 0

    with transaction.atomic():
        with connection.cursor() as cur:
            if replace:
                cur.execute(
                    """
                    UPDATE tekuis_parcel
                       SET status = 0,
                           last_edited_date = now()
                     WHERE meta_id = %s
                       AND COALESCE(status,1) = 1
                """,
                    [meta_id],
                )
                deactivated = cur.rowcount or 0

            for f in features or []:
                geom = f.get("geometry") or {}
                gtype = (geom.get("type") or "").lower()
                if "polygon" not in gtype:  # yalnız (Multi)Polygon saxlayırıq
                    skipped += 1
                    continue

                props = f.get("properties") or {}
                tekuis_db_id = _guess_tekuis_id(props)
                colvals = {
                    "kateqoriya": _prop_ci(props, "LAND_CATEGORY_ENUM"),
                    "uqodiya": _prop_ci(props, "LAND_CATEGORY2ENUM"),
                    "alt_kateqoriya": _prop_ci(props, "LAND_CATEGORY3ENUM"),
                    "alt_uqodiya": _prop_ci(props, "LAND_CATEGORY4ENUM"),
                    "islahat_uqodiyasi": _prop_ci(props, "OLD_LAND_CATEGORY2ENUM"),
                    "mulkiyyet": _prop_ci(props, "OWNER_TYPE_ENUM"),
                    "suvarma": _prop_ci(props, "SUVARILMA_NOVU_ENUM"),
                    "emlak_novu": _prop_ci(props, "EMLAK_NOVU_ENUM"),
                    "rayon_adi": _prop_ci(props, "RAYON_ADI"),
                    "ied_adi": _prop_ci(props, "IED_ADI"),
                    "belediyye_adi": _prop_ci(props, "BELEDIYE_ADI"),
                    "sahe_ha": _to_float_or_none(_prop_ci(props, "AREA_HA")),
                    "qeyd": _prop_ci(props, "NAME"),
                    "tekuis_db_id": tekuis_db_id,
                }

                geom_json = json.dumps(geom)

                cur.execute(
                    """
                    INSERT INTO tekuis_parcel (
                        kateqoriya, uqodiya, alt_kateqoriya, alt_uqodiya,
                        mulkiyyet, suvarma, emlak_novu, islahat_uqodiyasi,
                        rayon_adi, ied_adi, belediyye_adi,
                        sahe_ha, qeyd, tekuis_db_id, geom,
                        meta_id, created_date, last_edited_date, status,
                        user_id, user_full_name
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        ST_Multi( ST_Buffer( ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 0) ),
                        %s, now(), now(), 1,
                        %s, %s
                    )
                    RETURNING tekuis_id
                """,
                    [
                        colvals["kateqoriya"],
                        colvals["uqodiya"],
                        colvals["alt_kateqoriya"],
                        colvals["alt_uqodiya"],
                        colvals["mulkiyyet"],
                        colvals["suvarma"],
                        colvals["emlak_novu"],
                        colvals["islahat_uqodiyasi"],
                        colvals["rayon_adi"],
                        colvals["ied_adi"],
                        colvals["belediyye_adi"],
                        colvals["sahe_ha"],
                        colvals["qeyd"],
                        colvals["tekuis_db_id"],
                        geom_json,
                        int(meta_id),
                        user_id,
                        user_full_name,
                    ],
                )

                _ = cur.fetchone()[0]  # lazım olsa istifadə et
                saved += 1

    return {"saved": saved, "skipped": skipped, "deactivated": deactivated}


def _json_body(request):
    try:
        raw = request.body.decode("utf-8") if request.body else ""
        return json.loads(raw) if raw else {}
    except Exception:
        return {}


def _meta_id_from_request(request):
    """
    Hər yerdə eyni meta_id qaydasını təmin et:
    1) X-Meta-Id / ?meta_id / body.meta_id varsa – onu götür
    2) Yoxsa ticket (X-Ticket | ?ticket | body.ticket) CRC32 -> int
    """
    # 1) Explicit meta id
    meta_hdr = request.headers.get("X-Meta-Id")
    meta_qs = request.GET.get("meta_id")
    meta_bd = _json_body(request).get("meta_id")
    for m in (meta_hdr, meta_qs, meta_bd):
        if m is not None:
            try:
                return int(m)
            except Exception:
                pass

    # 2) Ticket-dən türet
    body = _json_body(request)
    ticket = (
        request.headers.get("X-Ticket")
        or request.GET.get("ticket")
        or request.POST.get("ticket")
        or body.get("ticket")
        or ""
    )
    ticket = str(ticket).strip()
    return int(zlib.crc32(ticket.encode("utf-8")) & 0x7FFFFFFF)


@csrf_exempt
def validate_tekuis_parcels(request):
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "POST only"}, status=405)

    try:
        data = json.loads(request.body or "{}")
    except Exception:
        data = {}

    gj = data.get("geojson") or (data if "features" in data else None)
    if not gj:
        return JsonResponse({"ok": False, "error": "geojson is required"}, status=422)

    # ⬅️ meta_id-ni həmişə eyni qaydada götür (CRC32 və ya header/query)
    meta_id = _meta_id_from_request(request)

    min_overlap = data.get("min_overlap_sqm")
    min_gap = data.get("min_gap_sqm")

    # Dissolve olunmuş kimi görünürmü? (tək Polygon/MultiPolygon gəlibsə)
    feats = (gj or {}).get("features", [])
    looks_dissolved = len(feats) == 1 and ((feats[0].get("geometry") or {}).get("type") in ("Polygon", "MultiPolygon"))

    res = validate_tekuis(gj, meta_id, min_overlap_sqm=min_overlap, min_gap_sqm=min_gap)

    out = {"ok": True, "validation": res}
    if looks_dissolved:
        out["warning"] = "features_look_dissolved"  # Fronta göstərə bilərsən
    return JsonResponse(out)


@csrf_exempt
def ignore_tekuis_gap(request):
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "POST only"}, status=405)

    data = _json_body(request)
    meta_id = _meta_id_from_request(request)
    h = data.get("hash")
    geom = data.get("geom")
    if not h:
        return JsonResponse({"ok": False, "error": "hash required"}, status=400)

    ok = ignore_gap(meta_id, h, geom)
    return JsonResponse({"ok": bool(ok)}, status=200 if ok else 500)


@csrf_exempt
@require_valid_ticket
def tekuis_validate_view(request):
    if request.method != "POST":
        return HttpResponseBadRequest("POST gözlənirdi.")
    try:
        payload = getattr(request, "_json_cached", None) or json.loads(request.body.decode("utf-8"))
    except Exception:
        return HttpResponseBadRequest("Yanlış JSON.")

    geojson = payload.get("geojson")
    if not geojson:
        return HttpResponseBadRequest("geojson tələb olunur.")

    meta_id = payload.get("meta_id")
    if meta_id is None:
        meta_id = getattr(request, "fk_metadata", None)
    if meta_id is None:
        return JsonResponse({"ok": False, "error": "meta_id yoxdur"}, status=400)

    # ayarlardan hədləri götür
    min_ov = float(
        getattr(settings, "TEKUIS_VALIDATION_MIN_OVERLAP_SQM", getattr(settings, "TEKUIS_VALIDATION_MIN_AREA_SQM", 1.0))
    )
    min_ga = float(
        getattr(settings, "TEKUIS_VALIDATION_MIN_GAP_SQM", getattr(settings, "TEKUIS_VALIDATION_MIN_AREA_SQM", 1.0))
    )

    res = validate_tekuis(geojson, int(meta_id), min_overlap_sqm=min_ov, min_gap_sqm=min_ga)

    has_err = res.get("stats", {}).get("overlap_count", 0) > 0 or res.get("stats", {}).get("gap_count", 0) > 0

    status = 422 if has_err else 200
    return JsonResponse({"ok": not has_err, "validation": res}, status=status)


@csrf_exempt
@require_valid_ticket
def tekuis_validate_ignore_gap_view(request):
    if request.method != "POST":
        return HttpResponseBadRequest("POST gözlənirdi.")
    try:
        payload = getattr(request, "_json_cached", None) or json.loads(request.body.decode("utf-8"))
    except Exception:
        return HttpResponseBadRequest("Yanlış JSON.")

    meta_id = payload.get("meta_id")
    if meta_id is None:
        meta_id = getattr(request, "fk_metadata", None)
    h = (payload.get("hash") or "").strip()
    if not (meta_id and h):
        return HttpResponseBadRequest("meta_id və hash tələb olunur.")

    ok = ignore_gap(int(meta_id), h, geom_geojson=payload.get("geom"))
    return JsonResponse({"ok": bool(ok)})


@csrf_exempt
@require_valid_ticket
def save_tekuis_parcels(request):
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "POST only"}, status=405)

    data = _json_body(request)
    fc = data.get("geojson") or {}
    if not isinstance(fc, dict) or fc.get("type") != "FeatureCollection":
        return JsonResponse({"ok": False, "error": "geojson FeatureCollection tələb olunur"}, status=400)

    features = fc.get("features") or []
    if not features:
        return JsonResponse({"ok": False, "error": "Boş FeatureCollection"}, status=400)

    # --- HƏMİŞƏ REAL FK METADATA ---
    meta_id = getattr(request, "fk_metadata", None)
    if meta_id is None:
        return JsonResponse({"ok": False, "error": "unauthorized"}, status=401)

    # Body-də meta_id verilirsə, uyğunsuzluq olmasın
    body_meta = data.get("meta_id")
    if body_meta is not None and int(body_meta) != int(meta_id):
        return JsonResponse({"ok": False, "error": "meta_id mismatch"}, status=409)

    # Ticket lazımdır (log/trace və _insert_tekuis_parcel_rows üçün)
    ticket = (
        request.headers.get("X-Ticket")
        or request.GET.get("ticket")
        or request.POST.get("ticket")
        or data.get("ticket")
        or ""
    ).strip()
    if not ticket:
        return JsonResponse({"ok": False, "error": "ticket tələb olunur"}, status=400)

    # === NEW: İKİNCİ DƏFƏ SAXLAMAYA QADAĞA (aktiv sətirlər varsa) ===
    if _has_active_tekuis(int(meta_id)):
        return JsonResponse(
            {"ok": False, "code": "ALREADY_SAVED", "message": "TEKUİS parsellər local bazada yadda saxlanılıb"},
            status=409,
        )

    skip_validation = bool(data.get("skip_validation", False))
    if not skip_validation:
        min_ov = float(
            getattr(settings, "TEKUIS_VALIDATION_MIN_OVERLAP_SQM", getattr(settings, "TEKUIS_VALIDATION_MIN_AREA_SQM", 1.0))
        )
        min_ga = float(
            getattr(settings, "TEKUIS_VALIDATION_MIN_GAP_SQM", getattr(settings, "TEKUIS_VALIDATION_MIN_AREA_SQM", 1.0))
        )

        v = validate_tekuis(fc, int(meta_id), min_overlap_sqm=min_ov, min_gap_sqm=min_ga)

        # ignored-ları müxtəlif formatlarda dəstəklə
        def _collect_ignored_keys(payload_ignored: dict):
            ov = (
                payload_ignored.get("overlap_keys")
                or payload_ignored.get("overlaps")
                or payload_ignored.get("overlap_hashes")
                or payload_ignored.get("ignored_overlap_keys")
                or []
            )
            gp = (
                payload_ignored.get("gap_keys")
                or payload_ignored.get("gaps")
                or payload_ignored.get("gap_hashes")
                or payload_ignored.get("ignored_gap_keys")
                or []
            )
            return set(map(str, ov)), set(map(str, gp))

        ignored = data.get("ignored") or {}
        ignored_overlap_keys, ignored_gap_keys = _collect_ignored_keys(ignored)

        def _eff_key(obj):
            if isinstance(obj, dict) and (obj.get("key") or obj.get("hash")):
                return str(obj.get("key") or obj.get("hash"))
            return _topo_key_py(obj)

        effective_overlaps = [o for o in (v.get("overlaps") or []) if _eff_key(o) not in ignored_overlap_keys]
        effective_gaps = [g for g in (v.get("gaps") or []) if _eff_key(g) not in ignored_gap_keys]

        if effective_overlaps or effective_gaps:
            return JsonResponse({"ok": False, "validation": v}, status=422)

    try:
        uid = getattr(request, "user_id_from_token", None)
        ufn = getattr(request, "user_full_name_from_token", None)

        res = _insert_tekuis_parcel_rows(
            meta_id=int(meta_id),
            ticket=ticket,
            features=features,
            replace=bool(data.get("replace", True)),
            user_id=uid,
            user_full_name=ufn,
        )

        return JsonResponse(
            {
                "ok": True,
                "meta_id": int(meta_id),
                "ticket": ticket,
                "saved_count": res["saved"],
                "skipped_non_polygon": res["skipped"],
                "deactivated_old": res["deactivated"],
            },
            status=200,
        )
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)


def _topo_key_py(obj):
    """
    JavaScript topoKey() funksiyasının server tərəfi ekvivalenti.
    Əgər obyektin içində 'key' və ya 'hash' varsa, birbaşa onu istifadə edirik ki,
    Frontend-lə eyni identifikator alınsın. Əks halda geometriyadan açar yaradırıq.
    """
    import hashlib
    import random

    try:
        if isinstance(obj, dict):
            k = obj.get("key") or obj.get("hash")
            if isinstance(k, str) and k:
                return k
            g = obj.get("geom")
        else:
            g = obj
        norm = json.dumps(_round_deep_py(g, 6), sort_keys=True)
        return "k" + hashlib.md5(norm.encode()).hexdigest()[:12]
    except Exception:
        return "k" + "".join(random.choices("0123456789abcdef", k=12))


def _round_deep_py(x, d=6):
    """Rekursiv olaraq ədədləri yuvarlaqlaqlaşdır"""
    if isinstance(x, list):
        return [_round_deep_py(v, d) for v in x]
    if isinstance(x, float):
        return round(x, d)
    if isinstance(x, dict):
        return {k: _round_deep_py(v, d) for k, v in sorted(x.items())}
    return x


__all__ = [
    "_has_active_tekuis",
    "ignore_tekuis_gap",
    "save_tekuis_parcels",
    "tekuis_parcels_by_attach_ticket",
    "tekuis_parcels_by_bbox",
    "tekuis_parcels_by_geom",
    "tekuis_validate_ignore_gap_view",
    "tekuis_validate_view",
    "validate_tekuis_parcels",
]