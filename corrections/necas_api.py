# necas_api.py
# -*- coding: utf-8 -*-

import os, json, re
import oracledb
from django.http import JsonResponse, HttpResponseBadRequest
from django.views.decorators.http import require_GET, require_POST
from django.views.decorators.csrf import csrf_exempt
from shapely import wkt as _wkt
from shapely import wkb as _wkb
from shapely.geometry import mapping
import logging

logger = logging.getLogger(__name__)

# ---------------------------
# Env helper-lər
# ---------------------------
ENV = os.environ.get

NECAS_HOST   = ENV("NECAS_ORA_HOST")
NECAS_PORT   = int(ENV("NECAS_ORA_PORT", "1521"))
NECAS_SVC    = ENV("NECAS_ORA_SERVICE")
NECAS_USER   = ENV("NECAS_ORA_USER")
NECAS_PASS   = ENV("NECAS_ORA_PASSWORD")

NECAS_SCHEMA = ENV("NECAS_SCHEMA", "NECASMAPUSER")
NECAS_TABLE  = ENV("NECAS_TABLE",  "PARCEL")
NECAS_SRID   = int(ENV("NECAS_SRID", "4326"))

ISDEL_PRED = ENV("NECAS_ISDEL_PRED", "NVL(p.IS_DELETE,0)=0")

# ---------------------------
# Oracle pool
# ---------------------------
_POOL = None
def get_pool():
    global _POOL
    if _POOL is None:
        dsn = oracledb.makedsn(NECAS_HOST, NECAS_PORT, service_name=NECAS_SVC)
        _POOL = oracledb.create_pool(
            user=NECAS_USER,
            password=NECAS_PASS,
            dsn=dsn,
            min=1, max=4, increment=1
        )
    return _POOL

def ql_table():
    return f'{NECAS_SCHEMA}.{NECAS_TABLE}'

# ---------------------------
# NECAS atributları
# ---------------------------
NECAS_ATTRS = ("CADASTER_NUMBER", "KATEQORIYA", "UQODIYA")
ATTR_SQL = ", ".join([f"p.{c}" for c in NECAS_ATTRS])

def _props_from_vals(vals, rid=None):
    props = {k: v for k, v in zip(NECAS_ATTRS, vals)}
    props["SOURCE"] = "NECAS"
    if rid:
        props["RID"] = rid
    return props

# ---------------------------
# WKT Helper functions (TEKUIS-dən götürülüb)
# ---------------------------
NUM_RE  = r'[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?'
TYPE_RE = r'(?:POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)'

def _clean_wkt_text(w):
    """WKT mətnini təmizlə"""
    if not w:
        return ""
    return re.sub(r'\s+', ' ', str(w).strip())

def _normalize_wkt_remove_m_dims(w: str) -> str:
    """ZM/M dimensiyalarını 2D-yə çevir"""
    s = w
    m_hdr = re.match(rf'^\s*(?:{TYPE_RE})\s+(ZM|M)\b', s, flags=re.I)
    if not m_hdr:
        return s
    dim = m_hdr.group(1).upper()
    if dim == 'ZM':
        s = re.sub(rf'\b({TYPE_RE})\s+ZM\b', r'\1 Z', s, flags=re.I)
        s = re.sub(rf'({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})', r'\1 \2 \3', s)
    elif dim == 'M':
        s = re.sub(rf'\b({TYPE_RE})\s+M\b', r'\1', s, flags=re.I)
        s = re.sub(rf'({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})', r'\1 \2', s)
    return s

def _clip_tail(s: str) -> str:
    """WKT-dən sonrakı zibili kəs"""
    if s.upper().startswith("SRID=") and ";" in s:
        s = s.split(";", 1)[1].strip()
    depth = 0
    end = -1
    for i, ch in enumerate(s):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth == 0:
                end = i
                break
    return s[:end+1].strip() if end >= 0 else s.strip()

def _payload_to_wkt_list(payload):
    """Payload-dan WKT siyahısı çıxar (GeoJSON dəstəyi də daxil)"""
    wkt_list = []
    
    # Direct WKT
    if "wkt" in payload:
        w = _clean_wkt_text(payload["wkt"])
        if w:
            wkt_list.append(w)
    
    # GeoJSON support
    if "geojson" in payload:
        try:
            from shapely.geometry import shape
            geojson = payload["geojson"]
            if isinstance(geojson, dict):
                if geojson.get("type") == "FeatureCollection":
                    for feat in geojson.get("features", []):
                        if feat.get("geometry"):
                            geom = shape(feat["geometry"])
                            wkt_list.append(geom.wkt)
                elif geojson.get("type") == "Feature":
                    if geojson.get("geometry"):
                        geom = shape(geojson["geometry"])
                        wkt_list.append(geom.wkt)
                else:
                    # Direct geometry
                    geom = shape(geojson)
                    wkt_list.append(geom.wkt)
        except Exception as e:
            logger.warning("[NECAS] GeoJSON parse error: %s", str(e))
    
    return wkt_list

def _infer_srid(wkts: list[str], fallback: int) -> int:
    """WKT koordinatlarından SRID təxmin et"""
    try:
        g0 = _wkt.loads(wkts[0])
        minx, miny, maxx, maxy = g0.bounds
        if (-180 <= minx <= 180 and -180 <= maxx <= 180 and -90 <= miny <= 90 and -90 <= maxy <= 90):
            return 4326
    except Exception:
        pass
    return fallback

# ---------------------------
# API: /api/necas/parcels/by-bbox/
# ---------------------------
@require_GET
def necas_parcels_by_bbox(request):
    try:
        minx = float(request.GET.get("minx"))
        miny = float(request.GET.get("miny"))
        maxx = float(request.GET.get("maxx"))
        maxy = float(request.GET.get("maxy"))
    except (TypeError, ValueError):
        return HttpResponseBadRequest("minx/miny/maxx/maxy parametrləri tələb olunur")

    # BBOX üçün WKT POLYGON
    bbox_wkt = f"POLYGON(({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, {minx} {miny}))"
    
    # Müxtəlif SQL variant-ları (TEKUIS pattern-i)
    sql_variants = [
        # Variant 1: Sadə SDO functions
        f"""
        WITH q AS (
          SELECT SDO_GEOMETRY(:wkt, :srid) g FROM dual
        )
        SELECT
          ROWIDTOCHAR(p.ROWID) AS rid,
          SDO_UTIL.TO_WKTGEOMETRY(p.shape) AS wkt,
          {ATTR_SQL}
        FROM {ql_table()} p, q
        WHERE SDO_ANYINTERACT(p.shape, q.g) = 'TRUE'
            AND {ISDEL_PRED}
        """,
        
        # Variant 2: SDE functions
        f"""
        WITH q AS (
          SELECT sde.st_geomfromtext(:wkt, :srid) g FROM dual
        )
        SELECT
          ROWIDTOCHAR(p.ROWID) AS rid,
          sde.st_astext(p.shape) AS wkt,
          {ATTR_SQL}
        FROM {ql_table()} p, q
        WHERE sde.st_intersects(p.shape, q.g) = 1
            AND {ISDEL_PRED}
        """,
        
        # Variant 3: TO_GEOJSON versiyası  
        f"""
        WITH q AS (
          SELECT sde.st_geomfromtext(:wkt, :srid) g FROM dual
        )
        SELECT
          ROWIDTOCHAR(p.ROWID) AS rid,
          MDSYS.SDO_UTIL.TO_GEOJSON(p.shape) AS geojson,
          {ATTR_SQL}
        FROM {ql_table()} p, q
        WHERE sde.st_intersects(p.shape, q.g) = 1
            AND {ISDEL_PRED}
        """
    ]

    binds = dict(wkt=bbox_wkt, srid=NECAS_SRID)
    
    for i, sql in enumerate(sql_variants, 1):
        try:
            with get_pool().acquire() as con:
                with con.cursor() as cur:
                    cur.execute(sql, binds)
                    rows = cur.fetchall()
                    
                    features = []
                    for row in rows:
                        if len(sql_variants) == 3 and i == 3:  # GeoJSON variant
                            rid, geojson_data, *attr_vals = row
                            geojson_text = geojson_data.read() if hasattr(geojson_data, "read") else geojson_data
                            try:
                                geometry = json.loads(geojson_text)
                            except:
                                continue
                        else:  # WKT variants
                            rid, wkt_data, *attr_vals = row
                            wkt_text = wkt_data.read() if hasattr(wkt_data, "read") else wkt_data
                            wkt_clean = _clean_wkt_text(wkt_text)
                            if not wkt_clean:
                                continue
                            try:
                                geom = _wkt.loads(_normalize_wkt_remove_m_dims(_clip_tail(wkt_clean)))
                                geometry = mapping(geom)
                            except:
                                continue
                        
                        props = _props_from_vals(attr_vals, rid)
                        features.append({
                            "type": "Feature", 
                            "geometry": geometry, 
                            "properties": props
                        })
                    
                    logger.info("[NECAS][BBOX] variant%d returned=%d bbox=(%s,%s,%s,%s)",
                                i, len(features), minx, miny, maxx, maxy)
                    return JsonResponse({"type": "FeatureCollection", "features": features})
                    
        except oracledb.DatabaseError as e:
            logger.warning("[NECAS][BBOX] variant%d failed: %s", i, str(e))
            if i == len(sql_variants):
                return JsonResponse({
                    "ok": False,
                    "error": {
                        "stage": "bbox_all_variants_failed",
                        "message": "NECAS BBOX sorğusu uğursuz oldu - bütün variant-lar",
                        "oracle": str(e)
                    }
                }, status=500)

# ---------------------------
# API: /api/necas/parcels/by-geom/
# ---------------------------
@csrf_exempt
@require_POST
def necas_parcels_by_geom(request):
    # ---- input ----
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return HttpResponseBadRequest("JSON gözlənilirdi")

    srid_in_payload = int(payload.get("srid") or NECAS_SRID)
    buffer_m = float(payload.get("buffer_m") or 0.0)

    # WKT siyahısını topla
    wkt_list = _payload_to_wkt_list(payload)
    if not wkt_list:
        w_single = _clean_wkt_text(payload.get("wkt")) if payload.get("wkt") else None
        if not w_single:
            return HttpResponseBadRequest("wkt və ya geojson tələb olunur")
        wkt_list = [w_single]

    # Input sanitizasiya (TEKUIS pattern-i)
    safe_wkts, bad_empty, bad_curved, bad_parse = [], 0, 0, 0
    for w in wkt_list:
        s = _clean_wkt_text(w or "")
        if not s:
            bad_empty += 1
            continue
        if re.search(r'\b(CURVEPOLYGON|CIRCULARSTRING|COMPOUNDCURVE|ELLIPTICARC|MULTICURVE|MULTISURFACE|GEOMETRYCOLLECTION)\b', s, flags=re.I):
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
        logger.info("[NECAS][GEOM] all invalid input. empty=%d, curved=%d, parse=%d", 
                    bad_empty, bad_curved, bad_parse)
        return JsonResponse({"type": "FeatureCollection", "features": []})

    # SRID auto-detect
    srid_in = _infer_srid(safe_wkts, srid_in_payload)

    logger.info("[NECAS][GEOM] input_sanitized=%d dropped=%d srid_in=%d buf_m=%.3f", 
                len(safe_wkts), bad_empty + bad_curved + bad_parse, srid_in, buffer_m)

    # SQL generator functions
    def _make_sql_variants(n_items: int) -> list[tuple[str, dict]]:
        bind_names = [f"w{i}" for i in range(n_items)]
        g_raw_sql = " \nUNION ALL\n".join([f"  SELECT :{bn} AS wkt FROM dual" for bn in bind_names])
        
        params_base = {bn: safe_wkts[i] for i, bn in enumerate(bind_names)}
        params_base.update({
            "srid_in": int(srid_in), 
            "bufm": float(buffer_m), 
            "table_srid": int(NECAS_SRID)
        })

        variants = []
        
        # Variant 1: SDE functions with transform
        if buffer_m > 0:
            buffer_clause = """
                sde.st_transform(
                    sde.st_buffer(
                        sde.st_transform(sde.st_geomfromtext(wkt, :srid_in), 3857), :bufm
                    ),
                    :table_srid
                )
            """
        else:
            buffer_clause = "sde.st_transform(sde.st_geomfromtext(wkt, :srid_in), :table_srid)"
            
        sql1 = f"""
            WITH g_raw AS (
        {g_raw_sql}
            ),
            g AS (
                SELECT {buffer_clause} AS geom FROM g_raw
            ),
            ids AS (
                SELECT DISTINCT p.ROWID AS rid
                FROM {ql_table()} p, g
                WHERE sde.st_envintersects(p.shape, g.geom) = 1
                AND sde.st_intersects(p.shape, g.geom) = 1
                AND {ISDEL_PRED}
            )
            SELECT p.ROWID AS rid,
                sde.st_astext(p.shape) AS wkt,
                {ATTR_SQL}
            FROM {ql_table()} p
            JOIN ids ON p.ROWID = ids.rid
        """
        variants.append((sql1, params_base.copy()))

        # Variant 2: Simple SDO functions
        if buffer_m > 0:
            buffer_clause2 = "SDO_GEOM.SDO_BUFFER(SDO_GEOMETRY(wkt, :srid_in), :bufm, 0.005, 'unit=meter')"
        else:
            buffer_clause2 = "SDO_GEOMETRY(wkt, :srid_in)"
            
        sql2 = f"""
            WITH g_raw AS (
        {g_raw_sql}
            ),
            g AS (
                SELECT {buffer_clause2} AS geom FROM g_raw
            )
            SELECT ROWIDTOCHAR(p.ROWID) AS rid,
                SDO_UTIL.TO_WKTGEOMETRY(p.shape) AS wkt,
                {ATTR_SQL}
            FROM {ql_table()} p, g
            WHERE SDO_ANYINTERACT(p.shape, g.geom) = 'TRUE'
            AND {ISDEL_PRED}
        """
        variants.append((sql2, params_base.copy()))

        return variants

    def _make_single_wkt_sql() -> list[tuple[str, str]]:
        """Single WKT fallback SQL variants"""
        variants = []
        
        # SDE variant
        if buffer_m > 0:
            geom_clause = """
                sde.st_transform(
                    sde.st_buffer(
                        sde.st_transform(sde.st_geomfromtext(:w, :srid_in), 3857), :bufm
                    ),
                    :table_srid
                )
            """
        else:
            geom_clause = "sde.st_transform(sde.st_geomfromtext(:w, :srid_in), :table_srid)"
            
        sql_sde = f"""
            WITH g AS (
                SELECT {geom_clause} AS geom FROM dual
            )
            SELECT p.ROWID AS rid,
                   sde.st_astext(p.shape) AS wkt,
                   {ATTR_SQL}
            FROM {ql_table()} p, g
            WHERE sde.st_intersects(p.shape, g.geom) = 1
                AND {ISDEL_PRED}
        """
        variants.append(("sde", sql_sde))
        
        # SDO variant
        if buffer_m > 0:
            geom_clause2 = "SDO_GEOM.SDO_BUFFER(SDO_GEOMETRY(:w, :srid_in), :bufm, 0.005, 'unit=meter')"
        else:
            geom_clause2 = "SDO_GEOMETRY(:w, :srid_in)"
            
        sql_sdo = f"""
            WITH g AS (
                SELECT {geom_clause2} AS geom FROM dual
            )
            SELECT ROWIDTOCHAR(p.ROWID) AS rid,
                   SDO_UTIL.TO_WKTGEOMETRY(p.shape) AS wkt,
                   {ATTR_SQL}
            FROM {ql_table()} p, g
            WHERE SDO_ANYINTERACT(p.shape, g.geom) = 'TRUE'
                AND {ISDEL_PRED}
        """
        variants.append(("sdo", sql_sdo))
        
        return variants

    def _make_wkb_sql() -> str:
        """WKB fallback SQL"""
        if buffer_m > 0:
            geom_clause = """
                sde.st_transform(
                    sde.st_buffer(
                        sde.st_transform(sde.st_geomfromwkb(hextoraw(:wkb), :srid_in), 3857), :bufm
                    ),
                    :table_srid
                )
            """
        else:
            geom_clause = "sde.st_transform(sde.st_geomfromwkb(hextoraw(:wkb), :srid_in), :table_srid)"
            
        return f"""
            WITH g AS (
                SELECT {geom_clause} AS geom FROM dual
            )
            SELECT p.ROWID AS rid,
                sde.st_astext(p.shape) AS wkt,
                {ATTR_SQL}
            FROM {ql_table()} p, g
            WHERE sde.st_intersects(p.shape, g.geom) = 1
                AND {ISDEL_PRED}
        """

    # Main processing (TEKUIS pattern-i)
    features, seen_rids = [], set()
    out_skip_empty = out_skip_parse = out_skip_curved = out_tailfix = 0

    def _consume_cursor(cur):
        nonlocal out_skip_empty, out_skip_curved, out_skip_parse, out_tailfix
        for row in cur:
            rid, wkt_lob, *attr_vals = row
            rid_key = str(rid) if rid is not None else None
            if rid_key and rid_key in seen_rids:
                continue

            raw = wkt_lob.read() if hasattr(wkt_lob, "read") else wkt_lob
            w = _clean_wkt_text(raw)
            if not w:
                out_skip_empty += 1
                continue
            if re.search(r'\b(CURVEPOLYGON|CIRCULARSTRING|COMPOUNDCURVE|ELLIPTICARC|MULTICURVE|MULTISURFACE)\b', w, flags=re.I):
                out_skip_curved += 1
                continue

            # tail kəs + M/ZM → 2D
            w2 = _clip_tail(w)
            if w2 != w:
                out_tailfix += 1
            w2 = _normalize_wkt_remove_m_dims(w2)
            
            try:
                geom = _wkt.loads(w2)
            except Exception:
                out_skip_parse += 1
                continue

            props = _props_from_vals(attr_vals, rid_key)
            features.append({"type": "Feature", "geometry": mapping(geom), "properties": props})
            if rid_key:
                seen_rids.add(rid_key)

    # Execute queries
    with get_pool().acquire() as con:
        with con.cursor() as cur:
            CHUNK = 200
            for start in range(0, len(safe_wkts), CHUNK):
                sub = safe_wkts[start:start + CHUNK]
                sql_variants = _make_sql_variants(len(sub))
                
                success = False
                for variant_name, (sql, params) in enumerate(sql_variants, 1):
                    try:
                        # CLOB input sizes
                        try:
                            cur.setinputsizes(**{k: oracledb.DB_TYPE_CLOB for k in params if k.startswith("w")})
                        except Exception:
                            pass
                        cur.execute(sql, params)
                        _consume_cursor(cur)
                        success = True
                        logger.info("[NECAS][GEOM] chunk success with variant%d", variant_name)
                        break
                    except oracledb.DatabaseError as e:
                        logger.warning("[NECAS][GEOM] chunk variant%d failed: %s", variant_name, str(e))
                
                if not success:
                    # Fallback: single WKT processing
                    logger.info("[NECAS][GEOM] falling back to single WKT processing for chunk")
                    single_sql_variants = _make_single_wkt_sql()
                    
                    for w in sub:
                        processed = False
                        for variant_name, sql in single_sql_variants:
                            try:
                                params_single = {
                                    "w": w, 
                                    "srid_in": int(srid_in), 
                                    "bufm": float(buffer_m), 
                                    "table_srid": int(NECAS_SRID)
                                }
                                cur.execute(sql, params_single)
                                _consume_cursor(cur)
                                processed = True
                                break
                            except Exception:
                                continue
                        
                        if not processed:
                            # Final WKB fallback
                            try:
                                g = _wkt.loads(w)
                                wkb_hex = _wkb.dumps(g, hex=True)
                                params_wkb = {
                                    "wkb": wkb_hex, 
                                    "srid_in": int(srid_in), 
                                    "bufm": float(buffer_m), 
                                    "table_srid": int(NECAS_SRID)
                                }
                                cur.execute(_make_wkb_sql(), params_wkb)
                                _consume_cursor(cur)
                            except Exception as e:
                                head = (w[:220] + "…") if len(w) > 220 else w
                                logger.warning("[NECAS][GEOM] skipped WKT: %s, error: %s", head, str(e)[:240])

    logger.info("[NECAS][GEOM] returned=%d unique_rids=%d skipped_out=%d tailfix=%d", 
                len(features), len(seen_rids), out_skip_empty + out_skip_curved + out_skip_parse, out_tailfix)

    return JsonResponse({"type": "FeatureCollection", "features": features})