import io, os, json, tempfile, zipfile, shutil, subprocess
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Any
from datetime import date, datetime
from decimal import Decimal
from math import isfinite

from django.http import JsonResponse, HttpResponseBadRequest
from django.views.decorators.csrf import csrf_exempt
from django.db import connection, transaction
from django.conf import settings
from django.views.decorators.http import require_GET, require_POST
from django.utils.text import get_valid_filename

import shapefile  # pyshp
from pyproj import CRS, Transformer
import csv, re
import requests
import logging
import oracledb
from shapely import wkt as shapely_wkt
from shapely.geometry import mapping, shape as shapely_shape
from shapely.ops import unary_union
import time
from functools import wraps
from .tekuis_validation import validate_tekuis, ignore_gap
import zlib
import base64


logger = logging.getLogger(__name__)



ALLOWED_INFO_FIELDS = {
    "ORG_ID", "RE_TYPE_ID", "RE_CATEGORY_ID", "RE_ADDRESS", "RE_FACTUAL_USE",
    "LAND_AREA_D", "LAND_AREA_F",
    "TOTAL_AREA_D", "TOTAL_AREA_F",
    "MAIN_AREA_D", "MAIN_AREA_F",
    "AUX_AREA_D", "AUX_AREA_F",
    "ROOM_COUNT_D", "ROOM_COUNT_F",
    "ILLEGAL_BUILDS", "NOTES", "CONCLUSION", "OPINION", "REQUEST_NUMBER"
}


# TEKUİS atributları (M_G_PARSEL-dən oxunacaq sütunlar)
TEKUIS_ATTRS = (
    "LAND_CATEGORY2ENUM", "LAND_CATEGORY_ENUM", "NAME", "OWNER_TYPE_ENUM",
    "SUVARILMA_NOVU_ENUM", "EMLAK_NOVU_ENUM", "OLD_LAND_CATEGORY2ENUM",
    "TERRITORY_NAME", "RAYON_ADI", "IED_ADI", "BELEDIYE_ADI",
    "LAND_CATEGORY3ENUM", "LAND_CATEGORY4ENUM", "AREA_HA"
)

def _tekuis_props_from_row(vals):
    """Oracle-dan oxunan dəyərləri sütun adları ilə properties-ə çevirir."""
    return {k: v for k, v in zip(TEKUIS_ATTRS, vals)}


# ==========================
# pyodbc (MSSQL)
# ==========================
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except Exception:
    pyodbc = None
    PYODBC_AVAILABLE = False

# ==========================
# .rar dəstəyi
# ==========================
try:
    import rarfile  # pip install rarfile
    RAR_AVAILABLE = True
except Exception:
    rarfile = None
    RAR_AVAILABLE = False


def _unauthorized(msg="unauthorized"):
    return JsonResponse({"ok": False, "error": msg}, status=401)


def _redeem_ticket_with_token(ticket: str):
    """
    Node redeem-dən həm fk_metadata (id), həm də token qaytarır.
    Token yoxdursa və ya vaxtı keçibsə -> (None, None).
    """
    url = getattr(settings, "NODE_REDEEM_URL",
                  "http://10.11.1.73:8080/api/requests/handoff/redeem").rstrip("/")
    timeout = int(getattr(settings, "NODE_REDEEM_TIMEOUT", 8))
    bearer = getattr(settings, "NODE_REDEEM_BEARER", None)
    headers = {"Accept": "application/json"}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    try:
        resp = requests.post(url, data={"ticket": (ticket or "").strip()},
                             headers={**headers, "Content-Type":"application/x-www-form-urlencoded"},
                             timeout=timeout)
        if resp.status_code != 200:
            return None, None
        data = resp.json()
        if data.get("valid", True) is False:
            return None, None
        tok = (data.get("token") or "").strip()
        exp = data.get("exp")
        # token mütləq lazımdır:
        if not tok:
            return None, None
        # vaxt yoxlaması:
        exp_ms = _coerce_exp_ms(exp)
        if exp_ms is None or _now_ms() > exp_ms + int(getattr(settings, "NODE_REDEEM_EXP_SKEW_SEC", 15))*1000:
            return None, None
        # id götür:
        rid = data.get("id") or data.get("rowid") or data.get("fk") or data.get("fk_metadata")
        try:
            return int(str(rid).strip()), tok
        except Exception:
            return None, None
    except Exception:
        return None, None


def _extract_ticket(request) -> str:
    t = (
        request.POST.get("ticket")
        or request.GET.get("ticket")
        or request.headers.get("X-Ticket")
        or ""
    ).strip()
    if t:
        return t

    # JSON body-dən də yoxla
    ctype = (request.META.get("CONTENT_TYPE") or request.content_type or "").lower()
    if "application/json" in ctype:
        try:
            raw = request.body.decode("utf-8") if request.body else ""
            if raw:
                data = json.loads(raw)
                t2 = (data.get("ticket") or "").strip()
                if t2:
                    # istəsəniz reuse üçün saxlayıram
                    setattr(request, "_json_cached", data)
                    return t2
        except Exception:
            pass
    return ""


def _parse_jwt_user(tok: str) -> tuple[Optional[int], Optional[str]]:
    """
    JWT payload-ını imza yoxlamadan oxuyur və (user_id, full_name) qaytarır.
    Token payload nümunəsi: {"id": 2, "fullName": "..." , ...}
    """
    try:
        parts = (tok or "").split(".")
        if len(parts) < 2:
            return None, None
        b = parts[1]
        # base64url padding
        b += "=" * (-len(b) % 4)
        payload = json.loads(base64.urlsafe_b64decode(b).decode("utf-8"))
        uid = payload.get("id") or payload.get("userId") or payload.get("uid")
        try:
            uid = int(uid) if uid is not None else None
        except Exception:
            uid = None
        fullname = (
            payload.get("fullName")
            or payload.get("fullname")
            or payload.get("name")
            or payload.get("FullName")
        )
        if fullname is not None:
            fullname = str(fullname).strip()
        return uid, fullname
    except Exception:
        return None, None






def require_valid_ticket(view_fn):
    @wraps(view_fn)
    def _wrap(request, *args, **kwargs):
        ticket = _extract_ticket(request)
        fk, tok = _redeem_ticket_with_token(ticket)
        if not (fk and tok):
            return JsonResponse({"ok": False, "error": "unauthorized"}, status=401)

        request.fk_metadata = fk  # metadata id
        request.jwt_token = tok   # xammal JWT
        # token-dən user məlumatını çıxart
        uid, fname = _parse_jwt_user(tok)
        request.user_id_from_token = uid
        request.user_full_name_from_token = fname

        return view_fn(request, *args, **kwargs)
    return _wrap





# ==========================
# Köməkçi: arxivdən çıxarma
# ==========================
def _extract_archive_to_tmp(uploaded_bytes: bytes, filename: str) -> Path:
    tmpdir = Path(tempfile.mkdtemp(prefix="shp_"))
    ext = Path(filename).suffix.lower()
    archive_path = tmpdir / f"upload{ext}"
    with open(archive_path, "wb") as f:
        f.write(uploaded_bytes)

    if ext == ".zip":
        with zipfile.ZipFile(archive_path) as z:
            z.extractall(tmpdir)
    elif ext == ".rar":
        if not RAR_AVAILABLE:
            raise RuntimeError("RAR dəstəyi üçün 'rarfile' paketi və sistemi aləti lazımdır.")
        with rarfile.RarFile(archive_path) as rf:
            rf.extractall(tmpdir)
    else:
        raise ValueError("Yalnız .zip və .rar arxivləri qəbul edilir.")
    return tmpdir

def _find_main_shp(tmpdir: Path) -> Path:
    for p in tmpdir.rglob("*.shp"):
        return p
    raise FileNotFoundError("Arxivdə .shp tapılmadı.")

def _read_prj_for_crs(shp_path: Path) -> Optional[CRS]:
    prj_path = shp_path.with_suffix(".prj")
    if prj_path.exists():
        try:
            wkt = prj_path.read_text(encoding="utf-8", errors="ignore")
            return CRS.from_wkt(wkt)
        except Exception:
            return None
    return None

def _guess_crs_or_transformer(first_xy: Tuple[float, float]) -> Optional[Transformer]:
    x, y = first_xy
    if -180 <= x <= 180 and -90 <= y <= 90:
        return None
    candidates = [CRS.from_epsg(32638), CRS.from_epsg(32639)]  # UTM 38N, 39N
    for cand in candidates:
        try:
            t = Transformer.from_crs(cand, CRS.from_epsg(4326), always_xy=True)
            lon, lat = t.transform(x, y)
            if 40 <= lon <= 55 and 35 <= lat <= 50:
                return t
        except Exception:
            continue
    return None

def _make_transformer(shp_path: Path, first_xy: Tuple[float, float]) -> Optional[Transformer]:
    src_crs = _read_prj_for_crs(shp_path)
    if src_crs:
        try:
            if src_crs.to_epsg() == 4326:
                return None
        except Exception:
            pass
        try:
            return Transformer.from_crs(src_crs, CRS.from_epsg(4326), always_xy=True)
        except Exception:
            return _guess_crs_or_transformer(first_xy)
    else:
        return _guess_crs_or_transformer(first_xy)

def _parts_indices(shape) -> List[Tuple[int, int]]:
    parts = getattr(shape, "parts", []) or []
    if not parts:
        return [(0, len(shape.points))]
    idxs = []
    for i, start in enumerate(parts):
        end = parts[i + 1] if i + 1 < len(parts) else len(shape.points)
        idxs.append((start, end))
    return idxs

def _transform_coords(coords: List[Tuple[float, float]], transformer: Optional[Transformer]):
    if transformer is None:
        return coords
    out = []
    for x, y in coords:
        lon, lat = transformer.transform(x, y)
        out.append((lon, lat))
    return out

def _shape_to_geojson_geometry(shape, transformer: Optional[Transformer]) -> dict:
    st = shape.shapeType
    pts = shape.points or []

    if st in (shapefile.POINT, shapefile.POINTZ, shapefile.POINTM):
        lonlat = _transform_coords([pts[0]], transformer)[0]
        return {"type": "Point", "coordinates": lonlat}
    if st in (shapefile.MULTIPOINT, shapefile.MULTIPOINTZ, shapefile.MULTIPOINTM):
        lonlats = _transform_coords(pts, transformer)
        return {"type": "MultiPoint", "coordinates": lonlats}
    if st in (shapefile.POLYLINE, shapefile.POLYLINEZ, shapefile.POLYLINEM):
        parts = _parts_indices(shape)
        lines = []
        for s, e in parts:
            lonlats = _transform_coords(pts[s:e], transformer)
            lines.append(lonlats)
        if len(lines) == 1:
            return {"type": "LineString", "coordinates": lines[0]}
        return {"type": "MultiLineString", "coordinates": lines}
    if st in (shapefile.POLYGON, shapefile.POLYGONZ, shapefile.POLYGONM):
        parts = _parts_indices(shape)
        rings = []
        for s, e in parts:
            lonlats = _transform_coords(pts[s:e], transformer)
            if lonlats and lonlats[0] != lonlats[-1]:
                lonlats.append(lonlats[0])
            rings.append(lonlats)
        if len(rings) == 1:
            return {"type": "Polygon", "coordinates": [rings[0]]}
        return {"type": "MultiPolygon", "coordinates": [[r] for r in rings]}
    return {"type": "GeometryCollection", "geometries": []}

def _records_as_props(reader: shapefile.Reader, rec) -> dict:
    field_names = [f[0] for f in reader.fields[1:]]  # DeletionFlag-i burax
    values = list(rec.record)
    props = {}
    for k, v in zip(field_names, values):
        try:
            props[k] = v if isinstance(v, (int, float, str)) or v is None else str(v)
        except Exception:
            props[k] = str(v)
    return props


def _clean_wkt_text(w):
    """
    ST_AsText nəticəsində gələ bilən SRID prefiksi və ya
    bağlanış mötərizəsindən sonrakı əlavə parçaları kəsir.
    """
    if w is None:
        return None
    w = w.strip()

    # 1) SRID=xxxx; prefixini at
    if w.upper().startswith("SRID="):
        parts = w.split(";", 1)
        if len(parts) == 2:
            w = parts[1].strip()

    # 2) Bağlanış mötərizəsindən sonrakı zibili at (məs., "…)) 4326")
    r = w.rfind(")")
    if r != -1:
        w = w[:r+1].strip()

    return w


def _oracle_connect():
    host     = os.getenv("ORA_HOST", "alldb-scan.emlak.gov.az")
    port     = int(os.getenv("ORA_PORT", "1521"))
    service  = os.getenv("ORA_SERVICE", "tekuisdb")
    user     = os.getenv("ORA_USER")
    password = os.getenv("ORA_PASSWORD")

    dsn = oracledb.makedsn(host, port, service_name=service)

    # Bəzi oracledb versiyalarında 'encoding' dəstəklənmir → geriyə uyğun bağla
    try:
        return oracledb.connect(user=user, password=password, dsn=dsn, encoding="UTF-8", nencoding="UTF-8")
    except TypeError:
        return oracledb.connect(user=user, password=password, dsn=dsn)




# ==========================
# CSV/TXT köməkçiləri
# ==========================
def _decode_bytes_to_text(data: bytes) -> str:
    try:
        return data.decode('utf-8-sig')
    except Exception:
        return data.decode('latin-1', errors='ignore')

def _sniff_dialect(sample: str) -> csv.Dialect:
    try:
        return csv.Sniffer().sniff(sample, delimiters=',;\t| ')
    except Exception:
        class Simple(csv.Dialect):
            delimiter = ','
            quotechar = '"'
            escapechar = None
            doublequote = True
            lineterminator = '\n'
            quoting = csv.QUOTE_MINIMAL
            skipinitialspace = True
        return Simple()

_DEF_X = {'x', 'lon', 'long', 'longitude', 'easting', 'utm_e', 'utm_x'}
_DEF_Y = {'y', 'lat', 'latitude', 'northing', 'utm_n', 'utm_y'}

def _normalize(name: str) -> str:
    return re.sub(r'[^a-z0-9]+', '', name.lower())

def _find_xy_columns(header: list[str]) -> tuple[int, int] | tuple[None, None]:
    norm = [_normalize(h) for h in header]
    x_idx = next((i for i, n in enumerate(norm) if n in _DEF_X), None)
    y_idx = next((i for i, n in enumerate(norm) if n in _DEF_Y), None)
    return x_idx, y_idx

# --- CRS sütunu ---
_CRS_COL_CANDIDATES = {
    'coordinatesystem', 'coordsystem', 'coordsys', 'coord_system', 'coordinate_system', 'crs'
}

def _find_crs_column(header: list[str]) -> Optional[int]:
    norm = [_normalize(h) for h in header]
    for i, n in enumerate(norm):
        if n in _CRS_COL_CANDIDATES:
            return i
    return None

def _canonize_crs_value(text: str) -> Optional[str]:
    if not text:
        return None
    t = _normalize(str(text))
    if 'wgs84' in t or '4326' in t or 'lonlat' in t:
        return 'wgs84'
    if '32638' in t or 'utm38' in t:
        return 'utm38'
    if '32639' in t or 'utm39' in t:
        return 'utm39'
    return None

def _build_transformer_for_points(crs_choice: str) -> Optional[Transformer]:
    crs_choice = (crs_choice or 'wgs84').lower()
    if crs_choice in ('auto', 'detect'):
        return None
    if crs_choice == 'wgs84':
        return None
    if crs_choice == 'utm38':
        return Transformer.from_crs(CRS.from_epsg(32638), CRS.from_epsg(4326), always_xy=True)
    if crs_choice == 'utm39':
        return Transformer.from_crs(CRS.from_epsg(32639), CRS.from_epsg(4326), always_xy=True)
    return None

def _row_to_float(v):
    if isinstance(v, str):
        v = v.strip().replace(',', '.')
    return float(v)

# ==========================
# Upload servisleri
# ==========================
@csrf_exempt
@require_valid_ticket
def upload_shp(request):
    if request.method != "POST":
        return HttpResponseBadRequest("POST gözlənirdi.")
    f = request.FILES.get("file")
    if not f:
        return HttpResponseBadRequest("Fayl göndərilməyib: 'file' sahəsi boşdur.")

    tmpdir = None
    try:
        data = f.read()
        tmpdir = _extract_archive_to_tmp(data, f.name)
        shp_path = _find_main_shp(tmpdir)

        r = shapefile.Reader(str(shp_path))
        if r.numRecords == 0:
            return HttpResponseBadRequest("Shapefile boşdur.")

        first_shape = r.shape(0)
        if not first_shape.points:
            return HttpResponseBadRequest("Geometriya tapılmadı.")
        first_xy = first_shape.points[0]
        transformer = _make_transformer(shp_path, first_xy)

        features = []
        for i in range(r.numRecords):
            s = r.shape(i)
            rec = r.shapeRecord(i)
            geom = _shape_to_geojson_geometry(s, transformer)
            props = _records_as_props(r, rec)
            features.append({"type": "Feature", "geometry": geom, "properties": props})
        fc = {"type": "FeatureCollection", "features": features}
        return JsonResponse(fc, safe=False)
    except Exception as e:
        return HttpResponseBadRequest(f"Xəta: {e}")
    finally:
        if tmpdir and tmpdir.exists():
            try:
                shutil.rmtree(tmpdir)
            except Exception:
                pass

@csrf_exempt
@require_valid_ticket
def upload_points(request):
    """
    CSV/TXT oxu və WGS84-ə çevir. Prioritet:
      1) Sətirdə 'coordinate_system' sütunu varsa → onu istifadə et
      2) POST 'crs' (radio) gəlirsə → onu istifadə et
      3) Əks halda auto-detect (mövcud məntiq)
    """
    if request.method != "POST":
        return HttpResponseBadRequest("POST gözlənirdi.")
    f = request.FILES.get("file")
    posted_crs_choice = request.POST.get("crs", "wgs84")
    if not f:
        return HttpResponseBadRequest("Fayl göndərilməyib: 'file' sahəsi boşdur.")

    try:
        data_bytes = f.read()
        text = _decode_bytes_to_text(data_bytes)

        sample = text[:4096]
        dialect = _sniff_dialect(sample)
        reader = csv.reader(io.StringIO(text), dialect)

        rows = list(reader)
        if not rows:
            return HttpResponseBadRequest("Fayl boşdur.")

        has_header = csv.Sniffer().has_header(sample) if len(rows) > 1 else False

        if has_header:
            header = rows[0]
            body = rows[1:]
        else:
            max_len = max(len(r) for r in rows)
            header = [f'col{i+1}' for i in range(max_len)]
            body = rows

        x_idx, y_idx = _find_xy_columns(header)
        if x_idx is None or y_idx is None:
            if len(header) >= 2:
                x_idx, y_idx = 0, 1
            else:
                return HttpResponseBadRequest("X/Y (vəya lon/lat) sütunları tapılmadı.")

        crs_idx = _find_crs_column(header)
        default_transformer = _build_transformer_for_points(posted_crs_choice)

        features = []
        for r in body:
            if len(r) <= max(x_idx, y_idx):
                continue
            try:
                x = _row_to_float(r[x_idx])
                y = _row_to_float(r[y_idx])
            except Exception:
                continue

            row_transformer = default_transformer
            if crs_idx is not None and crs_idx < len(r):
                code = _canonize_crs_value(r[crs_idx])
                if code:
                    row_transformer = _build_transformer_for_points(code)

            if row_transformer:
                lon, lat = row_transformer.transform(x, y)
            else:
                lon, lat = x, y

            if not (-180 <= lon <= 180 and -90 <= lat <= 90):
                continue

            props = {}
            for i, val in enumerate(r):
                if i in (x_idx, y_idx):
                    continue
                key = header[i] if i < len(header) else f'col{i+1}'
                props[key] = val

            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props
            })

        if not features:
            return HttpResponseBadRequest("Etibarlı nöqtə tapılmadı.")

        fc = {"type": "FeatureCollection", "features": features}
        return JsonResponse(fc, safe=False)

    except Exception as e:
        return HttpResponseBadRequest(f"Xəta: {e}")


def _now_ms() -> int:
    # UTC vaxtını millisekundla qaytarır
    return int(time.time() * 1000)

def _coerce_exp_ms(exp_val) -> int | None:
    """Node-dan gələn exp həm saniyə (10^9), həm də millisekund (10^13) ola bilər.
       Saniyədirsə *1000, yoxdursa olduğu kimi qaytar."""
    try:
        v = int(exp_val)
    except Exception:
        return None
    # 10^12-dən kiçikdirsə, çox güman saniyədir
    return v * 1000 if v < 10**12 else v


# ==========================
# Node redeem (FORM prioritet + configurable)
# ==========================
def _redeem_ticket(ticket: str) -> Optional[int]:
    """
    Node redeem endpoint-ini çağırır və yalnız aşağıdakılar ödənərsə id qaytarır:
      - HTTP 200 + JSON parse OK
      - data.valid != False
      - token mövcuddur (boş deyil)
      - exp mövcuddur və _now_ms() < exp (+ kiçik saat fərqi buferi)
    Əks halda None.
    """
    ticket = (ticket or "").strip()
    if not ticket:
        return None

    url = getattr(settings, "NODE_REDEEM_URL",
                  "http://10.11.1.73:8080/api/requests/handoff/redeem").rstrip("/")
    timeout = int(getattr(settings, "NODE_REDEEM_TIMEOUT", 8))
    prefer  = (getattr(settings, "NODE_REDEEM_METHOD", "FORM") or "FORM").upper()

    require_token   = bool(getattr(settings, "NODE_REDEEM_REQUIRE_TOKEN", True))
    skew_sec        = int(getattr(settings, "NODE_REDEEM_EXP_SKEW_SEC", 15))  # kiçik saat fərqi buferi
    skew_ms         = skew_sec * 1000

    bearer = getattr(settings, "NODE_REDEEM_BEARER", None)
    base_headers = {"Accept": "application/json"}
    if bearer:
        base_headers["Authorization"] = f"Bearer {bearer}"

    def _parse_and_validate(resp) -> Optional[int]:
        if resp.status_code != 200:
            logger.warning("redeem HTTP %s: %s",
                           resp.status_code, (resp.text[:300] if resp.content else ""))
            return None
        try:
            data = resp.json()
        except Exception:
            logger.warning("redeem JSON parse failed: %r", resp.text[:200])
            return None

        # valid=false isə rədd
        if data.get("valid", True) is False:
            logger.info("redeem: valid=false qaytdı")
            return None

        # token tələbi (default: tələb olunur)
        tok = (data.get("token") or "").strip()
        if require_token and not tok:
            logger.info("redeem: token yoxdur (require_token=True)")
            return None

        # exp yoxlaması
        exp_ms = _coerce_exp_ms(data.get("exp"))
        if exp_ms is None:
            logger.info("redeem: exp yoxdur/yolverilməz")
            return None
        now = _now_ms()
        if now > (exp_ms + skew_ms):
            logger.info("redeem: token expiry keçib (now=%s, exp=%s, skew_ms=%s)", now, exp_ms, skew_ms)
            return None

        # id götür
        rid = data.get("id") or data.get("rowid") or data.get("fk") or data.get("fk_metadata")
        try:
            return int(str(rid).strip())
        except Exception:
            logger.warning("redeem: 'id' parse olunmadı: %r", rid)
            return None

    def _post_form(key: str) -> Optional[int]:
        try:
            h = {**base_headers, "Content-Type": "application/x-www-form-urlencoded"}
            resp = requests.post(url, data={key: ticket}, headers=h, timeout=timeout)
            logger.info("redeem POST FORM %s → %s", key, resp.status_code)
            return _parse_and_validate(resp)
        except Exception as e:
            logger.warning("redeem POST FORM (%s) failed: %s", key, e)
            return None

    def _post_json(key: str) -> Optional[int]:
        try:
            h = {**base_headers, "Content-Type": "application/json"}
            resp = requests.post(url, json={key: ticket}, headers=h, timeout=timeout)
            logger.info("redeem POST JSON %s → %s", key, resp.status_code)
            return _parse_and_validate(resp)
        except Exception as e:
            logger.warning("redeem POST JSON (%s) failed: %s", key, e)
            return None

    def _get_qs(key: str) -> Optional[int]:
        try:
            resp = requests.get(url, params={key: ticket}, headers=base_headers,
                                 timeout=timeout, allow_redirects=False)
            logger.info("redeem GET %s → %s", key, resp.status_code)
            return _parse_and_validate(resp)
        except Exception as e:
            logger.warning("redeem GET (%s) failed: %s", key, e)
            return None

    order_map = {
        "FORM": (_post_form, _post_json, _get_qs),
        "JSON": (_post_json, _get_qs, _post_form),
        "GET":  (_get_qs, _post_form, _post_json),
    }
    order = order_map.get(prefer, order_map["FORM"])

    for fn in order:
        for key in ("ticket", "hash"):
            rid = fn(key)
            if rid is not None:
                return rid

    logger.error("redeem failed for ticket (all attempts or token/exp invalid)")
    return None





# ==========================
# PostGIS insert (save)
# ==========================
@csrf_exempt
@require_valid_ticket
def save_polygon(request):

    if request.method != "POST":
        return HttpResponseBadRequest("POST gözlənirdi.")

    # Body oxu
    try:
        payload = getattr(request, "_json_cached", None) or json.loads(request.body.decode("utf-8"))
    except Exception:
        return HttpResponseBadRequest("Yanlış JSON.")

    # Auth/meta
    fk_metadata = getattr(request, "fk_metadata", None)
    if not fk_metadata:
        return JsonResponse({"ok": False, "error": "unauthorized"}, status=401)

    allowed, sid = _is_edit_allowed_for_fk(fk_metadata)
    if not allowed:
        return JsonResponse(
            {"ok": False, "error": "Bu müraciət statusunda GIS redaktə qadağandır.", "status_id": sid},
            status=403
        )


    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT COUNT(1)
                  FROM gis_data
                 WHERE fk_metadata = %s
                   AND COALESCE(status, 1) = 1
            """, [fk_metadata])
            active_cnt = cur.fetchone()[0] or 0

        if active_cnt > 0:

            return JsonResponse({
                "ok": False,
                "code": "ALREADY_SAVED",
                "fk_metadata": int(fk_metadata),
                "message": "Məlumatlar artıq yadda saxlanılıb!"
            }, status=409)
    except Exception as e:
        logger.warning("save_polygon: exists-check failed: %s", e)
        return JsonResponse({
            "ok": False,
            "error": "Məlumat yoxlaması alınmadı, əməliyyat dayandırıldı."
        }, status=500)

    # Bu nöqtəyə yalnız AKTİV sətir YOXDURsa gəlirik (yəni status=0-dır və ya ümumiyyətlə sətir yoxdur)

    # Giriş geometriyaları (WKT/GeoJSON)
    wkts_raw = _payload_to_wkt_list(payload)
    if (not wkts_raw) and payload.get("wkt"):
        wkts_raw = [_clean_wkt_text(str(payload["wkt"]))]

    if not wkts_raw:
        return HttpResponseBadRequest("wkt və/vəya geojson boşdur.")

    # MultiPolygon → Polygon-lara parçala
    single_polygon_wkts: List[str] = []
    for w in wkts_raw:
        if not w:
            continue
        try:
            g = shapely_wkt.loads(w)
        except Exception:
            continue
        if g.is_empty:
            continue
        gt = g.geom_type
        if gt == "Polygon":
            single_polygon_wkts.append(g.wkt)
        elif gt == "MultiPolygon":
            for sub in g.geoms:
                if sub and (not sub.is_empty):
                    single_polygon_wkts.append(sub.wkt)
        else:
            continue

    # Dublikatları at
    single_polygon_wkts = list(dict.fromkeys(single_polygon_wkts))
    if not single_polygon_wkts:
        return HttpResponseBadRequest("Yalnız (Multi)Polygon geometriyaları qəbul olunur.")


    replace = False

    uid = getattr(request, "user_id_from_token", None)
    ufn = getattr(request, "user_full_name_from_token", None)

    try:
        ids = []
        replaced_old = 0
        with transaction.atomic():
            with connection.cursor() as cur:
                if replace:
                    cur.execute("""
                        UPDATE gis_data
                           SET status = 0, last_edited_date = NOW()
                         WHERE fk_metadata = %s
                           AND COALESCE(status,1) = 1
                    """, [fk_metadata])
                    replaced_old = cur.rowcount or 0

                # İndi insert etmək olar (aktiv sətir yoxdur)
                for poly_wkt in single_polygon_wkts:
                    cur.execute("""
                        INSERT INTO gis_data (fk_metadata, geom, status, user_id, user_full_name)
                        VALUES (%s, ST_GeomFromText(%s, 4326), 1, %s, %s)
                        RETURNING id
                    """, [fk_metadata, poly_wkt, uid, ufn])
                    ids.append(cur.fetchone()[0])

        # MSSQL OBJECTID (birinci id ilə)
        mssql_ok = False
        try:
            if PYODBC_AVAILABLE and ids:
                mssql_ok = _mssql_set_objectid(int(fk_metadata), int(ids[0]))
            else:
                logger.warning("pyodbc not available or no ids; skipping OBJECTID update")
        except Exception as e:
            logger.error("OBJECTID update exception: %s", e)

        return JsonResponse({
            "ok": True,
            "fk_metadata": int(fk_metadata),
            "inserted_count": len(ids),
            "ids": ids,
            "mssql_objectid_updated": bool(mssql_ok),
            "replaced_old": replaced_old if replace else 0
        }, status=200)

    except Exception as e:
        return HttpResponseBadRequest(f"Xəta: {e}")





######## tekuis helper

def _flatten_geoms(g):
    """GeometryCollection/Multi* daxil olmaqla bütün hissələri sadə geometrlərə parçala."""
    if g.is_empty:
        return []
    gt = g.geom_type
    if gt == "GeometryCollection":
        out = []
        for sub in g.geoms:
            out.extend(_flatten_geoms(sub))
        return out
    if gt.startswith("Multi"):
        return [sub for sub in g.geoms if not sub.is_empty]
    return [g]

def _payload_to_wkt_list(payload: dict) -> List[str]:
    """
    payload-dakı wkt/geojson-u oxuyur, bütün geometrləri ayrı-ayrılıqda WKT siyahısı kimi qaytarır.
    (Feature, FeatureCollection, list, tək geometry – hamısı dəstəklənir.)
    """
    wkts = []

    # 1) WKT birbaşa verilibsə
    if payload.get("wkt"):
        w = _clean_wkt_text(str(payload["wkt"]))
        if w:
            wkts.append(w)
        return wkts

    gj = payload.get("geojson")
    if gj is None:
        return wkts

    def _to_geom(obj):
        if isinstance(obj, dict) and obj.get("type") == "Feature":
            return shapely_shape(obj.get("geometry"))
        return shapely_shape(obj)

    geoms = []
    try:
        if isinstance(gj, dict) and gj.get("type") == "FeatureCollection":
            for f in gj.get("features", []):
                try:
                    g = _to_geom(f)
                    geoms.extend(_flatten_geoms(g))
                except Exception:
                    continue
        elif isinstance(gj, dict) and gj.get("type"):
            geoms.extend(_flatten_geoms(_to_geom(gj)))
        elif isinstance(gj, list):
            for item in gj:
                try:
                    geoms.extend(_flatten_geoms(_to_geom(item)))
                except Exception:
                    continue
    except Exception:
        pass

    for g in geoms:
        try:
            wkts.append(g.wkt)
        except Exception:
            continue

    return wkts




def _payload_to_single_wkt(payload: dict) -> Optional[str]:
    """
    payload içindəki 'wkt' və ya 'geojson' (Feature/FeatureCollection/list) → tək WKT
    """
    # 1) WKT birbaşa verilibsə
    if payload.get("wkt"):
        return _clean_wkt_text(str(payload["wkt"]))

    gj = payload.get("geojson")
    if gj is None:
        return None

    def _to_geom(obj):
        # Feature → geometry hissəsi, yoxsa birbaşa geometry
        if isinstance(obj, dict) and obj.get("type") == "Feature":
            return shapely_shape(obj.get("geometry"))
        return shapely_shape(obj)

    geoms = []

    # FeatureCollection
    if isinstance(gj, dict) and gj.get("type") == "FeatureCollection":
        for f in gj.get("features", []):
            try:
                g = _to_geom(f)
                if not g.is_empty:
                    geoms.append(g)
            except Exception:
                continue
    # Tək Feature və ya tək Geometry
    elif isinstance(gj, dict) and gj.get("type"):
        try:
            g = _to_geom(gj)
            if not g.is_empty:
                geoms.append(g)
        except Exception:
            pass
    # Siyahı (bir neçə feature/geometry)
    elif isinstance(gj, list):
        for item in gj:
            try:
                g = _to_geom(item)
                if not g.is_empty:
                    geoms.append(g)
            except Exception:
                continue

    if not geoms:
        return None

    merged = unary_union(geoms) if len(geoms) > 1 else geoms[0]
    return merged.wkt





# ==========================
# Məlumat Paneli üçün API
# ==========================
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
    host   = (getattr(settings, "MSSQL_HOST", "") or "").strip()
    port   = int(getattr(settings, "MSSQL_PORT", 1433))
    db     = (getattr(settings, "MSSQL_NAME", "") or "").strip()
    user   = (getattr(settings, "MSSQL_USER", "") or "").strip()
    pwd    = getattr(settings, "MSSQL_PASSWORD", "")
    enc    = "yes" if _as_bool(getattr(settings, "MSSQL_ENCRYPT", True)) else "no"
    trust  = "yes" if _as_bool(getattr(settings, "MSSQL_TRUST_CERT", False)) else "no"
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
            cur.execute("""
                SELECT UPPER(COLUMN_NAME)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'TBL_REQUEST_REG'
            """, (getattr(settings, "MSSQL_SCHEMA", "dbo"),))
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
                        cur.execute(f"SELECT TOP 1 ORG_NAME_SHORT FROM {schema}.TBL_ORGS WHERE ORG_ID = ?", (int(org_id),))
                        r = cur.fetchone()
                        if r and r[0]:
                            data["ORG_ID"] = r[0]
                    except Exception:
                        pass


                re_type_id = data.get("RE_TYPE_ID")
                if re_type_id is not None:
                    try:
                        cur.execute(f"SELECT TOP 1 RE_TYPE_NAME FROM {schema}.DIC_RE_TYPES WHERE RE_TYPE_ID = ?", (int(re_type_id),))
                        r = cur.fetchone()
                        if r and r[0]:
                            data["RE_TYPE_ID"] = r[0]
                    except Exception:
                        pass

               
                re_cat_id = data.get("RE_CATEGORY_ID")
                if re_cat_id is not None:
                    try:
                        cur.execute(f"SELECT TOP 1 RE_CATEGORY_NAME FROM {schema}.DIC_RE_CATEGORIES WHERE RE_CATEGORY_ID = ?", (int(re_cat_id),))
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
            cur.execute("""
                SELECT UPPER(COLUMN_NAME)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'TBL_REQUEST_REG'
            """, (schema,))
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



def _has_active_tekuis(meta_id: int) -> bool:
    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT 1
                  FROM tekuis_parcel
                 WHERE meta_id = %s
                   AND COALESCE(status, 1) = 1
                 LIMIT 1
            """, [int(meta_id)])
            return cur.fetchone() is not None
    except Exception:
        return False





def _resolve_fk_by_wkt(wkt: str) -> Optional[int]:
    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT fk_metadata
                FROM gis_data
                WHERE COALESCE(status,1) = 1
                AND ST_Intersects(geom, ST_GeomFromText(%s, 4326))
                LIMIT 1
            """, [wkt])
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
    except Exception:
        return None

@csrf_exempt
def info_by_geom(request):
    if request.method != "POST":
        return HttpResponseBadRequest("POST gözlənirdi.")
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return HttpResponseBadRequest("Yanlış JSON.")

    wkt = payload.get("wkt")
    if not wkt:
        return HttpResponseBadRequest("wkt tələb olunur.")

    fk = _resolve_fk_by_wkt(wkt)
    if fk is None:
        return JsonResponse({"ok": True, "fk_metadata": None, "data": None})

    details = _mssql_fetch_request(fk)
    filtered = _filter_request_fields(details)
    return JsonResponse({"ok": True, "fk_metadata": fk, "data": filtered})

def info_by_fk(request, fk: int):
    if fk is None:
        return HttpResponseBadRequest("fk düzgün deyil.")
    details = _mssql_fetch_request(int(fk))
    filtered = _filter_request_fields(details)
    status = 200 if filtered else 404
    return JsonResponse({"ok": bool(filtered), "fk_metadata": fk, "data": filtered}, status=status)




@require_GET
def ticket_status(request):
    ticket = (request.GET.get("ticket") or "").strip()
    fk, tok = _redeem_ticket_with_token(ticket)
    if not (fk and tok):
        return JsonResponse({"ok": False}, status=401)
  
    try:
        allowed, sid = _is_edit_allowed_for_fk(fk)
    except Exception:
        allowed, sid = False, None
    return JsonResponse({"ok": True, "status_id": sid, "allow_edit": bool(allowed)})


# NEW: TEKUIS-in artıq saxlanılıb-saxlanılmadığını ticket-lə soruş
@require_GET
@require_valid_ticket
def tekuis_exists_by_ticket(request):
    meta_id = getattr(request, "fk_metadata", None)
    if not meta_id:
        return JsonResponse({"ok": False, "error": "unauthorized"}, status=401)
    return JsonResponse({"ok": True, "exists": _has_active_tekuis(int(meta_id))})





@require_GET
def layers_by_ticket(request):
    ticket = request.GET.get("ticket", "").strip()
    if not ticket:
        return HttpResponseBadRequest("ticket tələb olunur.")

    fk_metadata = _redeem_ticket(ticket)
    if fk_metadata is None:
        return _unauthorized()

    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT id, fk_metadata, ST_AsGeoJSON(geom) AS gj
                FROM gis_data
                WHERE fk_metadata = %s
                AND COALESCE(status,1) = 1
            """, [fk_metadata])
            rows = cur.fetchall()

        features = []
        for rid, fk, gj in rows:
            try:
                geom = json.loads(gj) if isinstance(gj, str) else gj
            except Exception:
                geom = None
            if not geom:
                continue
            features.append({
                "type": "Feature",
                "id": rid,
                "geometry": geom,
                "properties": {"fk_metadata": fk}
            })
        fc = {"type": "FeatureCollection", "features": features, "count": len(features), "fk_metadata": fk_metadata}
        return JsonResponse(fc, safe=False)
    except Exception as e:
        return HttpResponseBadRequest(f"Xəta: {e}")


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
            cur.execute("""
                SELECT UPPER(COLUMN_NAME)
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'TBL_REQUEST_REG'
            """, (schema,))
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




@csrf_exempt
@require_POST
def soft_delete_gis_by_ticket(request):
    ticket = request.GET.get('ticket') or request.POST.get('ticket')
    if not ticket:
        return HttpResponseBadRequest('ticket is required')

    meta_id = _redeem_ticket(ticket)
    if meta_id is None:
        return _unauthorized()

    # meta_id-i int-ə çevir ki, tip uyğunsuzluğu olmasın
    try:
        meta_id_int = int(meta_id)
    except Exception:
        return JsonResponse({"ok": False, "error": f"Bad meta_id: {meta_id!r}"}, status=400)

    with transaction.atomic():
        with connection.cursor() as cur:

            # 1) TEKUIS: schema-ni tam yaz + cast et, RETURNING ilə diaqnostika
            cur.execute("""
                UPDATE public.tekuis_parcel
                SET status = 0,
                    last_edited_date = NOW()
                WHERE meta_id = %s::int
                  AND COALESCE(status, 1) <> 0
                RETURNING tekuis_id
            """, [meta_id_int])
            updated_rows = cur.fetchall()  # təsirlənən sətrlərin id-ləri
            affected_parcel = len(updated_rows)

            # 2) GIS DATA
            cur.execute("""
                UPDATE public.gis_data
                SET status = 0,
                    last_edited_date = NOW()
                WHERE fk_metadata = %s::int
                  AND COALESCE(status, 1) <> 0
            """, [meta_id_int])
            affected_gis = cur.rowcount or 0

            # 3) ATTACH
            cur.execute("""
                UPDATE public.attach_file
                SET status = 0
                WHERE meta_id = %s::int
                  AND COALESCE(status, 1) <> 0
            """, [meta_id_int])
            affected_attach = cur.rowcount or 0

        try:
            objectid_nullified = _mssql_clear_objectid(meta_id_int)
        except Exception as e:
            logger.error("MSSQL OBJECTID clear failed: %s", e)
            objectid_nullified = False

    return JsonResponse({
        'ok': True,
        'meta_id': meta_id_int,
        'affected_parcel': affected_parcel,
        'affected_gis': affected_gis,
        'affected_attach': affected_attach,
        'objectid_nullified': bool(objectid_nullified),
        'debug_tekuis_ids': updated_rows,   # istəsən sonradan sil
    })




# ==========================
# ATTACH: UNC → lokal fallback
# ==========================
ALLOWED_ATTACH_EXT = {'.zip', '.csv', '.txt'}


_CRS_LABELS = {
    'wgs84': 'WGS84 (lon/lat)',
    'utm38': 'UTM 38N',
    'utm39': 'UTM 39N',
}

_WINERR_RETRY = {53, 1326, 1219}

def _smb_net_use():
    unc = getattr(settings, "ATTACH_BASE_DIR", None)
    dom = getattr(settings, "ATTACH_SMB_DOMAIN", "") or ""
    user = getattr(settings, "ATTACH_SMB_USER", "") or ""
    pwd = getattr(settings, "ATTACH_SMB_PASSWORD", "") or ""

    if not unc:
        return

    # --- YENİ: UNC yolunu normallaşdır ---
    unc = str(unc).strip()
    # forward-slash-ları backslash-a çevir
    unc = unc.replace("/", "\\")
    # tək backslash-la başlayırsa iki backslash et
    if unc.startswith("\\") and not unc.startswith("\\\\"):
        unc = "\\" + unc

    # UNC açıqdırsa, keç
    try:
        if os.path.isdir(unc):
            return
    except Exception:
        pass

    def _run(cmd):
        return subprocess.run(cmd, capture_output=True, text=True)

    # 1219 (multiple connections) ehtimalı → əvvəl köhnəni sil
    _run(["cmd", "/c", "net", "use", unc, "/delete", "/y"])

    # UNC-dən host çıxart
    host = ""
    try:
        if unc.startswith("\\\\"):
            host = unc.split("\\")[2]
    except Exception:
        pass

    # Cəhd ardıcıllığı: DOMAIN\user → HOST\user → user (parolsuz ssenari də ola bilər)
    candidates = []
    if user and pwd:
        if dom:
            candidates.append((f"{dom}\\{user}", pwd))
        if host:
            candidates.append((f"{host}\\{user}", pwd))
        candidates.append((user, pwd))
    else:
        candidates = [(None, None)]

    last_err = None
    for u, p in candidates:
        cmd = ["cmd", "/c", "net", "use", unc]
        if u and p:
            cmd += [p, f"/user:{u}"]
        cmd += ["/persistent:no"]
        cp = _run(cmd)
        if cp.returncode == 0 and os.path.isdir(unc):
            return
        last_err = cp.stderr or cp.stdout

    raise RuntimeError(f"net use failed: {last_err or 'unknown error'}")

def _exists_with_retry(path: Path) -> bool:
    try:
        return path.exists()
    except OSError as e:
        if getattr(e, "winerror", None) in _WINERR_RETRY:
            _smb_net_use()
            return path.exists()
        raise

def _stat_with_retry(path: Path):
    try:
        return path.stat()
    except OSError as e:
        if getattr(e, "winerror", None) in _WINERR_RETRY:
            _smb_net_use()
            return path.stat()
        raise

def _open_zip_with_retry(zip_path: Path):
    try:
        return zipfile.ZipFile(zip_path, "r")
    except OSError as e:
        if getattr(e, "winerror", None) in _WINERR_RETRY:
            _smb_net_use()
            return zipfile.ZipFile(zip_path, "r")
        raise

def _read_bytes_with_retry(p: Path) -> bytes:
    try:
        return p.read_bytes()
    except OSError as e:
        if getattr(e, "winerror", None) in _WINERR_RETRY:
            _smb_net_use()
            return p.read_bytes()
        raise

def _attach_roots() -> List[Path]:
    primary = Path(getattr(settings, "ATTACH_BASE_DIR", r"\\10.11.1.74\crrs_attach"))
    fallback = Path(getattr(settings, "ATTACH_FALLBACK_DIR",
                            Path(getattr(settings, "BASE_DIR", Path.cwd())) / "attach_local"))
    force_local = _as_bool(getattr(settings, "ATTACH_FORCE_LOCAL", False))
    return [fallback] if force_local else ([primary] + ([] if str(fallback) == str(primary) else [fallback]))

def _attach_base_dir_for_write() -> Path:
    roots = _attach_roots()
    strict_unc = _as_bool(getattr(settings, "ATTACH_REQUIRE_UNC", False))
    last_err = None

    for i, root in enumerate(roots):
        try:
            if str(root).startswith("\\\\"):
                _smb_net_use()   # uğursuz olarsa Exception atacaq
            root.mkdir(parents=True, exist_ok=True)
            return root
        except Exception as e:
            last_err = e
            if strict_unc and str(root).startswith("\\\\"):
                raise RuntimeError(f"UNC not reachable: {root} — {e}")
            if i == len(roots) - 1:
                raise
            continue

    if last_err:
        raise last_err
    return roots[-1]

@require_GET
def debug_attach(request):
    try:
        chosen = str(_attach_base_dir_for_write())
    except Exception as e:
        chosen = f"ERROR: {e}"
    return JsonResponse({
        "ATTACH_BASE_DIR": getattr(settings, "ATTACH_BASE_DIR", None),
        "ATTACH_FALLBACK_DIR": getattr(settings, "ATTACH_FALLBACK_DIR", None),
        "ATTACH_FORCE_LOCAL": getattr(settings, "ATTACH_FORCE_LOCAL", None),
        "ATTACH_REQUIRE_UNC": getattr(settings, "ATTACH_REQUIRE_UNC", None),
        "ATTACH_SMB_DOMAIN": getattr(settings, "ATTACH_SMB_DOMAIN", None),
        "ATTACH_SMB_USER": getattr(settings, "ATTACH_SMB_USER", None),
        "chosen_for_write": chosen,
    })

def _safe_filename(name: str) -> str:
    name = Path(name).name
    name2 = get_valid_filename(name).replace('..', '').strip(' /\\')
    return name2 or 'file'

def _unique_name(folder: Path, name: str) -> str:
    p = folder / name
    if not p.exists():
        return name
    stem = Path(name).stem
    suf = Path(name).suffix
    i = 1
    while True:
        cand = f"{stem} ({i}){suf}"
        if not (folder / cand).exists():
            return cand
        i += 1

def _allowed_ext(path_or_name: str) -> bool:
    ext = str(path_or_name).lower()
    return any(ext.endswith(e) for e in ALLOWED_ATTACH_EXT)

def _ensure_attach_folder(meta_id: int, base: Optional[Path] = None) -> Path:
    base = base or _attach_base_dir_for_write()
    folder = base / str(int(meta_id))
    folder.mkdir(parents=True, exist_ok=True)
    return folder

def _find_attach_file(meta_id: int, name: str) -> Optional[Path]:
    for root in _attach_roots():
        p = root / str(int(meta_id)) / name
        try:
            if _exists_with_retry(p):
                return p
        except Exception:
            continue
    return None

@csrf_exempt
def attach_upload(request):
    """
    POST multipart:
      - file: .zip | .csv | .txt
      - ticket: string (mütləq)
      - crs: optional (CSV/TXT üçün radio seçimi; DB-yə coordinate_system kimi yazılır)
    """
    if request.method != "POST":
        return HttpResponseBadRequest("POST gözlənirdi.")

    f = request.FILES.get("file")
    ticket = (request.POST.get("ticket") or "").strip()
    if not f:
        return HttpResponseBadRequest("Fayl göndərilməyib.")
    if not ticket:
        return HttpResponseBadRequest("ticket tələb olunur.")
    if not _allowed_ext(f.name):
        return HttpResponseBadRequest("Yalnız .zip, .csv və .txt fayllar qəbul edilir.")

    fk, tok = _redeem_ticket_with_token(ticket)
    if not (fk and tok):
        return _unauthorized()
    meta_id = fk
    uid, ufn = _parse_jwt_user(tok)


    allowed, sid = _is_edit_allowed_for_fk(meta_id)
    if not allowed:
        return JsonResponse(
            {"ok": False, "error": "Bu müraciət statusunda fayl əlavə etmək qadağandır.", "status_id": sid},
            status=403
        )


    if meta_id is None:
        return HttpResponseBadRequest("Ticket nömrəsi aktiv deyil.")

    posted_crs = (request.POST.get("crs") or "").strip().lower()
    ext = Path(f.name).suffix.lower()
    coordinate_system = None
    if ext in {".csv", ".txt"}:
        coordinate_system = _CRS_LABELS.get(posted_crs)

    try:
        base = _attach_base_dir_for_write()
        folder = _ensure_attach_folder(meta_id, base)
        safe_name = _safe_filename(f.name)
        final_name = _unique_name(folder, safe_name)
        dst_path = folder / final_name

        with open(dst_path, "wb") as out:
            for chunk in f.chunks():
                out.write(chunk)

        with connection.cursor() as cur:
            cur.execute("""
                INSERT INTO attach_file (meta_id, attach_name, coordinate_system, status, user_id, user_full_name)
                VALUES (%s, %s, %s, 1, %s, %s)
                RETURNING attach_id
            """, [meta_id, final_name, coordinate_system, uid, ufn])

            attach_id = cur.fetchone()[0]

        return JsonResponse({
            "ok": True,
            "attach_id": attach_id,
            "meta_id": meta_id,
            "attach_name": final_name,
            "coordinate_system": coordinate_system
        })
    except Exception as e:
        return HttpResponseBadRequest(f"Xəta: {e}")

@require_GET
def attach_list_by_ticket(request):
    ticket = (request.GET.get("ticket") or "").strip()
    if not ticket:
        return HttpResponseBadRequest("ticket tələb olunur.")
    meta_id = _redeem_ticket(ticket)
    if meta_id is None:
        return _unauthorized()

    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT attach_id, attach_name, coordinate_system
                FROM attach_file
                WHERE meta_id = %s
                AND COALESCE(status,1) = 1
                ORDER BY attach_id DESC
            """, [meta_id])
            rows = cur.fetchall()

        items = []
        for aid, name, coord_sys in rows:
            p = _find_attach_file(meta_id, name)
            exists = False
            size = None
            if p:
                try:
                    exists = _exists_with_retry(p)
                    if exists:
                        size = _stat_with_retry(p).st_size
                except Exception:
                    exists, size = False, None
            ext = Path(name).suffix.lower()
            items.append({
                "attach_id": aid,
                "attach_name": name,
                "exists": exists,
                "size": size,
                "ext": ext,
                "has_geometry": ext in {".zip", ".csv", ".txt"},
                "coordinate_system": coord_sys
            })
        return JsonResponse({"ok": True, "meta_id": meta_id, "items": items})
    except Exception as e:
        return HttpResponseBadRequest(f"Xəta: {e}")



@require_GET
def kateqoriya_name_by_tekuis_code(request):
    """
    GET /api/dict/kateqoriya/by-tekuis-code?code=88001
    Qaytarır: { ok: True, code: "88001", name: "..." } və ya { ok: False }
    """
    code = (request.GET.get("code") or "").strip()
    if not code:
        return JsonResponse({"ok": False, "error": "code is required"}, status=400)

    try:
        with connection.cursor() as cur:
            # həm int, həm text tiplərini rahat tutmaq üçün ::text ilə müqayisə edirik
            cur.execute("""
                SELECT kateqoriya_tekuis_name
                  FROM kateqoriya
                 WHERE kateqoriya_tekuis_code::text = %s
                 LIMIT 1
            """, [code])
            row = cur.fetchone()
        if not row or row[0] in (None, ""):
            return JsonResponse({"ok": False, "code": code}, status=404)
        return JsonResponse({"ok": True, "code": code, "name": row[0]})
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)


@require_GET
def kateqoriya_name_by_ticket(request):
    """
    GET /api/dict/kateqoriya/by-ticket?ticket=XXXX
    1) Node redeem-dən tekuisId götür
    2) kateqoriya cədvəlindən kateqoriya_tekuis_name tap
    3) { ok: True, code: "...", name: "..." } qaytar
    """
    ticket = (request.GET.get("ticket") or "").strip()
    if not ticket:
        return JsonResponse({"ok": False, "error": "ticket is required"}, status=400)

    # 1) Node redeem çağır
    url = getattr(settings, "NODE_REDEEM_URL",
                  "http://10.11.1.73:8080/api/requests/handoff/redeem").rstrip("/")
    timeout = int(getattr(settings, "NODE_REDEEM_TIMEOUT", 8))
    bearer = getattr(settings, "NODE_REDEEM_BEARER", None)

    headers = {"Accept": "application/json"}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"

    try:
        resp = requests.post(
            url,
            data={"ticket": ticket},
            headers={**headers, "Content-Type": "application/x-www-form-urlencoded"},
            timeout=timeout,
        )
        if resp.status_code != 200:
            return JsonResponse(
                {"ok": False, "error": f"redeem HTTP {resp.status_code}"},
                status=resp.status_code,
            )
        data = resp.json()
    except Exception as e:
        return JsonResponse(
            {"ok": False, "error": f"redeem error: {e}"},
            status=500,
        )

    tekuis_id = data.get("tekuisId")
    if tekuis_id in (None, ""):
        return JsonResponse(
            {"ok": False, "error": "tekuisId not found in redeem"},
            status=404,
        )

    code_str = str(tekuis_id).strip()

    # 2) kateqoriya cədvəlindən adı tap
    try:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT kateqoriya_tekuis_name
                  FROM kateqoriya
                 WHERE kateqoriya_tekuis_code::text = %s
                 LIMIT 1
            """,
                [code_str],
            )
            row = cur.fetchone()
    except Exception as e:
        return JsonResponse(
            {"ok": False, "error": f"DB error: {e}"},
            status=500,
        )

    if not row or not row[0]:
        return JsonResponse(
            {"ok": False, "error": "kateqoriya not found for code", "code": code_str},
            status=404,
        )

    return JsonResponse(
        {"ok": True, "code": code_str, "name": row[0]},
        status=200,
    )






# ---- ZIP (SHP) üçün GEOJSON çevirmə ----
def _geojson_from_zip_file(zip_path: Path) -> dict:
    tmpdir = Path(tempfile.mkdtemp(prefix="att_"))
    try:
        with _open_zip_with_retry(zip_path) as z:
            z.extractall(tmpdir)
        shp_path = _find_main_shp(tmpdir)
        r = shapefile.Reader(str(shp_path))
        if r.numRecords == 0:
            return {"type": "FeatureCollection", "features": []}
        first_shape = r.shape(0)
        first_xy = first_shape.points[0]
        transformer = _make_transformer(shp_path, first_xy)

        features = []
        for i in range(r.numRecords):
            s = r.shape(i)
            rec = r.shapeRecord(i)
            geom = _shape_to_geojson_geometry(s, transformer)
            props = _records_as_props(r, rec)
            features.append({"type": "Feature", "geometry": geom, "properties": props})
        return {"type": "FeatureCollection", "features": features}
    finally:
        try:
            shutil.rmtree(tmpdir)
        except Exception:
            pass

# ---- CSV/TXT üçün GEOJSON çevirmə (attach üçün) ----
def _candidate_point_transformers():
    return [
        ('wgs84', None),
        ('utm38', Transformer.from_crs(CRS.from_epsg(32638), CRS.from_epsg(4326), always_xy=True)),
        ('utm39', Transformer.from_crs(CRS.from_epsg(32639), CRS.from_epsg(4326), always_xy=True)),
    ]

def _score_transformer_on_rows(rows, x_idx, y_idx, transformer, sample_limit=200):
    cnt = 0
    checked = 0
    for r in rows:
        if len(r) <= max(x_idx, y_idx):
            continue
        try:
            x = float(str(r[x_idx]).strip().replace(',', '.'))
            y = float(str(r[y_idx]).strip().replace(',', '.'))
        except Exception:
            continue

        if transformer is not None:
            try:
                lon, lat = transformer.transform(x, y)
            except Exception:
                continue
        else:
            lon, lat = x, y

        if all(isfinite(v) for v in (lon, lat)) and (-180 <= lon <= 180) and (-90 <= lat <= 90):
            cnt += 1

        checked += 1
        if checked >= sample_limit:
            break
    return cnt

def _auto_pick_points_transformer(rows, x_idx, y_idx):
    best = ('wgs84', None)
    best_score = -1
    for name, tr in _candidate_point_transformers():
        score = _score_transformer_on_rows(rows, x_idx, y_idx, tr)
        if score > best_score:
            best = (name, tr)
            best_score = score
    return best  # (name, transformer)

def _geojson_from_csvtxt_file(txt_path: Path, crs_choice: str = "auto") -> dict:
    data_bytes = _read_bytes_with_retry(txt_path)
    text = _decode_bytes_to_text(data_bytes)
    sample = text[:4096]
    dialect = _sniff_dialect(sample)
    reader = csv.reader(io.StringIO(text), dialect)
    rows = list(reader)
    if not rows:
        return {"type": "FeatureCollection", "features": []}

    has_header = csv.Sniffer().has_header(sample) if len(rows) > 1 else False
    if has_header:
        header, body = rows[0], rows[1:]
    else:
        max_len = max(len(r) for r in rows)
        header = [f'col{i+1}' for i in range(max_len)]
        body = rows

    x_idx, y_idx = _find_xy_columns(header)
    if x_idx is None or y_idx is None:
        if len(header) >= 2:
            x_idx, y_idx = 0, 1
        else:
            return {"type": "FeatureCollection", "features": []}

    crs_idx = _find_crs_column(header)

    choice = (crs_choice or 'auto').lower()
    chosen_name = choice
    transformer = _build_transformer_for_points(choice)

    if (choice in ('auto', 'detect')) and (crs_idx is None):
        chosen_name, transformer = _auto_pick_points_transformer(body, x_idx, y_idx)

    features = []
    for r in body:
        if len(r) <= max(x_idx, y_idx):
            continue
        try:
            x = float(str(r[x_idx]).strip().replace(',', '.'))
            y = float(str(r[y_idx]).strip().replace(',', '.'))
        except Exception:
            continue

        row_transformer = transformer
        row_crs_code = None
        if crs_idx is not None and crs_idx < len(r):
            row_crs_code = _canonize_crs_value(r[crs_idx])
            if row_crs_code:
                row_transformer = _build_transformer_for_points(row_crs_code)

        if row_transformer:
            try:
                lon, lat = row_transformer.transform(x, y)
            except Exception:
                continue
        else:
            lon, lat = x, y

        if not (-180 <= lon <= 180 and -90 <= lat <= 90):
            continue

        props = {}
        for i, val in enumerate(r):
            if i in (x_idx, y_idx):
                continue
            key = header[i] if i < len(header) else f'col{i+1}'
            props[key] = val

        if row_crs_code:
            props.setdefault("_crs_used", row_crs_code)
        else:
            props.setdefault("_crs_used", chosen_name)

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props
        })
    return {"type": "FeatureCollection", "features": features}

@require_GET
def attach_geojson(request, attach_id: int):
    req_crs = (request.GET.get("crs") or "auto").lower()

    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT meta_id, attach_name, coordinate_system
                FROM attach_file
                WHERE attach_id = %s
                AND COALESCE(status,1) = 1
            """, [attach_id])

            row = cur.fetchone()
        if not row:
            return HttpResponseBadRequest("attach tapılmadı.")
        meta_id, name, coord_label = row

        try:
            _smb_net_use()
        except Exception:
            pass

        p = _find_attach_file(meta_id, name)
        if not p:
            return HttpResponseBadRequest("Fayl tapılmadı.")

        ext = p.suffix.lower()
        if ext == ".zip":
            fc = _geojson_from_zip_file(p)
        elif ext in {".csv", ".txt"}:
            db_code = _canonize_crs_value(coord_label) if coord_label else None
            choice = db_code or req_crs or "auto"
            fc = _geojson_from_csvtxt_file(p, crs_choice=choice)
        else:
            return HttpResponseBadRequest("Dəstəklənməyən attach fayl növü.")

        for ftr in fc.get("features", []):
            props = ftr.setdefault("properties", {})
            props.setdefault("attach_id", int(attach_id))
            props.setdefault("attach_name", name)

        return JsonResponse(fc, safe=False)
    except Exception as e:
        return HttpResponseBadRequest(f"Xəta: {e}")


@require_GET
def attach_geojson_by_ticket(request):
    ticket = (request.GET.get("ticket") or "").strip()
    req_crs = (request.GET.get("crs") or "auto").lower()
    if not ticket:
        return HttpResponseBadRequest("ticket tələb olunur.")
    meta_id = _redeem_ticket(ticket)
    if meta_id is None:
        return _unauthorized()

    with connection.cursor() as cur:
        cur.execute("""
            SELECT COUNT(1)
            FROM gis_data
            WHERE fk_metadata = %s
            AND COALESCE(status,1) = 1
        """, [meta_id])
        active_cnt = cur.fetchone()[0]
    if not active_cnt:
        return JsonResponse({"type": "FeatureCollection", "features": []}, safe=False)



    if meta_id is None:
        return HttpResponseBadRequest("Ticket nömrəsi aktiv deyil.")

    try:
        with connection.cursor() as cur:
            cur.execute("""
                SELECT attach_id, attach_name, coordinate_system
                FROM attach_file
                WHERE meta_id = %s
                AND COALESCE(status,1) = 1
                ORDER BY attach_id
            """, [meta_id])
            rows = cur.fetchall()

        try:
            _smb_net_use()
        except Exception:
            pass

        out_features = []
        for aid, name, coord_label in rows:
            p = _find_attach_file(meta_id, name)
            if not p:
                continue
            ext = p.suffix.lower()
            if ext == ".zip":
                fc = _geojson_from_zip_file(p)
            elif ext in {".csv", ".txt"}:
                db_code = _canonize_crs_value(coord_label) if coord_label else None
                choice = db_code or req_crs or "auto"
                fc = _geojson_from_csvtxt_file(p, crs_choice=choice)
            else:
                continue
            for ftr in fc.get("features", []):
                props = ftr.setdefault("properties", {})
                props.setdefault("attach_id", int(aid))
                props.setdefault("attach_name", name)
                props.setdefault("meta_id", int(meta_id))
                out_features.append(ftr)

        return JsonResponse({"type": "FeatureCollection", "features": out_features}, safe=False)
    except Exception as e:
        return HttpResponseBadRequest(f"Xəta: {e}")


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
    table  = getattr(settings, "TEKUIS_TABLE",  os.getenv("TEKUIS_TABLE",  "M_G_PARSEL"))

    sql = f"""
        SELECT sde.st_astext(t.SHAPE) AS wkt,
               t.LAND_CATEGORY2ENUM, t.LAND_CATEGORY_ENUM, t.NAME, t.OWNER_TYPE_ENUM,
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
    table  = os.getenv("TEKUIS_TABLE",  "M_G_PARSEL")
    table_srid = int(os.getenv("TEKUIS_TABLE_SRID", 4326))  # cədvəl SRID

    # --- TEKUİS atributları (SELECT və properties üçün eyni sıra)
    TEKUIS_ATTRS = (
        "LAND_CATEGORY2ENUM", "LAND_CATEGORY_ENUM", "NAME", "OWNER_TYPE_ENUM",
        "SUVARILMA_NOVU_ENUM", "EMLAK_NOVU_ENUM", "OLD_LAND_CATEGORY2ENUM",
        "TERRITORY_NAME", "RAYON_ADI", "IED_ADI", "BELEDIYE_ADI","LAND_CATEGORY3ENUM","LAND_CATEGORY4ENUM", "AREA_HA"
    )
    ATTR_SQL = ", ".join([f"t.{c}" for c in TEKUIS_ATTRS])

    def _props_from_vals(vals):
        return {k: v for k, v in zip(TEKUIS_ATTRS, vals)}

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

    NUM_RE  = r'[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?'
    TYPE_RE = r'(?:POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)'

    def _normalize_wkt_remove_m_dims(w: str) -> str:
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
        """WKTdən sonrakı zibili kəs (məs: 'POLYGON((...))49.80…' → 'POLYGON((...))')."""
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

    # Input sanitizasiya
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
        print(f"[TEKUIS][GEOM][input_sanitize] all invalid. empty={bad_empty}, curved={bad_curved}, parse={bad_parse}")
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
                   {ATTR_SQL}
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
                   {ATTR_SQL}
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
            if re.search(r'\b(CURVEPOLYGON|CIRCULARSTRING|COMPOUNDCURVE|ELLIPTICARC|MULTICURVE|MULTISURFACE)\b', w, flags=re.I):
                out_skip_curved += 1
                continue

            # tail kəs + M/ZM → 2D
            w2 = _clip_tail(w)
            if w2 != w:
                out_tailfix += 1
            m_hdr = re.match(rf'^\s*(?:{TYPE_RE})\s+(ZM|M)\b', w2, flags=re.I)
            if m_hdr:
                dim = m_hdr.group(1).upper()
                if dim == 'ZM':
                    w2 = re.sub(rf'\b({TYPE_RE})\s+ZM\b', r'\1 Z', w2, flags=re.I)
                    w2 = re.sub(rf'({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})', r'\1 \2 \3', w2)
                else:
                    w2 = re.sub(rf'\b({TYPE_RE})\s+M\b', r'\1', w2, flags=re.I)
                    w2 = re.sub(rf'({NUM_RE})\s+({NUM_RE})\s+({NUM_RE})', r'\1 \2', w2)
            try:
                geom = _wkt.loads(w2)
            except Exception:
                out_skip_parse += 1
                continue

            props = _props_from_vals(attr_vals)
            props["SOURCE"] = "TEKUIS"

            features.append({"type": "Feature", "geometry": mapping(geom), "properties": props})
            if rid_key:
                seen_rids.add(rid_key)

    with _oracle_connect() as cn:
        with cn.cursor() as cur:
            CHUNK = 200
            for start in range(0, len(safe_wkts), CHUNK):
                sub = safe_wkts[start:start + CHUNK]
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
                                       {ATTR_SQL}
                                  FROM {schema}.{table} t
                                  JOIN ids ON t.ROWID = ids.rid
                                """,
                                {"w": w, "srid_in": int(srid_in), "bufm": float(buf_m), "table_srid": int(table_srid)}
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
                                    {"wkb": wkb_hex, "srid_in": int(srid_in), "bufm": float(buf_m), "table_srid": int(table_srid)}
                                )
                                _consume_cursor(cur)
                                ok = True
                            except Exception as e2:
                                head = (w[:220] + "…") if len(w) > 220 else w
                                print(f"[TEKUIS][GEOM] skipped one WKT due to SDE error.\nWKT head: {head}\nWKB fallback err: {str(e2)[:240]}")
                        if not ok:
                            continue

    print(f"[TEKUIS][GEOM] input_sanitized={len(safe_wkts)} dropped={bad_empty+bad_curved+bad_parse} "
          f"(empty={bad_empty}, curved={bad_curved}, parse={bad_parse}) srid_in={srid_in} table_srid={table_srid} buf_m={buf_m}")
    print(f"[TEKUIS][GEOM] returned={len(features)} unique_rids={len(seen_rids)} "
          f"skipped_out={out_skip_empty+out_skip_curved+out_skip_parse} tailfix={out_tailfix}")

    return JsonResponse({"type": "FeatureCollection", "features": features}, safe=False)






# ==========================
# Debug
# ==========================
@require_GET
def debug_mssql(request):
    rowid = request.GET.get("rowid")
    out = {
        "host": getattr(settings, "MSSQL_HOST", None),
        "port": getattr(settings, "MSSQL_PORT", None),
        "db": getattr(settings, "MSSQL_NAME", None),
        "user": getattr(settings, "MSSQL_USER", None),
        "driver": getattr(settings, "MSSQL_DRIVER", None),
        "encrypt": getattr(settings, "MSSQL_ENCRYPT", None),
        "trust_cert": getattr(settings, "MSSQL_TRUST_CERT", None),
        "schema": getattr(settings, "MSSQL_SCHEMA", "dbo"),
    }
    try:
        with _mssql_connect() as cn:
            cur = cn.cursor()
            cur.execute("SELECT DB_NAME(), SUSER_SNAME(), SCHEMA_NAME()")
            dbname, suser, schema = cur.fetchone()
            out.update({"connected": True, "server_db": dbname, "login": suser, "default_schema": schema})

            cur.execute("""
                SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'TBL_REQUEST_REG'
            """, (out["schema"],))
            out["table_exists"] = bool(cur.fetchone()[0])

            if rowid:
                cur.execute(f"SELECT COUNT(1) FROM {out['schema']}.TBL_REQUEST_REG WHERE ROW_ID = ?", (int(rowid),))
                out["row_exists_ROW_ID"] = bool(cur.fetchone()[0])
    except Exception as e:
        out.update({"connected": False, "error": str(e)})
    return JsonResponse(out)


@require_GET
def debug_odbc(request):
    info = {
        "env_driver_from_settings": getattr(settings, "MSSQL_DRIVER", None),
        "drivers_on_system": [],
    }
    try:
        info["drivers_on_system"] = list(pyodbc.drivers())
    except Exception as e:
        info["drivers_error"] = str(e)
    return JsonResponse(info)





# --- YENİ: attach-lardan WKT toplamaq üçün köməkçi ---
def _collect_attach_wkts_for_meta(meta_id: int, req_crs: str = "auto") -> List[str]:
    """
    Verilmiş meta_id üçün aktiv attach fayllarını oxuyur,
    onların içindəki bütün geometriyaları ayrı-ayrılıqda WKT kimi qaytarır.
    """
    wkts: List[str] = []

    with connection.cursor() as cur:
        cur.execute("""
            SELECT attach_id, attach_name, coordinate_system
            FROM attach_file
            WHERE meta_id = %s
              AND COALESCE(status,1) = 1
            ORDER BY attach_id
        """, [meta_id])
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
    NUM_RE = r'[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[Ee][+-]?\d+)?'
    TYPE_RE = r'(?:POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON)'
    def _normalize_wkt_remove_m_dims(w: str) -> str:
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

    # WKT bitdikdən sonrakı “tail”i kəsən funksiya
    def _clip_to_first_geometry(w: str) -> str:
        lvl = 0
        last = -1
        for i, ch in enumerate(w):
            if ch == '(':
                lvl += 1
            elif ch == ')':
                lvl -= 1
                if lvl == 0:
                    last = i
                    break
        return w[:last+1] if last >= 0 else w

    schema = os.getenv("TEKUIS_SCHEMA", "BTG_MIS")
    table  = os.getenv("TEKUIS_TABLE",  "M_G_PARSEL")
    max_features = int(os.getenv("TEKUIS_MAX_FEATURES", "20000"))
    row_limit = int(limit or max_features)

    with _oracle_connect() as cn:
        with cn.cursor() as cur:
            CHUNK = 200
            for start in range(0, len(wkt_list), CHUNK):
                if row_limit is not None and row_limit <= 0:
                    break

                chunk = wkt_list[start:start + CHUNK]
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
                    sql_fb = f"""
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
                    """
                    cur.execute(sql_fb, params)

                for rid, wkt_lob in cur:
                    rid_key = None
                    try:
                        rid_key = str(rid)
                    except Exception:
                        pass
                    if rid_key and rid_key in seen_rids:
                        continue

                    raw = wkt_lob.read() if hasattr(wkt_lob, "read") else wkt_lob
                    w = _clean_wkt_text(raw)
                    if not w:
                        skipped_empty += 1
                        continue

                    # Curve tiplərini at
                    if re.search(r'\b(CURVEPOLYGON|CIRCULARSTRING|COMPOUNDCURVE|ELLIPTICARC|MULTICURVE|MULTISURFACE)\b',
                                 w, flags=re.I):
                        skipped_curved += 1
                        continue

                    # Tail kəs + M/ZM normallaşdır
                    w2 = _normalize_wkt_remove_m_dims(_clip_to_first_geometry(w))

                    try:
                        geom = shapely_wkt.loads(w2)
                    except Exception as e:
                        skipped_parse += 1
                        if logged_parse_examples < 3:
                            head = (w[:280] + '…') if len(w) > 280 else w
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
    print(f"[TEKUIS][ATTACH] returned={len(features)} unique_rids={len(seen_rids)} "
          f"skipped_total={skipped_total} (empty={skipped_empty}, curved={skipped_curved}, parse={skipped_parse}) "
          f"srid={srid} buf_m={buf_m}")
    return features


# --- YENİ: Nöqtə attach olsa belə TEKUIS parsellərini gətirən endpoint ---
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
    knorm = ''.join(ch for ch in key.lower() if ch.isalnum())
    for k, v in props.items():
        kn = ''.join(ch for ch in str(k).lower() if ch.isalnum())
        if kn == knorm:
            return v
    return None


def _to_float_or_none(v):
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        return float(str(v).replace(',', '.'))
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


def _insert_tekuis_parcel_rows(meta_id: int, ticket: str, features: list, replace: bool = True,
                               user_id: Optional[int] = None, user_full_name: Optional[str] = None):

    saved = 0
    skipped = 0
    deactivated = 0

    with transaction.atomic():
        with connection.cursor() as cur:
            if replace:
                cur.execute("""
                    UPDATE tekuis_parcel
                       SET status = 0,
                           last_edited_date = now()
                     WHERE meta_id = %s
                       AND COALESCE(status,1) = 1
                """, [meta_id])
                deactivated = cur.rowcount or 0

            for f in (features or []):
                geom = f.get("geometry") or {}
                gtype = (geom.get("type") or "").lower()
                if "polygon" not in gtype:  # yalnız (Multi)Polygon saxlayırıq
                    skipped += 1
                    continue

                props = f.get("properties") or {}
                colvals = {
                    "kateqoriya":         _prop_ci(props, "LAND_CATEGORY_ENUM"),
                    "uqodiya":            _prop_ci(props, "LAND_CATEGORY2ENUM"),
                    "alt_kateqoriya":     _prop_ci(props, "LAND_CATEGORY3ENUM"),
                    "alt_uqodiya":        _prop_ci(props, "LAND_CATEGORY4ENUM"),
                    "islahat_uqodiyasi":  _prop_ci(props, "OLD_LAND_CATEGORY2ENUM"),
                    "mulkiyyet":          _prop_ci(props, "OWNER_TYPE_ENUM"),
                    "suvarma":            _prop_ci(props, "SUVARILMA_NOVU_ENUM"),
                    "emlak_novu":         _prop_ci(props, "EMLAK_NOVU_ENUM"),
                    "rayon_adi":          _prop_ci(props, "RAYON_ADI"),
                    "ied_adi":            _prop_ci(props, "IED_ADI"),
                    "belediyye_adi":      _prop_ci(props, "BELEDIYE_ADI"),
                    "sahe_ha":            _to_float_or_none(_prop_ci(props, "AREA_HA")),
                    "qeyd":               _prop_ci(props, "NAME"),
                }

                geom_json = json.dumps(geom)

                cur.execute("""
                    INSERT INTO tekuis_parcel (
                        kateqoriya, uqodiya, alt_kateqoriya, alt_uqodiya,
                        mulkiyyet, suvarma, emlak_novu, islahat_uqodiyasi,
                        rayon_adi, ied_adi, belediyye_adi,
                        sahe_ha, qeyd, geom,
                        meta_id, created_date, last_edited_date, status,
                        user_id, user_full_name
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        ST_Multi( ST_Buffer( ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326), 0) ),
                        %s, now(), now(), 1,
                        %s, %s
                    )
                    RETURNING tekuis_id
                """, [
                    colvals["kateqoriya"], colvals["uqodiya"], colvals["alt_kateqoriya"], colvals["alt_uqodiya"],
                    colvals["mulkiyyet"], colvals["suvarma"], colvals["emlak_novu"], colvals["islahat_uqodiyasi"],
                    colvals["rayon_adi"], colvals["ied_adi"], colvals["belediyye_adi"],
                    colvals["sahe_ha"], colvals["qeyd"],
                    geom_json,
                    int(meta_id),
                    user_id, user_full_name,
                ])


                _ = cur.fetchone()[0]  # lazım olsa istifadə et
                saved += 1

    return {"saved": saved, "skipped": skipped, "deactivated": deactivated}






def _json_body(request):
    try:
        raw = request.body.decode('utf-8') if request.body else ''
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
    meta_hdr = request.headers.get('X-Meta-Id')
    meta_qs  = request.GET.get('meta_id')
    meta_bd  = _json_body(request).get('meta_id')
    for m in (meta_hdr, meta_qs, meta_bd):
        if m is not None:
            try:
                return int(m)
            except Exception:
                pass

    # 2) Ticket-dən türet
    body = _json_body(request)
    ticket = (
        request.headers.get('X-Ticket')
        or request.GET.get('ticket')
        or request.POST.get('ticket')
        or body.get('ticket')
        or ''
    )
    ticket = str(ticket).strip()
    return int(zlib.crc32(ticket.encode('utf-8')) & 0x7FFFFFFF)

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
    min_gap     = data.get("min_gap_sqm")

    # Dissolve olunmuş kimi görünürmü? (tək Polygon/MultiPolygon gəlibsə)
    feats = (gj or {}).get("features", [])
    looks_dissolved = (
        len(feats) == 1 and
        ((feats[0].get("geometry") or {}).get("type") in ("Polygon", "MultiPolygon"))
    )

    res = validate_tekuis(gj, meta_id,
                          min_overlap_sqm=min_overlap,
                          min_gap_sqm=min_gap)

    out = {"ok": True, "validation": res}
    if looks_dissolved:
        out["warning"] = "features_look_dissolved"  # Fronta göstərə bilərsən
    return JsonResponse(out)


@csrf_exempt
def ignore_tekuis_gap(request):
    if request.method != 'POST':
        return JsonResponse({"ok": False, "error": "POST only"}, status=405)

    data = _json_body(request)
    meta_id = _meta_id_from_request(request)
    h = data.get('hash')
    geom = data.get('geom')
    if not h:
        return JsonResponse({"ok": False, "error": "hash required"}, status=400)

    ok = ignore_gap(meta_id, h, geom)
    return JsonResponse({"ok": bool(ok)}, status=200 if ok else 500)





# ↑ faylın yuxarı hissəsində already: from .tekuis_validation import validate_tekuis, ignore_gap

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
    min_ov = float(getattr(settings, "TEKUIS_VALIDATION_MIN_OVERLAP_SQM",
                           getattr(settings, "TEKUIS_VALIDATION_MIN_AREA_SQM", 1.0)))
    min_ga = float(getattr(settings, "TEKUIS_VALIDATION_MIN_GAP_SQM",
                           getattr(settings, "TEKUIS_VALIDATION_MIN_AREA_SQM", 1.0)))

    res = validate_tekuis(geojson, int(meta_id),
                          min_overlap_sqm=min_ov, min_gap_sqm=min_ga)

    has_err = (res.get("stats", {}).get("overlap_count", 0) > 0 or
               res.get("stats", {}).get("gap_count", 0) > 0)

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
    if request.method != 'POST':
        return JsonResponse({"ok": False, "error": "POST only"}, status=405)

    data = _json_body(request)
    fc = data.get('geojson') or {}
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
        request.headers.get('X-Ticket')
        or request.GET.get('ticket')
        or request.POST.get('ticket')
        or data.get('ticket') or ''
    ).strip()
    if not ticket:
        return JsonResponse({"ok": False, "error": "ticket tələb olunur"}, status=400)


        # === NEW: İKİNCİ DƏFƏ SAXLAMAYA QADAĞA (aktiv sətirlər varsa) ===
    if _has_active_tekuis(int(meta_id)):
        return JsonResponse({
            "ok": False,
            "code": "ALREADY_SAVED",
            "message": "TEKUİS parsellər local bazada yadda saxlanılıb"
        }, status=409)



    skip_validation = bool(data.get('skip_validation', False))
    if not skip_validation:
        min_ov = float(getattr(settings, "TEKUIS_VALIDATION_MIN_OVERLAP_SQM",
                               getattr(settings, "TEKUIS_VALIDATION_MIN_AREA_SQM", 1.0)))
        min_ga = float(getattr(settings, "TEKUIS_VALIDATION_MIN_GAP_SQM",
                               getattr(settings, "TEKUIS_VALIDATION_MIN_AREA_SQM", 1.0)))

        v = validate_tekuis(fc, int(meta_id),
                            min_overlap_sqm=min_ov, min_gap_sqm=min_ga)

        # ignored-ları müxtəlif formatlarda dəstəklə
        def _collect_ignored_keys(payload_ignored: dict):
            ov = (payload_ignored.get('overlap_keys')
                  or payload_ignored.get('overlaps')
                  or payload_ignored.get('overlap_hashes')
                  or payload_ignored.get('ignored_overlap_keys')
                  or [])
            gp = (payload_ignored.get('gap_keys')
                  or payload_ignored.get('gaps')
                  or payload_ignored.get('gap_hashes')
                  or payload_ignored.get('ignored_gap_keys')
                  or [])
            return set(map(str, ov)), set(map(str, gp))

        ignored = data.get('ignored') or {}
        ignored_overlap_keys, ignored_gap_keys = _collect_ignored_keys(ignored)

        def _eff_key(obj):
            if isinstance(obj, dict) and (obj.get('key') or obj.get('hash')):
                return str(obj.get('key') or obj.get('hash'))
            return _topo_key_py(obj)

        effective_overlaps = [o for o in (v.get('overlaps') or []) if _eff_key(o) not in ignored_overlap_keys]
        effective_gaps     = [g for g in (v.get('gaps') or [])     if _eff_key(g) not in ignored_gap_keys]

        if effective_overlaps or effective_gaps:
            return JsonResponse({"ok": False, "validation": v}, status=422)


    try:
        uid = getattr(request, "user_id_from_token", None)
        ufn = getattr(request, "user_full_name_from_token", None)

        res = _insert_tekuis_parcel_rows(
            meta_id=int(meta_id), ticket=ticket,
            features=features, replace=bool(data.get("replace", True)),
            user_id=uid, user_full_name=ufn
        )

        return JsonResponse({
            "ok": True, "meta_id": int(meta_id), "ticket": ticket,
            "saved_count": res["saved"],
            "skipped_non_polygon": res["skipped"],
            "deactivated_old": res["deactivated"],
        }, status=200)
    except Exception as e:
        return JsonResponse({"ok": False, "error": str(e)}, status=500)




@require_GET
@require_valid_ticket
def attributes_options(request):
    """
    Atribut select-ləri üçün mapping listlərini qaytarır.
    """
    def fetch(sel_sql):
        with connection.cursor() as cur:
            cur.execute(sel_sql)
            rows = cur.fetchall()
        # (code, name) qaytarırıq
        return [{"code": r[0], "name": r[1]} for r in rows]

    data = {
        # mənbə cədvəl/sütunlar: name sütunları *_tekuis_name, code sütunları *_tekuis_code
        "uqodiya":       fetch("SELECT uqodiya_tekuis_code, uqodiya_tekuis_name   FROM uqodiya       ORDER BY uqodiya_tekuis_name"),
        "kateqoriya":    fetch("SELECT kateqoriya_tekuis_code, kateqoriya_tekuis_name FROM kateqoriya    ORDER BY kateqoriya_tekuis_name"),
        "mulkiyyet":     fetch("SELECT mulkiyyet_tekuis_code, mulkiyyet_tekuis_name  FROM mulkiyyet     ORDER BY mulkiyyet_tekuis_name"),
        "suvarma":       fetch("SELECT suvarma_tekuis_code, suvarma_tekuis_name      FROM suvarma       ORDER BY suvarma_tekuis_name"),
        "emlak":         fetch("SELECT emlak_tekuis_code, emlak_tekuis_name          FROM emlak         ORDER BY emlak_tekuis_name"),
        "alt_kateqoriya":fetch("SELECT alt_kate_tekuis_code, alt_kate_tekuis_name    FROM alt_kateqoriya ORDER BY alt_kate_tekuis_name"),
        "alt_uqodiya":   fetch("SELECT alt_uqo_tekuis_code,  alt_uqo_tekuis_name     FROM alt_uqodiya   ORDER BY alt_uqo_tekuis_name"),
    }
    return JsonResponse({"ok": True, "data": data})




def _topo_key_py(obj):
    """
    JavaScript topoKey() funksiyasının server tərəfi ekvivalenti.
    Əgər obyektin içində 'key' və ya 'hash' varsa, birbaşa onu istifadə edirik ki,
    Frontend-lə eyni identifikator alınsın. Əks halda geometriyadan açar yaradırıq.
    """
    import hashlib, random
    try:
        if isinstance(obj, dict):
            k = obj.get('key') or obj.get('hash')
            if isinstance(k, str) and k:
                return k
            g = obj.get('geom')
        else:
            g = obj
        norm = json.dumps(_round_deep_py(g, 6), sort_keys=True)
        return 'k' + hashlib.md5(norm.encode()).hexdigest()[:12]
    except Exception:
        return 'k' + ''.join(random.choices('0123456789abcdef', k=12))

def _round_deep_py(x, d=6):
    """Rekursiv olaraq ədədləri yuvarlaqlaqlaşdır"""
    if isinstance(x, list):
        return [_round_deep_py(v, d) for v in x]
    elif isinstance(x, float):
        return round(x, d)
    elif isinstance(x, dict):
        return {k: _round_deep_py(v, d) for k, v in sorted(x.items())}
    return x

