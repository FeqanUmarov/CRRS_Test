import csv
import io
import os
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import List, Optional

import shapefile  # pyshp
from django.conf import settings
from django.db import connection
from django.http import HttpResponseBadRequest, JsonResponse
from django.utils.text import get_valid_filename
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET
from pyproj import CRS, Transformer

from .auth import _parse_jwt_user, _redeem_ticket, _redeem_ticket_with_token, _unauthorized
from .geo_utils import (
    _build_transformer_for_points,
    _canonize_crs_value,
    _decode_bytes_to_text,
    _find_crs_column,
    _find_main_shp,
    _find_xy_columns,
    _flatten_geoms,
    _make_transformer,
    _records_as_props,
    _shape_to_geojson_geometry,
    _sniff_dialect,
)
from .mssql import _as_bool, _is_edit_allowed_for_fk


# ==========================
# ATTACH: UNC → lokal fallback
# ==========================
ALLOWED_ATTACH_EXT = {".zip", ".csv", ".txt"}


_CRS_LABELS = {
    "wgs84": "WGS84 (lon/lat)",
    "utm38": "UTM 38N",
    "utm39": "UTM 39N",
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
    fallback = Path(
        getattr(settings, "ATTACH_FALLBACK_DIR", Path(getattr(settings, "BASE_DIR", Path.cwd())) / "attach_local")
    )
    force_local = _as_bool(getattr(settings, "ATTACH_FORCE_LOCAL", False))
    return [fallback] if force_local else ([primary] + ([] if str(fallback) == str(primary) else [fallback]))


def _attach_base_dir_for_write() -> Path:
    roots = _attach_roots()
    strict_unc = _as_bool(getattr(settings, "ATTACH_REQUIRE_UNC", False))
    last_err = None

    for i, root in enumerate(roots):
        try:
            if str(root).startswith("\\\\"):
                _smb_net_use()  # uğursuz olarsa Exception atacaq
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
    return JsonResponse(
        {
            "ATTACH_BASE_DIR": getattr(settings, "ATTACH_BASE_DIR", None),
            "ATTACH_FALLBACK_DIR": getattr(settings, "ATTACH_FALLBACK_DIR", None),
            "ATTACH_FORCE_LOCAL": getattr(settings, "ATTACH_FORCE_LOCAL", None),
            "ATTACH_REQUIRE_UNC": getattr(settings, "ATTACH_REQUIRE_UNC", None),
            "ATTACH_SMB_DOMAIN": getattr(settings, "ATTACH_SMB_DOMAIN", None),
            "ATTACH_SMB_USER": getattr(settings, "ATTACH_SMB_USER", None),
            "chosen_for_write": chosen,
        }
    )


def _safe_filename(name: str) -> str:
    name = Path(name).name
    name2 = get_valid_filename(name).replace("..", "").strip(" /\\")
    return name2 or "file"


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
            status=403,
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
            cur.execute(
                """
                INSERT INTO attach_file (meta_id, attach_name, coordinate_system, status, user_id, user_full_name)
                VALUES (%s, %s, %s, 1, %s, %s)
                RETURNING attach_id
            """,
                [meta_id, final_name, coordinate_system, uid, ufn],
            )

            attach_id = cur.fetchone()[0]

        return JsonResponse(
            {
                "ok": True,
                "attach_id": attach_id,
                "meta_id": meta_id,
                "attach_name": final_name,
                "coordinate_system": coordinate_system,
            }
        )
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
            cur.execute(
                """
                SELECT attach_id, attach_name, coordinate_system
                FROM attach_file
                WHERE meta_id = %s
                AND COALESCE(status,1) = 1
                ORDER BY attach_id DESC
            """,
                [meta_id],
            )
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
            items.append(
                {
                    "attach_id": aid,
                    "attach_name": name,
                    "exists": exists,
                    "size": size,
                    "ext": ext,
                    "has_geometry": ext in {".zip", ".csv", ".txt"},
                    "coordinate_system": coord_sys,
                }
            )
        return JsonResponse({"ok": True, "meta_id": meta_id, "items": items})
    except Exception as e:
        return HttpResponseBadRequest(f"Xəta: {e}")


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
        ("wgs84", None),
        ("utm38", Transformer.from_crs(CRS.from_epsg(32638), CRS.from_epsg(4326), always_xy=True)),
        ("utm39", Transformer.from_crs(CRS.from_epsg(32639), CRS.from_epsg(4326), always_xy=True)),
    ]


def _score_transformer_on_rows(rows, x_idx, y_idx, transformer, sample_limit=200):
    cnt = 0
    checked = 0
    for r in rows:
        if len(r) <= max(x_idx, y_idx):
            continue
        try:
            x = float(str(r[x_idx]).strip().replace(",", "."))
            y = float(str(r[y_idx]).strip().replace(",", "."))
        except Exception:
            continue

        if transformer is not None:
            try:
                lon, lat = transformer.transform(x, y)
            except Exception:
                continue
        else:
            lon, lat = x, y

        if (-180 <= lon <= 180) and (-90 <= lat <= 90):
            cnt += 1

        checked += 1
        if checked >= sample_limit:
            break
    return cnt


def _auto_pick_points_transformer(rows, x_idx, y_idx):
    best = ("wgs84", None)
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
        header = [f"col{i+1}" for i in range(max_len)]
        body = rows

    x_idx, y_idx = _find_xy_columns(header)
    if x_idx is None or y_idx is None:
        if len(header) >= 2:
            x_idx, y_idx = 0, 1
        else:
            return {"type": "FeatureCollection", "features": []}

    crs_idx = _find_crs_column(header)

    choice = (crs_choice or "auto").lower()
    chosen_name = choice
    transformer = _build_transformer_for_points(choice)

    if (choice in ("auto", "detect")) and (crs_idx is None):
        chosen_name, transformer = _auto_pick_points_transformer(body, x_idx, y_idx)

    features = []
    for r in body:
        if len(r) <= max(x_idx, y_idx):
            continue
        try:
            x = float(str(r[x_idx]).strip().replace(",", "."))
            y = float(str(r[y_idx]).strip().replace(",", "."))
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
            key = header[i] if i < len(header) else f"col{i+1}"
            props[key] = val

        if row_crs_code:
            props.setdefault("_crs_used", row_crs_code)
        else:
            props.setdefault("_crs_used", chosen_name)

        features.append({"type": "Feature", "geometry": {"type": "Point", "coordinates": [lon, lat]}, "properties": props})
    return {"type": "FeatureCollection", "features": features}


@require_GET
def attach_geojson(request, attach_id: int):
    req_crs = (request.GET.get("crs") or "auto").lower()

    try:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT meta_id, attach_name, coordinate_system
                FROM attach_file
                WHERE attach_id = %s
                AND COALESCE(status,1) = 1
            """,
                [attach_id],
            )

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
        cur.execute(
            """
            SELECT COUNT(1)
            FROM gis_data
            WHERE fk_metadata = %s
            AND COALESCE(status,1) = 1
        """,
            [meta_id],
        )
        active_cnt = cur.fetchone()[0]
    if not active_cnt:
        return JsonResponse({"type": "FeatureCollection", "features": []}, safe=False)

    if meta_id is None:
        return HttpResponseBadRequest("Ticket nömrəsi aktiv deyil.")

    try:
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


__all__ = [
    "_attach_base_dir_for_write",
    "_find_attach_file",
    "_geojson_from_csvtxt_file",
    "_geojson_from_zip_file",
    "_read_bytes_with_retry",
    "_smb_net_use",
    "attach_geojson",
    "attach_geojson_by_ticket",
    "attach_list_by_ticket",
    "attach_upload",
]