import csv
import io
import shutil
import tempfile
import zipfile
from pathlib import Path

import shapefile  # pyshp
from django.http import HttpResponseBadRequest, JsonResponse
from django.views.decorators.csrf import csrf_exempt

from .auth import require_valid_ticket
from .geo_utils import (
    _build_transformer_for_points,
    _canonize_crs_value,
    _decode_bytes_to_text,
    _find_crs_column,
    _find_main_shp,
    _find_xy_columns,
    _make_transformer,
    _records_as_props,
    _row_to_float,
    _shape_to_geojson_geometry,
    _sniff_dialect,
)

# ==========================
# .rar dəstəyi
# ==========================
try:
    import rarfile  # pip install rarfile

    RAR_AVAILABLE = True
except Exception:
    rarfile = None
    RAR_AVAILABLE = False


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
            header = [f"col{i+1}" for i in range(max_len)]
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
                key = header[i] if i < len(header) else f"col{i+1}"
                props[key] = val

            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props,
            })

        if not features:
            return HttpResponseBadRequest("Etibarlı nöqtə tapılmadı.")

        fc = {"type": "FeatureCollection", "features": features}
        return JsonResponse(fc, safe=False)

    except Exception as e:
        return HttpResponseBadRequest(f"Xəta: {e}")