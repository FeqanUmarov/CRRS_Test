import csv
import io
import re
from pathlib import Path
from typing import List, Optional, Tuple

import shapefile  # pyshp
from pyproj import CRS, Transformer
from shapely import wkt as shapely_wkt
from shapely.geometry import mapping, shape as shapely_shape
from shapely.ops import unary_union


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
        w = w[: r + 1].strip()

    return w


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


# ==========================
# CSV/TXT köməkçiləri
# ==========================

def _decode_bytes_to_text(data: bytes) -> str:
    try:
        return data.decode("utf-8-sig")
    except Exception:
        return data.decode("latin-1", errors="ignore")


def _sniff_dialect(sample: str) -> csv.Dialect:
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t| ")
    except Exception:
        class Simple(csv.Dialect):
            delimiter = ","
            quotechar = '"'
            escapechar = None
            doublequote = True
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
            skipinitialspace = True

        return Simple()


_DEF_X = {"x", "lon", "long", "longitude", "easting", "utm_e", "utm_x"}
_DEF_Y = {"y", "lat", "latitude", "northing", "utm_n", "utm_y"}


def _normalize(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", name.lower())


def _find_xy_columns(header: list[str]) -> tuple[int, int] | tuple[None, None]:
    norm = [_normalize(h) for h in header]
    x_idx = next((i for i, n in enumerate(norm) if n in _DEF_X), None)
    y_idx = next((i for i, n in enumerate(norm) if n in _DEF_Y), None)
    return x_idx, y_idx


# --- CRS sütunu ---
_CRS_COL_CANDIDATES = {
    "coordinatesystem",
    "coordsystem",
    "coordsys",
    "coord_system",
    "coordinate_system",
    "crs",
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
    if "wgs84" in t or "4326" in t or "lonlat" in t:
        return "wgs84"
    if "32638" in t or "utm38" in t:
        return "utm38"
    if "32639" in t or "utm39" in t:
        return "utm39"
    return None


def _build_transformer_for_points(crs_choice: str) -> Optional[Transformer]:
    crs_choice = (crs_choice or "wgs84").lower()
    if crs_choice in ("auto", "detect"):
        return None
    if crs_choice == "wgs84":
        return None
    if crs_choice == "utm38":
        return Transformer.from_crs(CRS.from_epsg(32638), CRS.from_epsg(4326), always_xy=True)
    if crs_choice == "utm39":
        return Transformer.from_crs(CRS.from_epsg(32639), CRS.from_epsg(4326), always_xy=True)
    return None


def _row_to_float(v):
    if isinstance(v, str):
        v = v.strip().replace(",", ".")
    return float(v)


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
