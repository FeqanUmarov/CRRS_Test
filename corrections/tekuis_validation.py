# tekuis_validation.py
# -*- coding: utf-8 -*-
import json
import hashlib
from typing import List, Dict, Any, Tuple, Set, Optional

from django.conf import settings
from django.db import connection

from shapely.geometry import shape as shapely_shape, mapping, Polygon, MultiPolygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from shapely.strtree import STRtree
from shapely.ops import transform as shp_transform
from pyproj import Transformer

# numpy opsionaldır – Shapely 2-də STRtree bəzən indeks (numpy.int64) qaytara bilir
try:
    import numpy as _np
    _NP_INT = (_np.integer,)
except Exception:
    _NP_INT = ()

# === Parametrlər ===
# Minimal sahə həddi (m²)
MIN_AREA_SQM = float(getattr(settings, "TEKUIS_VALIDATION_MIN_AREA_SQM", 1.0))

# Proyeksiya çeviriciləri
# (Daxilə 4326 gəlir; hesablamalar 3857-də aparılır; çıxış 4326 qayıdır)
_to_3857 = Transformer.from_crs(4326, 3857, always_xy=True).transform
_to_4326 = Transformer.from_crs(3857, 4326, always_xy=True).transform


# ---------------------------
# Köməkçi funksiyalar
# ---------------------------
def _geom_area_sqm(g: BaseGeometry) -> float:
    """
    Geometriyanın təxmini sahəsini m² ilə qaytarır.
    G daxil ola bilər 4326 və ya 3857; ehtiyat üçün 3857-yə çeviririk.
    """
    if not g or g.is_empty:
        return 0.0
    try:
        g3857 = shp_transform(_to_3857, g)
        return float(g3857.area)
    except Exception:
        return 0.0


def _flatten_polys(g: BaseGeometry) -> List[Polygon]:
    """Polygon/MultiPolygon/GeometryCollection daxilindən yalnız Polygon-ları düz siyahıda çıxarır."""
    if not g or g.is_empty:
        return []
    gt = g.geom_type
    if gt == "Polygon":
        return [g]
    if gt == "MultiPolygon":
        return [p for p in g.geoms if not p.is_empty]
    if gt == "GeometryCollection":
        out: List[Polygon] = []
        for sub in g.geoms:
            out.extend(_flatten_polys(sub))
        return out
    return []


def _gap_signature(g4326: BaseGeometry) -> str:
    """
    'gap' üçün sabit imza.
    4326-də zərfə + kvantlaşdırılmış sahə istifadə olunur (stabil olsun deyə).
    """
    minx, miny, maxx, maxy = g4326.envelope.bounds
    sig = f"{round(minx,6)},{round(miny,6)},{round(maxx,6)},{round(maxy,6)}|{round(_geom_area_sqm(g4326),1)}"
    return hashlib.md5(sig.encode("utf-8")).hexdigest()


def _ensure_ignore_table():
    with connection.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tekuis_validation_ignore (
              id BIGSERIAL PRIMARY KEY,
              meta_id INTEGER NOT NULL,
              kind VARCHAR(16) NOT NULL DEFAULT 'gap',
              hash TEXT NOT NULL,
              geom geometry(Geometry,4326),
              note TEXT,
              created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              UNIQUE(meta_id, kind, hash)
            );
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS tekuis_validation_ignore_geom_idx
            ON tekuis_validation_ignore
            USING GIST (geom);
        """)


def _is_gap_ignored(meta_id: int, h: str) -> bool:
    _ensure_ignore_table()
    with connection.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM tekuis_validation_ignore WHERE meta_id=%s AND kind='gap' AND hash=%s LIMIT 1",
            [int(meta_id), h],
        )
        return bool(cur.fetchone())


def ignore_gap(meta_id: int, h: str, geom_geojson: Optional[Dict[str, Any]] = None) -> bool:
    """
    Bir 'gap' üçün 'ignore' qeydini saxlayır. Geometriya verilirsə DB-yə də yazır.
    """
    _ensure_ignore_table()
    geom_json = json.dumps(geom_geojson) if geom_geojson else None
    with connection.cursor() as cur:
        try:
            if geom_json:
                cur.execute(
                    """
                    INSERT INTO tekuis_validation_ignore (meta_id, kind, hash, geom)
                    VALUES (%s, 'gap', %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
                    ON CONFLICT (meta_id, kind, hash) DO NOTHING
                    """,
                    [int(meta_id), h, geom_json],
                )
            else:
                cur.execute(
                    """
                    INSERT INTO tekuis_validation_ignore (meta_id, kind, hash)
                    VALUES (%s, 'gap', %s)
                    ON CONFLICT (meta_id, kind, hash) DO NOTHING
                    """,
                    [int(meta_id), h],
                )
            return True
        except Exception:
            return False


def _collect_polys_from_geojson_3857(geojson: Dict[str, Any]) -> List[Polygon]:
    """
    Ekrandan gələn GeoJSON (EPSG:4326) daxil olur.
    Yalnız Polygon/MultiPolygon-ları **EPSG:3857**-yə çevirib qaytarır.
    """
    out: List[Polygon] = []
    feats = (geojson or {}).get("features", [])
    for f in feats:
        try:
            g = shapely_shape(f.get("geometry"))
        except Exception:
            continue
        # validity fix
        g = g.buffer(0)
        for p in _flatten_polys(g):
            p3857 = shp_transform(_to_3857, p).buffer(0)
            if not p3857.is_empty:
                out.append(p3857)
    return out


# ---------------------------
# Əsas validator
# ---------------------------
def validate_tekuis(
    geojson: Dict[str, Any],
    meta_id: int,
    *,
    min_overlap_sqm: Optional[float] = None,
    min_gap_sqm: Optional[float] = None
) -> Dict[str, Any]:
    """
    Giriş GeoJSON-u (4326) **ekrandakı cari vəziyyət** kimi qəbul edir.
    Hesablamalar 3857-də aparılır, nəticələr 4326-ya transform olunaraq qaytarılır.

    Qaytarır:
      {
        meta_id, stats: {n_features, overlap_count, gap_count},
        overlaps: [{a_idx,b_idx, area_sqm, geom, centroid, bbox}],
        gaps:     [{hash, area_sqm, geom, centroid, bbox}]
      }
    """
    MIN_OV = float(min_overlap_sqm if min_overlap_sqm is not None else MIN_AREA_SQM)
    MIN_GA = float(min_gap_sqm     if min_gap_sqm     is not None else MIN_AREA_SQM)

    polys = _collect_polys_from_geojson_3857(geojson)  # 3857-də siyahı
    n = len(polys)

    result: Dict[str, Any] = {
        "meta_id": int(meta_id),
        "stats": {"n_features": n, "overlap_count": 0, "gap_count": 0},
        "overlaps": [],
        "gaps": []
    }
    if n == 0:
        return result

    # ---------- KƏSİŞMƏLƏR (Overlaps) : 3857
    tree = STRtree(polys)

    # Shapely 2 → indekslər, 1.x → obyektlər. Hər iki hal üçün etibarlı funksiya.
    def _query_candidates(g):
        try:
            idxs = tree.query(g, predicate="intersects")
            try:
                # numpy/piton int-ları üçün yoxla
                if len(idxs) > 0 and isinstance(idxs[0], (int,) + _NP_INT):
                    return [polys[int(j)] for j in idxs]
            except Exception:
                pass
            # Shapely 1.x: geometriyalar qayıda bilər
            return list(idxs)
        except Exception:
            # köhnə API fallback
            return list(tree.query(g))

    seen_pairs: Set[Tuple[int, int]] = set()
    for i, g in enumerate(polys):
        candidates = _query_candidates(g)
        for cand_geom in candidates:
            # indeks tap (Shapely 2: eyni obyekt listdədir; 1.x: WKB müqayisəsi)
            j = None
            try:
                j = polys.index(cand_geom)
            except ValueError:
                try:
                    j = next(k for k, pg in enumerate(polys) if pg.wkb == getattr(cand_geom, "wkb", b""))
                except StopIteration:
                    continue
            if j is None or j <= i or (i, j) in seen_pairs:
                continue

            inter = g.intersection(cand_geom)
            if inter.is_empty:
                continue
            inter = inter.buffer(0)  # etibarlılığı artır
            if inter.is_empty:
                continue

            inter_area = float(inter.area)  # 3857-də m²
            if inter_area <= MIN_OV:
                continue

            poly_parts = _flatten_polys(inter)
            if not poly_parts:
                continue
            inter_poly = unary_union(poly_parts) if len(poly_parts) > 1 else poly_parts[0]

            inter4326 = shp_transform(_to_4326, inter_poly)
            rep = inter4326.representative_point()
            result["overlaps"].append({
                "a_idx": i,
                "b_idx": j,
                "area_sqm": round(inter_area, 2),
                "geom": mapping(inter4326),
                "centroid": [float(rep.x), float(rep.y)],
                "bbox": list(inter4326.bounds)
            })
            seen_pairs.add((i, j))

    # ---------- BOŞLUQLAR (Gaps) : 3857 (BİRDƏFƏLİK HESABLA)
    try:
        u = unary_union(polys).buffer(0)
    except Exception:
        u = unary_union(polys)

    comps = _flatten_polys(u if isinstance(u, (Polygon, MultiPolygon)) else u.buffer(0))
    for poly in comps:
        outer = Polygon(poly.exterior)
        gap = outer.difference(poly)
        for gg in _flatten_polys(gap):
            a = float(gg.area)  # 3857 m²
            if a <= MIN_GA:
                continue
            gg4326 = shp_transform(_to_4326, gg)
            h = _gap_signature(gg4326)
            if _is_gap_ignored(meta_id, h):
                continue
            rep = gg4326.representative_point()
            result["gaps"].append({
                "hash": h,
                "area_sqm": round(a, 2),
                "geom": mapping(gg4326),
                "centroid": [float(rep.x), float(rep.y)],
                "bbox": list(gg4326.bounds)
            })

    # Statistikalar
    result["stats"]["overlap_count"] = len(result["overlaps"])
    result["stats"]["gap_count"] = len(result["gaps"])
    return result

