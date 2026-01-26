# views.py
import json
from django.http import JsonResponse, HttpResponseBadRequest
from django.db import connection
from django.views.decorators.http import require_GET

TEKUIS_DB_SELECT_COLUMNS = (
    ("tekuis_id", "id"),
    ("kateqoriya", "LAND_CATEGORY_ENUM"),
    ("uqodiya", "LAND_CATEGORY2ENUM"),
    ("alt_kateqoriya", "LAND_CATEGORY3ENUM"),
    ("alt_uqodiya", "LAND_CATEGORY4ENUM"),
    ("mulkiyyet", "OWNER_TYPE_ENUM"),
    ("suvarma", "SUVARILMA_NOVU_ENUM"),
    ("emlak_novu", "EMLAK_NOVU_ENUM"),
    ("islahat_uqodiyasi", "OLD_LAND_CATEGORY2ENUM"),
    ("rayon_adi", "RAYON_ADI"),
    ("ied_adi", "IED_ADI"),
    ("belediyye_adi", "BELEDIYE_ADI"),
    ("sahe_ha", "AREA_HA"),
    ("qeyd", "NAME"),
    ("meta_id", None),
)


def _build_tekuis_select_sql():
    column_sql = ",\n            ".join(
        f'{col} AS "{alias}"' if alias else col for col, alias in TEKUIS_DB_SELECT_COLUMNS
    )
    return f"""
        SELECT
            {column_sql},
            ST_AsGeoJSON(geom, 7) AS geom_geojson
        FROM tekuis_parcel
        WHERE meta_id = %s
          AND status = 1
    """



@require_GET
def tekuis_parcels_by_db(request):
    """
    GeoJSON FeatureCollection:
    - ?meta_id=XXX (üstün)
    - və ya ?ticket=ABC  -> helper ilə meta_id tapılır
    - status=1 filtrini tətbiq edir (cədvəldə status yoxdursa, şərti silmək olar)
    """
    meta_id = request.GET.get("meta_id")
    ticket  = request.GET.get("ticket")

    if not meta_id:
        if not ticket:
            return HttpResponseBadRequest("meta_id və ya ticket verilməlidir.")
        meta_id = _resolve_meta_id_from_ticket(ticket)
        if not meta_id:
            return HttpResponseBadRequest("ticket üçün meta_id tapılmadı.")

    try:
        meta_id_int = int(meta_id)
    except ValueError:
        return HttpResponseBadRequest("meta_id rəqəm olmalıdır.")

    sql = _build_tekuis_select_sql()
    features = []
    with connection.cursor() as cur:
        cur.execute(sql, [meta_id_int])
        cols = [c[0] for c in cur.description]
        for row in cur.fetchall():
            rec = dict(zip(cols, row))
            geom = rec.pop("geom_geojson", None)
            if not geom:
                continue
            features.append({
                "type": "Feature",
                "geometry": json.loads(geom),
                "properties": rec
            })

    return JsonResponse({
        "type": "FeatureCollection",
        "features": features
    })

def _resolve_meta_id_from_ticket(ticket: str):
    """
    Ticket-dən meta_id-ni tapmaq üçün gis_data və attach_file cədvəllərini yoxlayırıq.
    
    Sistemdə ticket birbaşa saxlanmırsa, bu funksiyanı sistemə uyğun düzəltmək lazımdır.
    Hazırda gis_data cədvəlindən fk_metadata-nı tapmağa çalışırıq.
    
    Əgər sistemdə ticket başqa bir yerdə saxlanırsa (məsələn MSSQL-də),
    o zaman bu funksiyanı müvafiq şəkildə yeniləmək lazımdır.
    """
    
    # VARİANT 1: Əgər ticket gis_data cədvəlində saxlanırsa
    # (Əgər ticket sütunu yoxdursa, bu variantı silin)
    """
    with connection.cursor() as cur:
        cur.execute('''
            SELECT DISTINCT fk_metadata
            FROM gis_data
            WHERE ticket = %s
              AND status = 1
            ORDER BY created_date DESC
            LIMIT 1
        ''', [ticket])
        row = cur.fetchone()
        if row:
            return row[0]
    """
    
    # VARİANT 2: Əgər ticket ilə meta_id əlaqəsi MSSQL-də saxlanırsa,
    # burada _redeem_ticket funksiyasından istifadə edə bilərsiniz
    # (Bu funksiya soft_delete_gis_by_ticket-də istifadə olunur)
    try:
        from .views import _redeem_ticket
        meta_id = _redeem_ticket(ticket)
        return meta_id
    except ImportError:
        pass
    
    # VARİANT 3: Əgər attach_file cədvəlində ticket varsa
    # (Əgər ticket sütunu yoxdursa, bu variantı silin)
    """
    with connection.cursor() as cur:
        cur.execute('''
            SELECT DISTINCT meta_id
            FROM attach_file
            WHERE ticket = %s
              AND status = 1
            ORDER BY created_date DESC
            LIMIT 1
        ''', [ticket])
        row = cur.fetchone()
        if row:
            return row[0]
    """
    
    # VARİANT 4: Əgər ticket bir hash və ya encoding formatındadırsa,
    # onu decode edib meta_id-ni çıxarmaq lazım ola bilər
    # Məsələn: ticket = "META123_20240101" formatında ola bilər
    """
    import re
    match = re.search(r'META(\d+)', ticket)
    if match:
        return int(match.group(1))
    """
    
    # Heç bir variant işləməsə None qaytarırıq
    return None