import json

import requests
from django.conf import settings
from django.db import connection
from django.http import HttpResponseBadRequest, JsonResponse
from django.views.decorators.http import require_GET

from .auth import _redeem_ticket, _redeem_ticket_with_token, _unauthorized, require_valid_ticket
from .mssql import _filter_request_fields, _is_edit_allowed_for_fk, _mssql_fetch_request
from .tekuis import _has_active_tekuis


def _resolve_fk_by_wkt(wkt: str):
    try:
        with connection.cursor() as cur:
            cur.execute(
                """
                SELECT fk_metadata
                FROM gis_data
                WHERE COALESCE(status,1) = 1
                AND ST_Intersects(geom, ST_GeomFromText(%s, 4326))
                LIMIT 1
            """,
                [wkt],
            )
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


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
            cur.execute(
                """
                SELECT id, fk_metadata, ST_AsGeoJSON(geom) AS gj
                FROM gis_data
                WHERE fk_metadata = %s
                AND COALESCE(status,1) = 1
            """,
                [fk_metadata],
            )
            rows = cur.fetchall()

        features = []
        for rid, fk, gj in rows:
            try:
                geom = json.loads(gj) if isinstance(gj, str) else gj
            except Exception:
                geom = None
            if not geom:
                continue
            features.append(
                {
                    "type": "Feature",
                    "id": rid,
                    "geometry": geom,
                    "properties": {"fk_metadata": fk},
                }
            )
        fc = {"type": "FeatureCollection", "features": features, "count": len(features), "fk_metadata": fk_metadata}
        return JsonResponse(fc, safe=False)
    except Exception as e:
        return HttpResponseBadRequest(f"Xəta: {e}")


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
            cur.execute(
                """
                SELECT kateqoriya_tekuis_name
                  FROM kateqoriya
                 WHERE kateqoriya_tekuis_code::text = %s
                 LIMIT 1
            """,
                [code],
            )
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
    url = getattr(settings, "NODE_REDEEM_URL", "http://10.11.1.73:8080/api/requests/handoff/redeem").rstrip("/")
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
            return JsonResponse({"ok": False, "error": f"redeem HTTP {resp.status_code}"}, status=resp.status_code)
        data = resp.json()
    except Exception as e:
        return JsonResponse({"ok": False, "error": f"redeem error: {e}"}, status=500)

    tekuis_id = data.get("tekuisId")
    if tekuis_id in (None, ""):
        return JsonResponse({"ok": False, "error": "tekuisId not found in redeem"}, status=404)

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
        return JsonResponse({"ok": False, "error": f"DB error: {e}"}, status=500)

    if not row or not row[0]:
        return JsonResponse({"ok": False, "error": "kateqoriya not found for code", "code": code_str}, status=404)

    return JsonResponse({"ok": True, "code": code_str, "name": row[0]}, status=200)


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
        "uqodiya": fetch(
            "SELECT uqodiya_tekuis_code, uqodiya_tekuis_name   FROM uqodiya       ORDER BY uqodiya_tekuis_name"
        ),
        "kateqoriya": fetch(
            "SELECT kateqoriya_tekuis_code, kateqoriya_tekuis_name FROM kateqoriya    ORDER BY kateqoriya_tekuis_name"
        ),
        "mulkiyyet": fetch(
            "SELECT mulkiyyet_tekuis_code, mulkiyyet_tekuis_name  FROM mulkiyyet     ORDER BY mulkiyyet_tekuis_name"
        ),
        "suvarma": fetch(
            "SELECT suvarma_tekuis_code, suvarma_tekuis_name      FROM suvarma       ORDER BY suvarma_tekuis_name"
        ),
        "emlak": fetch(
            "SELECT emlak_tekuis_code, emlak_tekuis_name          FROM emlak         ORDER BY emlak_tekuis_name"
        ),
        "alt_kateqoriya": fetch(
            "SELECT alt_kate_tekuis_code, alt_kate_tekuis_name    FROM alt_kateqoriya ORDER BY alt_kate_tekuis_name"
        ),
        "alt_uqodiya": fetch(
            "SELECT alt_uqo_tekuis_code,  alt_uqo_tekuis_name     FROM alt_uqodiya   ORDER BY alt_uqo_tekuis_name"
        ),
    }
    return JsonResponse({"ok": True, "data": data})