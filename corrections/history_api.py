# corrections/history_api.py
from django.http import JsonResponse
from django.views.decorators.http import require_GET
from django.db import connection

# Redeem üçün öz mövcud utilinizi istifadə edirik (views.py-dən)
from .views import _redeem_ticket


def _has_row(sql: str, params=()) -> bool:
    """Sətir var-yox (LIMIT 1)"""
    with connection.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone() is not None


def _check_attach(meta_id: int):
    has_row = _has_row(
        "SELECT 1 FROM attach_file WHERE meta_id = %s LIMIT 1",
        [meta_id]
    )
    ok = _has_row(
        "SELECT 1 FROM attach_file WHERE meta_id = %s AND COALESCE(status,1) = 1 LIMIT 1",
        [meta_id]
    )
    return has_row, ok


def _check_gis(meta_id: int):
    has_row = _has_row(
        "SELECT 1 FROM gis_data WHERE fk_metadata = %s LIMIT 1",
        [meta_id]
    )
    ok = _has_row(
        "SELECT 1 FROM gis_data WHERE fk_metadata = %s AND COALESCE(status,1) = 1 LIMIT 1",
        [meta_id]
    )
    return has_row, ok


def _check_tekuis(meta_id: int):
    has_row = _has_row(
        "SELECT 1 FROM tekuis_parcel WHERE meta_id = %s LIMIT 1",
        [meta_id]
    )
    ok = _has_row(
        "SELECT 1 FROM tekuis_parcel WHERE meta_id = %s AND COALESCE(status,1) = 1 LIMIT 1",
        [meta_id]
    )
    return has_row, ok

def _get_last_active_info(table: str, id_col: str, meta_id: int):
    """
    Verilən cədvəldə (status=1) ən son yaradılan sətrin user_full_name və created_date (Asia/Baku)
    sahələrini qaytarır. Tapılmasa (None, None) qaytarır.
    """
    sql = f"""
        SELECT
            user_full_name,
            to_char(
                COALESCE(created_date, NOW()) AT TIME ZONE 'Asia/Baku',
                'YYYY-MM-DD HH24:MI:SS'
            ) AS created_dt
        FROM {table}
        WHERE {id_col} = %s
          AND COALESCE(status,1) = 1
        ORDER BY created_date DESC NULLS LAST
        LIMIT 1
    """
    with connection.cursor() as cur:
        cur.execute(sql, [meta_id])
        row = cur.fetchone()
        return (row[0], row[1]) if row else (None, None)


def _format_history_msg(user_full_name: str | None, created_dt: str | None) -> str:
    """
    hm-msg üçün səliqəli mətn formalaşdırır.
    Nümunə: 'İstifadəçi: Ad Soyad  •  Yaradıldı: 2025-10-15 09:56:21'
    """
    name = (user_full_name or "—").strip()
    dt   = (created_dt or "—").strip()
    return f"İstifadəçi: {name}  •  Yaradıldı: {dt}"




@require_GET
def history_status(request):
    """
    GET /api/history/status/?ticket=...&meta_id=...

    Qaytarır:
    {
      ok: true,
      ticket: "...",
      meta_id: 123,
      items: {
        attach: { ok: true|false, has_row: true|false },
        gis:    { ok: true|false, has_row: true|false },
        tekuis: { ok: true|false, has_row: true|false }
      },
      messages: {...}
    }
    """
    ticket = (request.GET.get("ticket") or "").strip()
    meta_id_param = (request.GET.get("meta_id") or "").strip()

    # 1) Əgər meta_id birbaşa verilibsə onu götür
    meta_id = None
    if meta_id_param:
        try:
            meta_id = int(meta_id_param)
        except Exception:
            meta_id = None

    # 2) meta_id verilməyibsə, redeem ilə ticket → fk_metadata
    if meta_id is None and ticket:
        meta_id = _redeem_ticket(ticket)  # valid deyilsə None qaytaracaq

    # Cavab skeleti
    out = {
        "ok": True,
        "ticket": ticket or None,
        "meta_id": meta_id,
        "items": {
            "attach": {"ok": False, "has_row": False},
            "gis":    {"ok": False, "has_row": False},
            "tekuis": {"ok": False, "has_row": False},
        },
        "messages": {
            "attach": "Qoşma lay əlavə edilməyib",
            "gis": "Tədqiqat layı əlavə edilməyib",
            "tekuis": "TEKUİS parselləri local məlumat bazasına daxil edilməyib",
        }
    }

    # Redeem/meta_id tapılmadısa – 401 qaytar (front bunu xəbər kimi göstərəcək)
    if meta_id is None:
        return JsonResponse({"ok": False, "error": "unauthorized", **out}, status=401)

    # Yoxlamalar
    has_row, ok = _check_attach(meta_id)
    out["items"]["attach"] = {"ok": ok, "has_row": has_row}
    if ok:
        out["messages"]["attach"] = "Qoşma lay əlavə edildi"

        # hm-msg üçün user + created_date
    u, d = _get_last_active_info("attach_file", "meta_id", meta_id)
    out["messages"]["attach"] = _format_history_msg(u, d)


    has_row, ok = _check_gis(meta_id)
    out["items"]["gis"] = {"ok": ok, "has_row": has_row}
    if ok:
        out["messages"]["gis"] = "Tədqiqat layı əlavə edildi"

        # hm-msg üçün user + created_date
    u, d = _get_last_active_info("gis_data", "fk_metadata", meta_id)
    out["messages"]["gis"] = _format_history_msg(u, d)


    has_row, ok = _check_tekuis(meta_id)
    out["items"]["tekuis"] = {"ok": ok, "has_row": has_row}
    if ok:
        out["messages"]["tekuis"] = "TEKUİS parselləri local məlumat bazasına daxil edilib"

        # hm-msg üçün user + created_date
    u, d = _get_last_active_info("tekuis_parcel", "meta_id", meta_id)
    out["messages"]["tekuis"] = _format_history_msg(u, d)


    return JsonResponse(out)
