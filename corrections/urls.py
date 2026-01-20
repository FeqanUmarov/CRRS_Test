from django.urls import path
from .views import (
    upload_shp,
    upload_points,
    save_polygon,
    info_by_geom,
    info_by_fk,
    layers_by_ticket,
    debug_mssql,
    debug_odbc,
    attach_upload,
    attach_list_by_ticket,
    attach_geojson,
    attach_geojson_by_ticket,
    ticket_status,
    soft_delete_gis_by_ticket,
    tekuis_parcels_by_bbox,
    tekuis_parcels_by_geom,
    save_tekuis_parcels,
    validate_tekuis_parcels,
    ignore_tekuis_gap,
    tekuis_exists_by_ticket,
    attributes_options,
    kateqoriya_name_by_tekuis_code,
    kateqoriya_name_by_ticket


)

from .necas_api import necas_parcels_by_bbox, necas_parcels_by_geom
from .tekuis_parcel_db import tekuis_parcels_by_db
from .history_api import history_status






urlpatterns = [
    path('upload-shp/', upload_shp, name='upload_shp'),
    path('upload-points/', upload_points, name='upload_points'),
    path('save-polygon/', save_polygon, name='save_polygon'),

    # Məlumat paneli
    path('info/by-geom/', info_by_geom, name='info_by_geom'),
    path('info/by-fk/<int:fk>/', info_by_fk, name='info_by_fk'),
    path('layers/by-ticket/', layers_by_ticket, name='layers_by_ticket'),

    # Attach
    path('attach/upload/', attach_upload, name='attach_upload'),
    path('attach/list-by-ticket/', attach_list_by_ticket, name='attach_list_by_ticket'),
    path('attach/geojson/<int:attach_id>/', attach_geojson, name='attach_geojson'),
    path('attach/geojson/by-ticket/', attach_geojson_by_ticket, name='attach_geojson_by_ticket'),


    path("ticket-status/", ticket_status, name="ticket_status"),

    path("layers/soft-delete-by-ticket/", soft_delete_gis_by_ticket, name='soft_delete_by_ticket'),


    path("tekuis/parcels/by-bbox/", tekuis_parcels_by_bbox, name="tekuis_by_bbox"),
    path("tekuis/parcels/by-geom/", tekuis_parcels_by_geom, name="tekuis_by_geom"),



    path("necas/parcels/by-bbox/", necas_parcels_by_bbox, name="necas_by_bbox"),
    path("necas/parcels/by-geom/", necas_parcels_by_geom, name="necas_by_geom"),

    path("save-tekuis-parcels/", save_tekuis_parcels, name="save_tekuis_parcels"),
    path("tekuis/exists", tekuis_exists_by_ticket, name="tekuis_exists_by_ticket"),

    path("tekuis/parcels/by-db/", tekuis_parcels_by_db, name="tekuis_by_db"),

    path("tekuis/validate/", validate_tekuis_parcels, name="validate_tekuis_parcels"),
    path("tekuis/validate/ignore-gap/", ignore_tekuis_gap, name="ignore_tekuis_gap"),

    path("history/status/", history_status, name="history_status"),

    path("attributes/options/", attributes_options, name="attributes_options"),

    path('dict/kateqoriya/by-tekuis-code/', kateqoriya_name_by_tekuis_code),

    path("dict/kateqoriya/by-ticket", kateqoriya_name_by_ticket, name="kateqoriya_name_by_ticket"),








    # Debug
    path('debug/mssql/', debug_mssql, name='debug_mssql'),
    path('debug/odbc/', debug_odbc, name='debug_odbc'),

]
