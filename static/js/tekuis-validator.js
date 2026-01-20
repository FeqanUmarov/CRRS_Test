// static/js/tekuis-validator.js
(function () {
  "use strict";

  /* =========================================================
     KONFİQURASİYA
     ========================================================= */
  // API yollarınızı öz proyektinizə görə düzəldin:
  const API = {
    validate: "/api/tekuis/validate",
    ignoreGap: "/api/tekuis/validate/ignore-gap", // (istəyə görə) @require_valid_ticket
    save: "/save-tekuis-parcels/"
  };

  const TURF_URL =
    "https://cdn.jsdelivr.net/npm/@turf/turf@6/turf.min.js";

  // Gap hesablama çərçivəsi (default "ticket"):
  //  - "ticket": ticketLayer-union (mövcuddursa)
  //  - "hull":   daxil edilən TEKUİS poliqonlarının konveks qılafı
  let DEFAULT_GAP_FRAME = "ticket";

  // Səs-küy kimi sayılacaq minimal sahə (m²)
  let MIN_AREA_SQM = 0.25;

  /* =========================================================
     UTIL-LƏR
     ========================================================= */
  function loadScriptOnce(src) {
    return new Promise((resolve, reject) => {
      if ([...document.scripts].some((s) => s.src.includes(src)))
        return resolve();
      const s = document.createElement("script");
      s.src = src;
      s.onload = () => resolve();
      s.onerror = () => reject(new Error("Script load failed: " + src));
      document.head.appendChild(s);
    });
  }

  async function ensureTurf() {
    if (window.turf) return window.turf;
    await loadScriptOnce(TURF_URL);
    if (!window.turf) throw new Error("Turf.js yüklənmədi");
    return window.turf;
  }

  function round(n, p = 2) {
    const k = Math.pow(10, p);
    return Math.round(n * k) / k;
  }

  // GeoJSON Geometry (EPSG:4326) → OL Geometry (EPSG:3857)
  function geom4326To3857(geom4326) {
    const gj = new ol.format.GeoJSON();
    return gj.readGeometry(geom4326, {
      dataProjection: "EPSG:4326",
      featureProjection: "EPSG:3857"
    });
  }

  // Girişləri FeatureCollection-ə çevir
  function toFC(f) {
    if (!f) return { type: "FeatureCollection", features: [] };
    if (f.type === "FeatureCollection") return f;
    if (f.type === "Feature")
      return { type: "FeatureCollection", features: [f] };
    return {
      type: "FeatureCollection",
      features: [{ type: "Feature", geometry: f, properties: {} }]
    };
  }

  // Yalnız poliqonal obyektləri düzləndir (Polygon/MultiPolygon)
  function flattenToPolygons(fc, turf) {
    const out = [];
    turf.flattenEach(fc, (feat) => {
      const g = feat && feat.geometry;
      if (!g) return;
      if (g.type === "Polygon" || g.type === "MultiPolygon") {
        out.push({
          type: "Feature",
          properties: Object.assign({}, feat.properties || {}),
          geometry:
            g.type === "Polygon" ? g : turf.polygon(g.coordinates).geometry
        });
      }
    });
    return { type: "FeatureCollection", features: out };
  }

  // Sürətləndirmək üçün bbox ilə əvvəlcədən yoxlama → sonra dəqiq kəsişmə
  function detectOverlaps(polyFC, turf) {
    const feats = polyFC.features || [];
    const out = [];
    for (let i = 0; i < feats.length; i++) {
      const a = feats[i];
      const bbxA = turf.bbox(a);
      for (let j = i + 1; j < feats.length; j++) {
        const b = feats[j];
        const bbxB = turf.bbox(b);
        // bbox sürətli rədd
        if (
          bbxA[0] > bbxB[2] ||
          bbxA[2] < bbxB[0] ||
          bbxA[1] > bbxB[3] ||
          bbxA[3] < bbxB[1]
        )
          continue;
        // dəqiq kəsişmə
        try {
          const inter = turf.intersect(a, b);
          if (!inter) continue;

          // Intersection nəticəsi MultiPolygon/Polygon/GeometryCollection ola bilər.
          const fcInter = toFC(inter);
          turf.flattenEach(fcInter, (f) => {
            if (!f || !f.geometry) return;
            if (
              f.geometry.type !== "Polygon" &&
              f.geometry.type !== "MultiPolygon"
            )
              return;
            const area = turf.area(f);
            if (area >= MIN_AREA_SQM) {
              out.push({ geom: f.geometry, area_sqm: round(area, 2) });
            }
          });
        } catch {
          /* sağlamlıq üçün sükutla keç */
        }
      }
    }
    return out;
  }

  // Sadə ardıcıl union (problemliləri atır)
  function dissolveUnion(polyFC, turf) {
    const feats = polyFC.features || [];
    if (feats.length === 0) return null;
    let acc = feats[0];
    for (let i = 1; i < feats.length; i++) {
      try {
        const u = turf.union(acc, feats[i]);
        if (u) acc = u;
      } catch {
        /* problemli cütlük - atırıq */
      }
    }
    return acc; // Feature
  }

  // GAP üçün çərçivə: ticket-union varsa ondan, yoxdursa convex hull
  function buildGapFrame({ turf, polyFC, frameMode }) {
    // 1) Ticket layının union-u
    if (frameMode === "ticket" && window.ticketLayer && window.ticketLayer.getSource) {
      const src = window.ticketLayer.getSource();
      const feats = src.getFeatures ? src.getFeatures() : [];
      if (feats.length) {
        const gj = new ol.format.GeoJSON();
        const ticketFC = gj.writeFeaturesObject(feats, {
          dataProjection: "EPSG:4326",
          featureProjection: "EPSG:3857"
        });
        const tPolys = flattenToPolygons(toFC(ticketFC), turf);
        const tUnion = dissolveUnion(tPolys, turf);
        if (tUnion) return tUnion; // Feature
      }
    }
    // 2) Fallback: daxili poliqonların konveks qılafı
    try {
      const u = dissolveUnion(polyFC, turf);
      if (!u) return null;
      const pts = turf.explode(u);
      const hull = turf.convex(pts);
      if (hull) return hull;
    } catch {}
    return null;
  }

  function detectGaps({ turf, polyFC, frameMode }) {
    const frame = buildGapFrame({ turf, polyFC, frameMode });
    if (!frame) return [];
    const union = dissolveUnion(polyFC, turf);
    if (!union) return [];
    try {
      const diff = turf.difference(frame, union);
      if (!diff) return [];
      const res = [];
      const fc = toFC(diff);
      turf.flattenEach(fc, (f) => {
        if (!f || !f.geometry) return;
        if (
          f.geometry.type !== "Polygon" &&
          f.geometry.type !== "MultiPolygon"
        )
          return;
        const a = turf.area(f);
        if (a >= MIN_AREA_SQM) res.push({ geom: f.geometry, area_sqm: round(a, 2) });
      });
      return res;
    } catch {
      return [];
    }
  }

  function buildValidation({ turf, fc, frameMode }) {
    const polyFC = flattenToPolygons(toFC(fc), turf);
    const stats = { n_features: polyFC.features.length };
    const overlaps = detectOverlaps(polyFC, turf);
    const gaps = detectGaps({ turf, polyFC, frameMode });
    return { stats, overlaps, gaps };
  }

  /* =========================================================
     XƏRİTƏ OVERLAY (vizual)
     ========================================================= */
  function makeOverlay(map) {
    const src = new ol.source.Vector();
    const layer = new ol.layer.Vector({
      source: src,
      zIndex: 250,
      style: (feature) => {
        const t = feature.getGeometry().getType();
        const red = "#ef4444";
        if (/Point/i.test(t)) {
          return new ol.style.Style({
            image: new ol.style.Circle({
              radius: 6,
              fill: new ol.style.Fill({ color: "rgba(239,68,68,0.12)" }),
              stroke: new ol.style.Stroke({ color: red, width: 3 })
            })
          });
        }
        if (/LineString/i.test(t)) {
          return [
            new ol.style.Style({
              stroke: new ol.style.Stroke({
                color: "rgba(239,68,68,0.35)",
                width: 8
              })
            }),
            new ol.style.Style({
              stroke: new ol.style.Stroke({ color: red, width: 3 })
            })
          ];
        }
        // Polygon / MultiPolygon
        return [
          new ol.style.Style({
            fill: new ol.style.Fill({ color: "rgba(239,68,68,0.08)" })
          }),
          new ol.style.Style({
            stroke: new ol.style.Stroke({
              color: "rgba(239,68,68,0.35)",
              width: 6
            })
          }),
          new ol.style.Style({
            stroke: new ol.style.Stroke({ color: red, width: 3 })
          })
        ];
      }
    });
    layer.set("title", "TEKUİS Topoloji Overlay");
    layer.set("infoIgnore", true);
    layer.set("selectIgnore", true);
    layer.setVisible(false);
    map.addLayer(layer);
    return layer;
  }

  /* =========================================================
     SERVER FALLBACK
     ========================================================= */
  async function validateOnServer({ geojson, ticket, metaId }) {
    const headers = {
      "Content-Type": "application/json",
      Accept: "application/json"
    };
    if (ticket) headers["X-Ticket"] = String(ticket);
    if (Number.isFinite(+metaId)) headers["X-Meta-Id"] = String(+metaId);

    const resp = await fetch(API.validate, {
      method: "POST",
      headers,
      body: JSON.stringify({
        geojson,
        ticket: ticket || "",
        ...(Number.isFinite(+metaId) ? { meta_id: +metaId } : {})
      })
    });
    if (!resp.ok) {
      const txt = await resp.text().catch(() => "");
      throw new Error(txt || "HTTP " + resp.status);
    }
    // Gözlənilən cavab: { ok: boolean, validation: {...} }
    return await resp.json();
  }

  /* =========================================================
     PUBLİK API
     ========================================================= */
  const TekuisValidator = {
    /**
     * Xidməti işə sal: overlay yaradılır və API obyektini qaytarır
     */
    init({ map, ticket = "", metaId = null } = {}) {
      if (!map || typeof map.getView !== "function") {
        throw new Error("Map obyektini verin (OpenLayers ol.Map).");
      }

      const overlay = makeOverlay(map);

      const api = {
        map,
        overlay,
        ticket,
        metaId,

        /* Parametrlər */
        setTicket(t) {
          this.ticket = (t || "").toString().trim();
        },
        setMetaId(m) {
          this.metaId = Number.isFinite(+m) ? +m : null;
        },
        setFrameMode(mode) {
          // "ticket" | "hull"
          DEFAULT_GAP_FRAME = mode === "hull" ? "hull" : "ticket";
        },
        getFrameMode() {
          return DEFAULT_GAP_FRAME;
        },
        setMinAreaSqm(v) {
          if (Number.isFinite(+v) && +v >= 0) MIN_AREA_SQM = +v;
        },

        /**
         * Lokal (Turf) yoxlama; Turf əlçatmazdırsa yaxud preferServer=true göndərilibsə → serverə düşür.
         * @param {Object} opts
         *  - geojson: Feature/FeatureCollection/Geometry
         *  - preferServer: boolean
         *  - frameMode: "ticket" | "hull" (ops.)
         */
        async run({ geojson, preferServer = false, frameMode } = {}) {
          if (!geojson) {
            return {
              ok: true,
              validation: {
                stats: { n_features: 0 },
                overlaps: [],
                gaps: []
              }
            };
          }
          const _frame = frameMode || DEFAULT_GAP_FRAME;

          try {
            if (preferServer) throw new Error("preferServer");
            const turf = await ensureTurf();
            const validation = buildValidation({
              turf,
              fc: geojson,
              frameMode: _frame
            });
            return { ok: true, validation };
          } catch (e) {
            try {
              const data = await validateOnServer({
                geojson,
                ticket: this.ticket,
                metaId: this.metaId
              });
              return data && typeof data === "object"
                ? data
                : { ok: false, validation: { error: "Server cavabı düz deyil" } };
            } catch (err) {
              return {
                ok: false,
                validation: { error: (err && err.message) || "validate failed" }
              };
            }
          }
        },

        /**
         * Nəticələri xəritədə qırmızı overlay kimi göstər (overlap+gap).
         * @param {Object} validation  { overlaps:[{geom, area_sqm}], gaps:[...] }
         */
        renderOnMap(validation) {
          try {
            const src = overlay.getSource();
            src.clear(true);
            const add = (list) => {
              (list || []).forEach((item) => {
                if (!item || !item.geom) return;
                const g = geom4326To3857(item.geom);
                const f = new ol.Feature({ geometry: g });
                if (item.area_sqm != null) f.set("area_sqm", item.area_sqm);
                src.addFeature(f);
              });
            };
            add(validation && validation.overlaps);
            add(validation && validation.gaps);
            overlay.setVisible(src.getFeatures().length > 0);
          } catch (err) {
            console.warn("renderOnMap error:", err);
          }
        },

        clearOverlay() {
          try {
            overlay.getSource().clear(true);
            overlay.setVisible(false);
          } catch {}
        },

        destroy() {
          try {
            this.clearOverlay();
            this.map.removeLayer(overlay);
          } catch {}
        },

        /**
         * Verilən 4326 geometry-yə yaxınlaşdır (mövcudsa zoomAndHighlightTopoGeometry istifadə edir)
         */
        zoomTo(geom4326) {
          try {
            if (typeof window.zoomAndHighlightTopoGeometry === "function") {
              window.zoomAndHighlightTopoGeometry(geom4326);
              return;
            }
            const g = geom4326To3857(geom4326);
            const ext = g.getExtent();
            this.map.getView().fit(ext, {
              padding: [20, 20, 20, 20],
              duration: 600,
              maxZoom: 18
            });
          } catch {}
        },

        /**
         * (İstəyə görə) serverə “bu gap-ı sayma” kimi qeyd göndərmək üçün köməkçi.
         * Proyektinizdə istifadə etmirsinizsə, sadəcə çağırmayın.
         */
        async markGapIgnoredOnServer(payload) {
          // payload: { ticket, geom, reason? }
          try {
            const headers = {
              "Content-Type": "application/json",
              Accept: "application/json"
            };
            const ticket = (payload && payload.ticket) || this.ticket || "";
            if (ticket) headers["X-Ticket"] = String(ticket);
            const resp = await fetch(API.ignoreGap, {
              method: "POST",
              headers,
              body: JSON.stringify(payload || {})
            });
            return { ok: resp.ok, status: resp.status, text: await resp.text() };
          } catch (e) {
            return { ok: false, error: e && e.message ? e.message : "request failed" };
          }
        }
      };

      return api;
    }
  };

  // Qlobal export
  window.TekuisValidator = TekuisValidator;
})();
