/* TEKUİS vertex redaktəsi (frontend-only)
   Quraşdırma: index.html-də main.js və tekuis_erase.js-dən SONRA qoşun:
   <script src="{% static 'js/editvertex.js' %}?v=7" defer></script>
*/
(function () {
  'use strict';

  // ----------------------------- Utils -----------------------------
  const byId = (id) => document.getElementById(id);

  function waitForMap(timeoutMs = 10000, intervalMs = 120) {
    return new Promise((resolve, reject) => {
      const t0 = Date.now();
      const timer = setInterval(() => {
        if (window.ol && window.map && typeof map.getLayers === 'function') {
          clearInterval(timer); resolve(map);
        } else if (Date.now() - t0 > timeoutMs) {
          clearInterval(timer); reject(new Error('Xəritə (map) hazır deyil.'));
        }
      }, intervalMs);
    });
  }

  function _flattenLayers(groupOrCollection) {
    try {
      const arr = groupOrCollection.getArray
        ? groupOrCollection.getArray()
        : groupOrCollection.getLayers
          ? groupOrCollection.getLayers().getArray()
          : [];
      return arr.flatMap(l => (l instanceof ol.layer.Group) ? _flattenLayers(l.getLayers()) : [l]);
    } catch (_) { return []; }
  }
  function _allVectorLayers() {
    try {
      const all = _flattenLayers(map.getLayers());
      return all.filter(l => l instanceof ol.layer.Vector);
    } catch (_) { return []; }
  }
  function _findVectorByTitleFragments(fragments) {
    const frags = (fragments || []).map(s => String(s).toLowerCase());
    const layers = _allVectorLayers();
    return layers.find(l => {
      const title = (l.get && (l.get('title') || l.get('name'))) || '';
      const low = String(title).toLowerCase();
      return frags.some(f => low.includes(f));
    }) || null;
  }
  function _guessTekuisLayer() {
    try { if (window.tekuisLayer instanceof ol.layer.Vector) return window.tekuisLayer; } catch (_) {}
    let lyr = _findVectorByTitleFragments(['tekuis', 'parsel', 'parcel', 'm_g_parsel']);
    if (lyr) return lyr;

    const cands = _allVectorLayers().filter(l => {
      const src = l.getSource && l.getSource();
      const feats = (src && src.getFeatures && src.getFeatures()) || [];
      if (!feats.length) return false;
      const g = feats[0].getGeometry && feats[0].getGeometry();
      return !!g && /Polygon/i.test(g.getType());
    });
    if (!cands.length) return null;
    cands.sort((a, b) => (b.getSource().getFeatures().length) - (a.getSource().getFeatures().length));
    return cands[0] || null;
  }

  // Tədqiqat (ticket) layının source-unu tapmaq üçün helper
  function _guessResearchSource() {
    try { if (window.ticketLayerSource) return window.ticketLayerSource; } catch (_) {}
    try {
      if (window.ticketLayer && window.ticketLayer.getSource) return window.ticketLayer.getSource();
    } catch (_) {}
    try {
      const all = _allVectorLayers();
      const cand = all.find(l => {
        const t = (l.get && (l.get('title') || l.get('name') || '')).toString().toLowerCase();
        return /tədqiqat|tadqiqat|research|ticket/.test(t);
      });
      return cand ? (cand.getSource && cand.getSource()) : null;
    } catch (_) { return null; }
  }

  // Polygon-a yeni vertex əlavə etmək funksiyası
  function addVertexToPolygon(feature, coordinate) {
    const geom = feature.getGeometry();
    if (!geom || geom.getType() !== 'Polygon') return false;

    const coords = geom.getCoordinates();
    const exteriorRing = coords[0];
    if (!exteriorRing || exteriorRing.length < 4) return false;

    // Ən yaxın edge-i tap
    let minDistance = Infinity;
    let insertIndex = -1;

    for (let i = 0; i < exteriorRing.length - 1; i++) {
      const p1 = exteriorRing[i];
      const p2 = exteriorRing[i + 1];

      const distance = distancePointToLine(coordinate, p1, p2);
      if (distance < minDistance) {
        minDistance = distance;
        insertIndex = i + 1;
      }
    }

    if (insertIndex > 0) {
      exteriorRing.splice(insertIndex, 0, coordinate);
      geom.setCoordinates(coords);
      return true;
    }
    return false;
  }

  // MultiPolygon-a vertex əlavə etmək
  function addVertexToMultiPolygon(feature, coordinate) {
    const geom = feature.getGeometry();
    if (!geom || geom.getType() !== 'MultiPolygon') return false;

    const allCoords = geom.getCoordinates();
    let minDistance = Infinity;
    let targetPolygonIndex = -1;
    let insertIndex = -1;

    allCoords.forEach((polygonCoords, polygonIdx) => {
      const exteriorRing = polygonCoords[0];
      if (!exteriorRing || exteriorRing.length < 4) return;

      for (let i = 0; i < exteriorRing.length - 1; i++) {
        const p1 = exteriorRing[i];
        const p2 = exteriorRing[i + 1];
        const distance = distancePointToLine(coordinate, p1, p2);

        if (distance < minDistance) {
          minDistance = distance;
          targetPolygonIndex = polygonIdx;
          insertIndex = i + 1;
        }
      }
    });

    if (targetPolygonIndex >= 0 && insertIndex > 0) {
      allCoords[targetPolygonIndex][0].splice(insertIndex, 0, coordinate);
      geom.setCoordinates(allCoords);
      return true;
    }
    return false;
  }

  // Nöqtədən xəttə məsafə
  function distancePointToLine(point, lineStart, lineEnd) {
    const [px, py] = point;
    const [x1, y1] = lineStart;
    const [x2, y2] = lineEnd;

    const A = px - x1;
    const B = py - y1;
    const C = x2 - x1;
    const D = y2 - y1;

    const dot = A * C + B * D;
    const lenSq = C * C + D * D;

    if (lenSq === 0) return Math.sqrt(A * A + B * B);

    let param = dot / lenSq;
    let xx, yy;
    if (param < 0) { xx = x1; yy = y1; }
    else if (param > 1) { xx = x2; yy = y2; }
    else { xx = x1 + param * C; yy = y1 + param * D; }

    const dx = px - xx;
    const dy = py - yy;
    return Math.sqrt(dx * dx + dy * dy);
  }

  // ----------------------------- State -----------------------------
  const STATE = {
    enabled: false,
    select: null,
    modify: null,

    // TEKUİS: self-snap olmadan snap üçün features kolleksiyası
    snap: null,
    snapFeatures: null,
    tekuisSource: null,

    // Tədqiqat layına (ticket) snap üçün ayrıca interaction
    researchSource: null,
    snapResearch: null,

    // dinamik dəyişiklikləri izləmək üçün
    _lastResearchSourceId: null,
    _watcher: null,

    undoStack: [],          // { feature, geometry }  (EPSG:3857 klon)
    keyA: false,
    keyD: false,

    vertexOverlaySrc: null,
    vertexOverlayLyr: null,
    vertexListeners: new Map(), // feature -> unByKey

    escHandler: null,
    keyHandlerDown: null,
    keyHandlerUp: null,
    mapClickHandler: null, // A basılıykən klik üçün

    changed: [],
    wktWriter: new ol.format.WKT()
  };

  function selectedStyle() {
    return new ol.style.Style({
      stroke: new ol.style.Stroke({ color: '#ef4444', width: 3 }),
      fill:   new ol.style.Fill({ color: 'rgba(239,68,68,0.06)' })
    });
  }

  const vertexHandleStyle = new ol.style.Style({
    image: new ol.style.Circle({
      radius: 5,
      fill:   new ol.style.Fill({ color: '#ffffff' }),
      stroke: new ol.style.Stroke({ color: '#2563eb', width: 2 })
    })
  });
  const vertexPointStyle = new ol.style.Style({
    image: new ol.style.Circle({
      radius: 4,
      fill:   new ol.style.Fill({ color: '#2563eb' }),
      stroke: new ol.style.Stroke({ color: '#ffffff', width: 1.5 })
    })
  });

  // ---- Vertex Overlay helpers ----
  function ensureVertexOverlay() {
    if (!STATE.vertexOverlaySrc) {
      STATE.vertexOverlaySrc = new ol.source.Vector();
      STATE.vertexOverlayLyr = new ol.layer.Vector({
        source: STATE.vertexOverlaySrc,
        style: vertexPointStyle,
        zIndex: 99
      });
      STATE.vertexOverlayLyr.set('selectIgnore', true);
      STATE.vertexOverlayLyr.set('infoIgnore', true);
    }
  }

  function addFeatureGeomListener_(feature) {
    const key = feature.on('change:geometry', () => refreshVertexOverlay());
    STATE.vertexListeners.set(feature, key);
  }
  function removeFeatureGeomListener_(feature) {
    const key = STATE.vertexListeners.get(feature);
    if (key) {
      ol.Observable.unByKey(key);
      STATE.vertexListeners.delete(feature);
    }
  }

  function refreshVertexOverlay() {
    if (!STATE.enabled) return;
    ensureVertexOverlay();
    STATE.vertexOverlaySrc.clear(true);

    const selected = STATE.select ? STATE.select.getFeatures().getArray() : [];
    selected.forEach(feat => {
      const g = feat.getGeometry();
      if (!g) return;
      const type = g.getType();

      if (type === 'Polygon') {
        g.getCoordinates().forEach(ring => ring.forEach(coord => {
          STATE.vertexOverlaySrc.addFeature(new ol.Feature(new ol.geom.Point(coord)));
        }));
      } else if (type === 'MultiPolygon') {
        g.getCoordinates().forEach(poly => {
          poly.forEach(ring => ring.forEach(coord => {
            STATE.vertexOverlaySrc.addFeature(new ol.Feature(new ol.geom.Point(coord)));
          }));
        });
      }
    });
    STATE.vertexOverlayLyr.changed();
  }

  // ---- Snap helpers (self-snap yoxdur) ----
  function rebuildSnapFeatures() {
    if (!STATE.tekuisSource || !STATE.snapFeatures) return;
    const all = (STATE.tekuisSource.getFeatures && STATE.tekuisSource.getFeatures()) || [];
    const selected = STATE.select ? STATE.select.getFeatures().getArray() : [];
    const selectedSet = new Set(selected);

    STATE.snapFeatures.clear();
    all.forEach(f => { if (!selectedSet.has(f)) STATE.snapFeatures.push(f); });
  }

  // Tədqiqat layına snap interaction-ını qur / yenilə
  function ensureResearchSnapInteraction() {
    const src = _guessResearchSource();
    if (!src) return;

    // unique "id" kimi mənbənin daxili uid/obj ref istifadə edək
    const id = src.ol_uid || src.uid || String(src);
    if (STATE._lastResearchSourceId === id && STATE.snapResearch) return;

    STATE.researchSource = src;
    STATE._lastResearchSourceId = id;

    // köhnəni sök
    if (STATE.snapResearch) {
      try { map.removeInteraction(STATE.snapResearch); } catch(_) {}
      STATE.snapResearch = null;
    }

    STATE.snapResearch = new ol.interaction.Snap({
      source: STATE.researchSource,
      pixelTolerance: 12,
      edge: true,
      vertex: true
    });

    // Edit açıqdırsa – interaction-lar sırasının SONUNA əlavə et
    if (STATE.enabled) {
      map.addInteraction(STATE.snapResearch);
    }

    // global snap registrinə ver (opsional)
    try {
      if (typeof window.registerSnapSource === 'function') {
        window.registerSnapSource(STATE.researchSource);
      }
    } catch(_) {}
  }

  // Map click handler - A basılıykən vertex əlavə etmək üçün
  function onMapClick(evt) {
    if (!STATE.enabled || !STATE.keyA) return;

    const selected = STATE.select ? STATE.select.getFeatures().getArray() : [];
    if (selected.length === 0) return;

    const coordinate = evt.coordinate;
    let modified = false;

    selected.forEach(feature => {
      const geom = feature.getGeometry();
      if (geom) {
        STATE.undoStack.push({ feature: feature, geometry: geom.clone() });
      }
      const geomType = geom ? geom.getType() : '';
      let success = false;

      if (geomType === 'Polygon') {
        success = addVertexToPolygon(feature, coordinate);
      } else if (geomType === 'MultiPolygon') {
        success = addVertexToMultiPolygon(feature, coordinate);
      }

      if (success) {
        modified = true;
        try {
          const g3857 = feature.getGeometry();
          if (g3857) {
            const g4326 = g3857.clone().transform('EPSG:3857','EPSG:4326');
            const wkt   = STATE.wktWriter.writeGeometry(g4326, { decimals: 8 });
            const props = feature.getProperties() || {};
            const id = props.CADASTER_NUMBER || props.OBJECTID || props.ID || props.ROWID || null;
            STATE.changed = [{ id, wkt, props }];
            window._TEKUIS_VERTEX_LAST = { ts: Date.now(), items: STATE.changed };
          }
        } catch (err) {
          console.warn('vertex add serialize error:', err);
        }
      }
    });

    if (modified) {
      refreshVertexOverlay();
      rebuildSnapFeatures();
    }
  }

  function ensureInteractions() {
    const tekuis = _guessTekuisLayer();
    if (!tekuis) return { ok: false, reason: 'TEKUİS layı tapılmadı.' };
    STATE.tekuisSource = tekuis.getSource && tekuis.getSource();

    // Select
    if (!STATE.select) {
      STATE.select = new ol.interaction.Select({
        layers: (layer) => layer === tekuis,  // yalnız TEKUİS
        hitTolerance: 6,
        style: selectedStyle()
      });
      STATE.select.getFeatures().on('add', (e) => {
        const f = e.element; addFeatureGeomListener_(f);
        refreshVertexOverlay(); rebuildSnapFeatures();
      });
      STATE.select.getFeatures().on('remove', (e) => {
        const f = e.element; removeFeatureGeomListener_(f);
        refreshVertexOverlay(); rebuildSnapFeatures();
      });
    } else {
      // (özəl property-lər – mövcud obyektdə təzədən filtr)
      STATE.select.layers_ = [tekuis];
      STATE.select.layerFilter_ = (layer) => layer === tekuis;
    }

    // Modify
    if (!STATE.modify) {
      STATE.modify = new ol.interaction.Modify({
        features: STATE.select.getFeatures(),
        style: vertexHandleStyle,
        pixelTolerance: 15,
        deleteCondition: (evt) =>
          STATE.keyD && ol.events.condition.singleClick(evt)
      });

      STATE.modify.on('modifystart', (e) => {
        const feats = e.features ? e.features.getArray() : [];
        feats.forEach(f => {
          const g = f.getGeometry(); if (!g) return;
          STATE.undoStack.push({ feature: f, geometry: g.clone() });
        });
      });

      STATE.modify.on('modifyend', (e) => {
        try {
          refreshVertexOverlay();
          rebuildSnapFeatures();

          const feats = e.features ? e.features.getArray() : [];
          const arr = [];
          feats.forEach(f => {
            const g3857 = f.getGeometry();
            if (!g3857) return;
            const g4326 = g3857.clone().transform('EPSG:3857','EPSG:4326');
            const wkt   = STATE.wktWriter.writeGeometry(g4326, { decimals: 8 });
            const props = f.getProperties() || {};
            const id = props.CADASTER_NUMBER || props.OBJECTID || props.ID || props.ROWID || null;
            arr.push({ id, wkt, props });
          });
          STATE.changed = arr;
          window._TEKUIS_VERTEX_LAST = { ts: Date.now(), items: arr };
        } catch (err) {
          console.warn('modifyend serialize error:', err);
        }
      });
    }

    // TEKUİS snap (self-snap-dən yayınmaq üçün features kolleksiyası)
    if (!STATE.snapFeatures) STATE.snapFeatures = new ol.Collection();
    if (!STATE.snap) {
      STATE.snap = new ol.interaction.Snap({
        features: STATE.snapFeatures,
        pixelTolerance: 12,
        edge: true,
        vertex: true
      });
    }
    if (STATE.tekuisSource && !STATE._srcBound) {
      STATE._srcBound = true;
      STATE.tekuisSource.on('addfeature', rebuildSnapFeatures);
      STATE.tekuisSource.on('removefeature', rebuildSnapFeatures);
    }
    rebuildSnapFeatures();

    // Tədqiqat layına snap interaction-ı hazırla
    ensureResearchSnapInteraction();

    // Global snap sisteminiz varsa qeydiyyat (opsional)
    try {
      if (typeof window.registerSnapSource === 'function' && STATE.tekuisSource) {
        window.registerSnapSource(STATE.tekuisSource);
      }
      if (typeof window.registerSnapSource === 'function' && STATE.researchSource) {
        window.registerSnapSource(STATE.researchSource);
      }
    } catch(_) {}

    ensureVertexOverlay();

    return { ok: true };
  }

  // ---- Klaviatura: Ctrl+Z, A, D ----
  function onKeyDown(ev) {
    if (!STATE.enabled) return;

    // Ctrl+Z → UNDO
    if ((ev.ctrlKey || ev.metaKey) && (ev.key === 'z' || ev.key === 'Z')) {
      ev.preventDefault();
      const last = STATE.undoStack.pop();
      if (last && last.feature && last.geometry) {
        try {
          last.feature.setGeometry(last.geometry.clone());
          refreshVertexOverlay();
          rebuildSnapFeatures();
        } catch(_) {}
      }
      return;
    }

    if (ev.key === 'a' || ev.key === 'A') {
      if (!STATE.keyA) {
        STATE.keyA = true;
        try { STATE.select && STATE.select.setActive(false); } catch(_) {}
        try { STATE.modify && STATE.modify.setActive(false); } catch(_) {}
      }
    }
    if (ev.key === 'd' || ev.key === 'D') STATE.keyD = true;
  }
  function onKeyUp(ev) {
    if (ev.key === 'a' || ev.key === 'A') {
      STATE.keyA = false;
      try { STATE.select && STATE.select.setActive(true); } catch(_) {}
      try { STATE.modify && STATE.modify.setActive(true); } catch(_) {}
    }
    if (ev.key === 'd' || ev.key === 'D') STATE.keyD = false;
  }

  // ---- Dinamik watcher: ticketLayerSource dəyişəndə snap-ı yenilə
  function startResearchWatcher() {
    if (STATE._watcher) return;
    STATE._watcher = setInterval(() => {
      const current = _guessResearchSource();
      if (!current) return;
      const id = current.ol_uid || current.uid || String(current);
      if (STATE._lastResearchSourceId !== id) {
        ensureResearchSnapInteraction();
      }
    }, 800); // 0.8s kifayətdir, performansa yük vermir
  }
  function stopResearchWatcher() {
    if (STATE._watcher) {
      clearInterval(STATE._watcher);
      STATE._watcher = null;
    }
  }

  function enable() {
    if (STATE.enabled) return;
    const check = ensureInteractions();
    if (!check.ok) {
      window.Swal ? Swal.fire('Diqqət', check.reason, 'info') : alert(check.reason);
      return;
    }

    // Münaqişə ola biləcək modları söndür
    try { window.stopDraw && window.stopDraw(true); } catch(_) {}
    try { window.disableInfoMode && window.disableInfoMode(); } catch(_) {}

    // Interaction-lar: Select → Modify → TEKUİS Snap → Research Snap
    map.addInteraction(STATE.select);
    map.addInteraction(STATE.modify);
    map.addInteraction(STATE.snap);
    if (STATE.snapResearch) map.addInteraction(STATE.snapResearch);

    // vertex overlay layını xəritəyə əlavə et
    if (STATE.vertexOverlayLyr && !map.getLayers().getArray().includes(STATE.vertexOverlayLyr)) {
      map.addLayer(STATE.vertexOverlayLyr);
    }
    refreshVertexOverlay();

    setBtnActive(true);
    STATE.enabled = true;

    // ESC → dayandır
    STATE.escHandler = (ev) => { if (ev.key === 'Escape') disable(); };
    document.addEventListener('keydown', STATE.escHandler);

    // Klaviatura
    STATE.keyHandlerDown = onKeyDown;
    STATE.keyHandlerUp   = onKeyUp;
    document.addEventListener('keydown', STATE.keyHandlerDown);
    document.addEventListener('keyup',   STATE.keyHandlerUp);

    // Map click handler (A basılıykən vertex əlavə)
    STATE.mapClickHandler = onMapClick;
    map.on('click', STATE.mapClickHandler);

    // ticketLayerSource dəyişikliklərini izlə
    startResearchWatcher();

    if (window.Swal) {
      Swal.fire({
        title: 'Vertex redaktəsi aktivdir',
        html: '• <b>A</b> basılı + kənara klik → vertex <b>əlavə</b> et<br>' +
              '• <b>D</b> basılı + vertex-ə klik → vertex <b>sil</b><br>' +
              '• <b>Ctrl+Z</b> → <b>geri al</b><br>' +
              '• Bütün vertexlər punkt kimi görüntülənir. <b>ESC</b> ilə dayandırın.',
        icon: 'info',
        timer: 3200,
        showConfirmButton: false
      });
    }
  }

  function disable() {
    if (!STATE.enabled) return;
    try { map.removeInteraction(STATE.snapResearch); } catch(_) {}
    try { map.removeInteraction(STATE.snap); } catch(_) {}
    try { map.removeInteraction(STATE.modify); } catch(_) {}
    try { map.removeInteraction(STATE.select); } catch(_) {}
    try { STATE.select.getFeatures().clear(); } catch(_) {}

    if (STATE.vertexOverlaySrc) STATE.vertexOverlaySrc.clear(true);
    if (STATE.vertexOverlayLyr) {
      try { map.removeLayer(STATE.vertexOverlayLyr); } catch(_) {}
    }
    STATE.vertexListeners.forEach((key) => { ol.Observable.unByKey(key); });
    STATE.vertexListeners.clear();

    if (STATE.escHandler) {
      document.removeEventListener('keydown', STATE.escHandler);
      STATE.escHandler = null;
    }
    if (STATE.keyHandlerDown) {
      document.removeEventListener('keydown', STATE.keyHandlerDown);
      STATE.keyHandlerDown = null;
    }
    if (STATE.keyHandlerUp) {
      document.removeEventListener('keyup', STATE.keyHandlerUp);
      STATE.keyHandlerUp = null;
    }
    if (STATE.mapClickHandler) {
      map.un('click', STATE.mapClickHandler);
      STATE.mapClickHandler = null;
    }

    stopResearchWatcher();

    setBtnActive(false);
    STATE.enabled = false;
  }

  function toggle() { STATE.enabled ? disable() : enable(); }
  function setBtnActive(active) {
    const btn = byId('rtEditVertices');
    if (btn) btn.classList.toggle('active', !!active);
  }

  // ----------------------- Public API ----------------------
  window.TEKUIS_VERTEX_EDIT = {
    enable, disable, toggle,
    isEnabled: () => STATE.enabled,
    last: () => window._TEKUIS_VERTEX_LAST || { ts: null, items: [] }
  };

  // ----------------------- UI: right-tools button -------------------
  function injectButton() {
    const host = byId('rightTools');
    if (!host || byId('rtEditVertices')) return;

    const b = document.createElement('button');
    b.id = 'rtEditVertices';
    b.className = 'rt-btn';
    b.title = 'TEKUİS parsellerin vertex redaktəsi';

    const iconUrl = (window.RT_ICONS && window.RT_ICONS.editVertices) || null;
    if (iconUrl) {
      const img = document.createElement('img');
      img.src = iconUrl;
      img.alt = 'Edit vertices';
      img.className = 'rt-icon-img';
      b.appendChild(img);
    } else {
      b.innerHTML = `
        <svg class="rt-icon" viewBox="0 0 24 24" fill="none" aria-hidden="true">
          <path d="M4 15.5V20h4.5L19 9.5l-4.5-4.5L4 15.5z" stroke="#10b981" stroke-width="2" fill="rgba(16,185,129,0.15)"/>
          <path d="M14.5 5l4.5 4.5" stroke="#10b981" stroke-width="2" />
        </svg>`;
    }

    b.addEventListener('click', () => window.TEKUIS_VERTEX_EDIT.toggle());
    host.appendChild(b);
  }

  // ----------------------------- Start ------------------------------
  document.addEventListener('DOMContentLoaded', async () => {
    try {
      await waitForMap();
      injectButton();
    } catch (err) {
      console.warn('Map gözlənərkən problem:', err?.message || err);
    }
  });
})();
