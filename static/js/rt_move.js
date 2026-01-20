/* =========================
   MOVE TOOL (right-tools)
   - Seçilmiş vektor obyekti xəritədə sürüşdürmək
   - Move aktivdəykən basemap (pan/zoom/rotate) tam DONUR
   - Klaviatura ox düymələri ilə hərəkət dəstəyi
   ========================= */
(function () {
  'use strict';

  // ============ Daxili vəziyyət ============
  const MoveState = {
    enabled: false,
    isDragging: false,
    startCoord: null,        // [x,y] - EPSG:3857
    pickedFeature: null,     // ol.Feature
    pickedLayer: null,       // ol.layer.Vector
    originalGeom: null,      // ol.geom (backup)
    downKey: null,
    moveKey: null,
    upKey: null
  };

  // ============ Freeze / Unfreeze ============
  const Freeze = {
    applied: false,
    entries: [],     // [{i, active}]
    guardsOn: false, // wheel/touch guard
  };

  function ensureFreezeCSS() {
    if (document.getElementById('rt-move-freeze-style')) return;
    const style = document.createElement('style');
    style.id = 'rt-move-freeze-style';
    style.textContent = `
      .map-frozen * { cursor: grab !important; }
      .map-frozen.grabbing * { cursor: grabbing !important; }
    `;
    document.head.appendChild(style);
  }

  // MouseWheel/touch scroll-ı udmaq üçün guard-lar
  const _guards = { wheel: null, touchmove: null };
  function wireGuards(on) {
    const vp = map.getViewport?.();
    if (!vp) return;

    if (on && !Freeze.guardsOn) {
      _guards.wheel = (e) => { e.preventDefault(); e.stopPropagation(); };
      _guards.touchmove = (e) => { e.preventDefault(); e.stopPropagation(); };

      // passive=false vacibdir ki, preventDefault işləsin
      vp.addEventListener('wheel', _guards.wheel, { passive: false, capture: true });
      vp.addEventListener('touchmove', _guards.touchmove, { passive: false, capture: true });
      Freeze.guardsOn = true;
    } else if (!on && Freeze.guardsOn) {
      try { vp.removeEventListener('wheel', _guards.wheel, { capture: true }); } catch {}
      try { vp.removeEventListener('touchmove', _guards.touchmove, { capture: true }); } catch {}
      Freeze.guardsOn = false;
      _guards.wheel = _guards.touchmove = null;
    }
  }

  function freezeMap() {
    if (Freeze.applied) return;
    ensureFreezeCSS();

    const entries = [];
    map.getInteractions().forEach(i => {
      const was = (typeof i.getActive === 'function') ? !!i.getActive() : true;
      entries.push({ i, active: was });
      if (typeof i.setActive === 'function') i.setActive(false);
    });
    Freeze.entries = entries;
    Freeze.applied = true;

    const target = map.getTargetElement?.();
    if (target?.classList) target.classList.add('map-frozen');

    wireGuards(true);
  }

  function unfreezeMap() {
    if (!Freeze.applied) return;

    Freeze.entries.forEach(({ i, active }) => {
      if (typeof i.setActive === 'function') i.setActive(!!active);
    });
    Freeze.entries = [];
    Freeze.applied = false;

    const target = map.getTargetElement?.();
    if (target?.classList) {
      target.classList.remove('map-frozen');
      target.classList.remove('grabbing');
    }

    wireGuards(false);
  }

  // ============ Yardımçılar ============
  function hitPick(map, pixel) {
    let hit = null;
    map.forEachFeatureAtPixel(pixel, (feat, layer) => {
      if (!(layer instanceof ol.layer.Vector)) return false;
      if (layer.get('selectIgnore')) return false; 
      hit = { feature: feat, layer };
      return true;
    }, { hitTolerance: 5 });
    return hit;
  }

  function setActiveUI(on) {
    const btn = document.getElementById('rtMove');
    if (btn) btn.classList.toggle('active', !!on);
  }

  function pauseEditingIfAny() {
    try { if (typeof pauseEditingInteractions === 'function') pauseEditingInteractions(); } catch {}
    try { if (typeof stopDraw === 'function') stopDraw(true); } catch {}
  }
  
  function resumeEditingIfAny() {
    try { if (typeof resumeEditingInteractions === 'function') resumeEditingInteractions(); } catch {}
  }

  function findOwner(feature) {
    try {
      if (typeof findVectorLayerAndSourceOfFeature === 'function') {
        return findVectorLayerAndSourceOfFeature(feature) || null;
      }
    } catch {}
    return null;
  }

  function setViewportCursor(c) {
    try { map.getViewport().style.cursor = c || ''; } catch {}
    const target = map.getTargetElement?.();
    if (!target?.classList) return;
    if (c === 'grabbing') target.classList.add('grabbing'); 
    else target.classList.remove('grabbing');
  }

  // ESC = ləğv et
  function cancelDrag(map) {
    if (!MoveState.isDragging) return;
    try {
      if (MoveState.pickedFeature && MoveState.originalGeom) {
        MoveState.pickedFeature.setGeometry(MoveState.originalGeom);
      }
    } finally {
      MoveState.isDragging = false;
      MoveState.startCoord = null;
      MoveState.originalGeom = null;
      setViewportCursor('grab');
    }
  }

  // ---- Klaviatura ilə hərəkət ----
  const KEY_STEP_PX_BASE  = 5;   // normal addım: 5px
  const KEY_STEP_PX_FAST  = 20;  // Shift basılı: 20px
  const KEY_STEP_PX_TURBO = 100; // Ctrl/Meta basılı: 100px

  function translatePickedFeatureByPixels(map, dxPx, dyPx) {
    const feat = MoveState.pickedFeature;
    if (!feat) return false;

    const geom = feat.getGeometry?.();
    if (!geom || typeof geom.translate !== 'function') return false;

    try {
      // Xəritə görünüşündən resolution al
      const view = map.getView();
      const resolution = view.getResolution();
      
      // Piksel offsetini xəritə koordinat vahidlərinə çevir
      // OpenLayers-də Y oxu düzdür, sadəcə dy-ni tərsinə çevirmək lazım deyil
      const dx = dxPx * resolution;
      const dy = dyPx * resolution * -1; // Y oxunu tərsinə çevir (yuxarı = mənfi)
      
      // Geometriyanı translate et
      geom.translate(dx, dy);
      feat.setGeometry(geom);
      
      // Flash effekt
      try { 
        if (typeof flashFeature === 'function') {
          flashFeature(feat, { duration: 120, hz: 6 });
        }
      } catch {}
      
      return true;
      
    } catch (err) {
      console.error('Translation error:', err);
      return false;
    }
  }

  // ======== SNAP helpers ========
  const SNAP_PX = 12; 

  function layerFilterForSnap(layer, excludeLayer) {
    if (!(layer instanceof ol.layer.Vector)) return false;
    if (!layer.getVisible || !layer.getVisible()) return false;
    if (excludeLayer && layer === excludeLayer) return false;
    if (layer.get && layer.get('infoIgnore') === true) return false;
    return true;
  }

  function getSnappedCoordinate(map, pixel, baseCoord, excludeLayer) {
    let bestCoord = null;
    let bestDistPx = Infinity;

    const features = map.getFeaturesAtPixel(pixel, {
      hitTolerance: SNAP_PX,
      layerFilter: (layer) => layerFilterForSnap(layer, excludeLayer)
    }) || [];

    if (!features.length) return null;

    for (const feat of features) {
      const geom = feat && feat.getGeometry && feat.getGeometry();
      if (!geom || !geom.getClosestPoint) continue;
      const closest = geom.getClosestPoint(baseCoord);
      if (!closest) continue;

      const p = map.getPixelFromCoordinate(closest);
      const dx = p[0] - pixel[0], dy = p[1] - pixel[1];
      const dist = Math.sqrt(dx*dx + dy*dy);

      if (dist < bestDistPx && dist <= SNAP_PX) {
        bestDistPx = dist;
        bestCoord = closest;
      }
    }

    return bestCoord;
  }

  // ============ Pointer hadisələri ============
  function onPointerDown(evt) {
    if (!MoveState.enabled) return;
    if (!window.EDIT_ALLOWED) {
      try { Swal.fire('Diqqət', 'Bu əməliyyatı yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'info'); } catch {}
      return;
    }
    const map = evt.map;
    const hit = hitPick(map, evt.pixel);
    if (!hit) return;

    const { feature, layer } = hit;

    const owner = findOwner(feature);
    const source = owner?.source ?? layer?.getSource?.();
    if (!source) return;

    MoveState.isDragging = true;

    const snappedStart = getSnappedCoordinate(map, evt.pixel, evt.coordinate, layer);
    MoveState.startCoord = (snappedStart ? snappedStart.slice() : evt.coordinate.slice());

    MoveState.pickedFeature = feature;
    MoveState.pickedLayer = layer;
    MoveState.originalGeom = feature.getGeometry().clone();

    setViewportCursor('grabbing');
    evt.preventDefault();
    try { evt.originalEvent && evt.originalEvent.preventDefault(); } catch {}
  }

  function onPointerMove(evt) {
    if (!MoveState.enabled || !MoveState.isDragging) return;
    const geom = MoveState.pickedFeature?.getGeometry?.();
    if (!geom) return;

    const curPixel = evt.pixel;
    let targetCoord = evt.coordinate;

    const snapped = getSnappedCoordinate(evt.map, curPixel, targetCoord, MoveState.pickedLayer);
    if (snapped) {
      targetCoord = snapped;
    }

    const dx = targetCoord[0] - MoveState.startCoord[0];
    const dy = targetCoord[1] - MoveState.startCoord[1];

    const base = MoveState.originalGeom.clone();
    base.translate(dx, dy);
    MoveState.pickedFeature.setGeometry(base);

    evt.preventDefault();
    try { evt.originalEvent && evt.originalEvent.preventDefault(); } catch {}
  }

  function onPointerUp(evt) {
    if (!MoveState.enabled || !MoveState.isDragging) return;

    MoveState.isDragging = false;
    MoveState.startCoord = null;
    MoveState.originalGeom = null;

    try {
      if (typeof flashFeature === 'function' && MoveState.pickedFeature) {
        flashFeature(MoveState.pickedFeature, { duration: 700, hz: 3, baseColor:'#60a5fa' });
      }
    } catch {}

    try { if (typeof updateAllSaveButtons === 'function') updateAllSaveButtons(); } catch {}
    try { if (typeof updateDeleteButtonState === 'function') updateDeleteButtonState(); } catch {}
    try { if (typeof saveTekuisToLS === 'function') saveTekuisToLS(); } catch {}

    setViewportCursor('grab');
    evt.preventDefault();
    try { evt.originalEvent && evt.originalEvent.preventDefault(); } catch {}
  }

  // Klaviatura hadisələri
  function onKeyDown(evt) {
    if (!MoveState.enabled) return;

    const t = evt.target;
    const tag = (t && t.tagName) ? t.tagName.toLowerCase() : '';
    if (tag === 'input' || tag === 'textarea' || (t && t.isContentEditable)) return;

    // ESC
    if (evt.key === 'Escape') {
      const map = window.map;
      if (map) {
        cancelDrag(map);
        MoveState.pickedFeature = null;
        MoveState.pickedLayer = null;
      }
      return;
    }

    // Ox düymələri
    if (evt.key === 'ArrowUp' || evt.key === 'ArrowDown' || 
        evt.key === 'ArrowLeft' || evt.key === 'ArrowRight') {
      
      if (!MoveState.pickedFeature) return;

      const map = window.map;
      if (!map) return;

      // Addım ölçüsü
      let step = KEY_STEP_PX_BASE;
      if (evt.shiftKey) step = KEY_STEP_PX_FAST;
      if (evt.ctrlKey || evt.metaKey) step = KEY_STEP_PX_TURBO;

      let dxPx = 0, dyPx = 0;
      
      switch(evt.key) {
        case 'ArrowUp':    dyPx = -step; break;
        case 'ArrowDown':  dyPx = step; break;
        case 'ArrowLeft':  dxPx = -step; break;
        case 'ArrowRight': dxPx = step; break;
      }
      
      const moved = translatePickedFeatureByPixels(map, dxPx, dyPx);

      if (moved) {
        evt.preventDefault();
        evt.stopPropagation();
        
        try { if (typeof updateAllSaveButtons === 'function') updateAllSaveButtons(); } catch {}
        try { if (typeof saveTekuisToLS === 'function') saveTekuisToLS(); } catch {}
      }
    }
  }

  function onKeyUp(evt) {
    if (!MoveState.enabled) return;
    
    if (evt.key === 'ArrowUp' || evt.key === 'ArrowDown' || 
        evt.key === 'ArrowLeft' || evt.key === 'ArrowRight') {
      
      try { if (typeof updateDeleteButtonState === 'function') updateDeleteButtonState(); } catch {}
      
      if (MoveState.pickedFeature) {
        try { setViewportCursor('grab'); } catch {}
      }
    }
  }

  // ============ Public API ============
  window.RTMove = {
    _inited: false,

    init({ map }) {
      if (!map || this._inited) return;
      this._inited = true;

      MoveState.downKey = map.on('pointerdown', onPointerDown);
      MoveState.moveKey = map.on('pointermove', onPointerMove);
      MoveState.upKey   = map.on('pointerup',   onPointerUp);
      
      window.addEventListener('keydown', onKeyDown, { capture: true });
      window.addEventListener('keyup', onKeyUp, { capture: true });
    },

    enable() {
      if (MoveState.enabled) return;
      MoveState.enabled = true;
      setActiveUI(true);

      freezeMap();
      pauseEditingIfAny();
      
      try { 
        showToast('Move aktivdir: obyektə klik edib mouse və ya ox düymələri ilə hərəkət etdirin', 3000); 
      } catch {}
      
      setViewportCursor('grab');
    },

    disable() {
      if (!MoveState.enabled) return;
      
      try { 
        const m = window.map; 
        if (m) cancelDrag(m); 
      } catch {}
      
      MoveState.enabled = false;
      setActiveUI(false);

      MoveState.pickedFeature = null;
      MoveState.pickedLayer = null;

      unfreezeMap();
      resumeEditingIfAny();
      setViewportCursor('');
    },

    toggle() {
      MoveState.enabled ? this.disable() : this.enable();
    }
  };

})();