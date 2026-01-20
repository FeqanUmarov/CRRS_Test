/* rtVertexToolbar.js — Vertex toolbar (freeze + rectangle delete, full removal allowed)
   Quraşdırma: editvertex/main.js-dən SONRA əlavə et:
   <script src="{% static 'js/rtVertexToolbar.js' %}?v=4" defer></script>
*/
(function () {
  'use strict';

  const log = (...a) => console.log('[rtVertexToolbar]', ...a);
  const warn = (...a) => console.warn('[rtVertexToolbar]', ...a);

  function toast(text, ms = 2000) {
    if (window.showToast) return window.showToast(text, ms);
    if (window.Swal) return window.Swal.fire({ text, timer: ms, toast: true, position: 'bottom', showConfirmButton: false });
    console.log(text);
  }

  // ================= STATE =================
  const ST = {
    inited: false,
    toolbar: null,
    currentTool: null, // 'select' | 'add' | 'delete'
    box: { drawing: false, moved: false, start: [0, 0], el: null },
    handlers: { mdown: null, mmove: null, mup: null },
    // freeze üçün interaction vəziyyətini saxlayırıq
    freeze: { applied: false, entries: [] }, // [{i, active}]
    getSelectedFeatures: () => {
      const a = (window.selectAny && window.selectAny.getFeatures && window.selectAny.getFeatures().getArray()) || [];
      const b = (window.selectInteraction && window.selectInteraction.getFeatures && window.selectInteraction.getFeatures().getArray()) || [];
      const set = new Set([...a, ...b]);
      return Array.from(set);
    }
  };

  // ================= CSS/HTML =================
  function ensureCSS() {
    if (document.getElementById('vertex-toolbar-styles')) return;
    const style = document.createElement('style');
    style.id = 'vertex-toolbar-styles';
    style.textContent = `
      .vertex-toolbar{
        position:fixed; left:50%; bottom:24px; transform:translateX(-50%) translateY(100px);
        z-index:10000; background:#fff; border-radius:12px; padding:8px;
        box-shadow:0 8px 24px rgba(0,0,0,.15), 0 4px 12px rgba(0,0,0,.1);
        opacity:0; pointer-events:none; transition:all .25s ease;
      }
      .vertex-toolbar.visible{ opacity:1; transform:translateX(-50%) translateY(0); pointer-events:auto; }
      .vertex-toolbar-inner{ display:flex; gap:6px; align-items:center; }
      .vtb-btn{ width:48px; height:48px; border-radius:10px; border:2px solid transparent; background:#f8f9fa; cursor:pointer; display:flex; align-items:center; justify-content:center; transition:all .2s; }
      .vtb-btn:hover{ background:#e9ecef; transform:translateY(-1px); box-shadow:0 4px 12px rgba(0,0,0,.08); }
      .vtb-btn.active{ background:#e3f2fd; border-color:#2196f3; box-shadow:0 0 0 3px rgba(33,150,243,.1); }
      .vtb-icon{ width:24px;height:24px; pointer-events:none; }
      .vtb-rect{ position:fixed; z-index:10002; pointer-events:none; border:2px dashed; }
      .vtb-rect.del{ border-color:#ef4444; box-shadow:inset 0 0 0 9999px rgba(239,68,68,.08); }
      .vtb-tip{ position:fixed; bottom:72px; left:50%; transform:translateX(-50%); background:#111827; color:#fff; padding:6px 10px; border-radius:8px; font-size:12px; box-shadow:0 10px 30px rgba(0,0,0,.18); z-index:10001; }
      .map-frozen * { cursor: crosshair !important; }
    `;
    document.head.appendChild(style);
  }

  function createToolbar() {
    ensureCSS();
    document.getElementById('vertexToolbar')?.remove();

    const toolbar = document.createElement('div');
    toolbar.id = 'vertexToolbar';
    toolbar.className = 'vertex-toolbar';
    toolbar.innerHTML = `
      <div class="vertex-toolbar-inner">
        <button id="vtb-select" class="vtb-btn" title="Seçim (mövcud davranış)" data-tool="select">
          <svg class="vtb-icon" viewBox="0 0 24 24" fill="none" stroke="#2196f3" stroke-width="2">
            <rect x="4" y="4" width="7" height="7" stroke-dasharray="2 2"/><rect x="13" y="4" width="7" height="7"/>
            <rect x="4" y="13" width="7" height="7"/><rect x="13" y="13" width="7" height="7"/>
          </svg>
        </button>
        <button id="vtb-add" class="vtb-btn" title="Vertex əlavə et (mövcud davranış)" data-tool="add">
          <svg class="vtb-icon" viewBox="0 0 24 24" fill="none" stroke="#4caf50" stroke-width="2">
            <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="16"/><line x1="8" y1="12" x2="16" y2="12"/>
          </svg>
        </button>
        <button id="vtb-delete" class="vtb-btn" title="Vertex sil (rectangle)" data-tool="delete">
          <svg class="vtb-icon" viewBox="0 0 24 24" fill="none" stroke="#f44336" stroke-width="2">
            <circle cx="12" cy="12" r="10"/><line x1="8" y1="12" x2="16" y2="12"/>
          </svg>
        </button>
      </div>
    `;
    document.body.appendChild(toolbar);

    toolbar.querySelectorAll('.vtb-btn').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const tool = btn.dataset.tool;
        if (ST.currentTool === tool) { deactivateTool(); return; }
        activateTool(tool);
      });
    });

    ST.toolbar = toolbar;
    return toolbar;
  }

  function setButtonsActive(tool) {
    document.querySelectorAll('.vtb-btn').forEach(b => b.classList.toggle('active', b.dataset.tool === tool));
  }

  function showToolbar() { (ST.toolbar || createToolbar()).classList.add('visible'); }
  function hideToolbar() { ST.toolbar?.classList.remove('visible'); deactivateTool(); }

  function tip(text, ms = 2200) {
    if (!text) return;
    const el = document.createElement('div');
    el.className = 'vtb-tip';
    el.textContent = text;
    document.body.appendChild(el);
    setTimeout(() => { el.style.transition = 'opacity .25s'; el.style.opacity = '0'; }, ms);
    el.addEventListener('transitionend', () => el.remove());
  }

  // ================= FREEZE / UNFREEZE =================
  function freezeMap() {
    if (ST.freeze.applied) return;
    const entries = [];
    map.getInteractions().forEach(i => {
      // Rectangle çəkərkən heç bir interaction işləməsin
      const was = (typeof i.getActive === 'function') ? !!i.getActive() : true;
      entries.push({ i, active: was });
      if (typeof i.setActive === 'function') i.setActive(false);
    });
    ST.freeze.entries = entries;
    ST.freeze.applied = true;

    // cursor və seçilməzlik üçün class
    const target = map.getTargetElement();
    target && target.classList && target.classList.add('map-frozen');
  }
  function unfreezeMap() {
    if (!ST.freeze.applied) return;
    ST.freeze.entries.forEach(({ i, active }) => {
      if (typeof i.setActive === 'function') i.setActive(!!active);
    });
    ST.freeze.entries = [];
    ST.freeze.applied = false;

    const target = map.getTargetElement();
    target && target.classList && target.classList.remove('map-frozen');
  }

  // ================= GEOMETRY HELPERS =================
  const eq2 = (a, b) => a && b && a[0] === b[0] && a[1] === b[1];

  // ring-i indekslərə görə silir, minimum-limit YOXDUR; nəticəni qaytarır
  function hardDeleteIndicesFromRing(ring, idxs) {
    if (!Array.isArray(ring) || ring.length === 0) return { deleted: 0, valid: false };

    const lastIdx = ring.length - 1;
    const isClosed = ring.length >= 2 && eq2(ring[0], ring[lastIdx]);

    // unik və azalan sırada
    const uniq = Array.from(new Set(idxs)).sort((a, b) => b - a);
    let deleted = 0;
    uniq.forEach(idx => {
      if (idx < 0 || idx >= ring.length) return;
      // bağlayıcı son nöqtəni silmək olar, çünki tam silmək istəyirik
      ring.splice(idx, 1);
      deleted++;
    });

    // bağlamanı bərpa: qalan ən az 3 nöqtə varsa
    if (ring.length >= 3) {
      // hazırda son nöqtə birinci ilə eyni deyilsə bağla
      const end = ring[ring.length - 1];
      if (!eq2(ring[0], end)) {
        if (isClosed) ring.push([ring[0][0], ring[0][1]]);
        else if (!isClosed && !eq2(ring[0], ring[ring.length - 1])) ring.push([ring[0][0], ring[0][1]]);
      }
    }

    // Validlik: ring ən azı 4 koordinat (sonu birinciyə bərabər) olmalıdır
    const valid = ring.length >= 4 && eq2(ring[0], ring[ring.length - 1]);
    return { deleted, valid };
  }

  // Feature-in hansı VectorSource-da olduğunu tap
  function findSourceOfFeature(feat) {
    let found = null;
    map.getLayers().forEach(l => {
      if (found) return;
      if (!(l instanceof ol.layer.Vector)) return;
      const src = l.getSource && l.getSource();
      if (src && typeof src.hasFeature === 'function' && src.hasFeature(feat)) found = src;
    });
    return found;
  }

  // ================= RECTANGLE DELETE =================
  function beginBox(e) {
    if (e.button !== 0) return;
    const vp = map.getViewport();
    if (!vp.contains(e.target) && vp !== e.target && !vp.contains?.(e.target)) return;

    ST.box.drawing = true; ST.box.moved = false;
    ST.box.start = [e.clientX, e.clientY];

    const box = document.createElement('div');
    box.className = 'vtb-rect del';
    box.style.left = `${e.clientX}px`; box.style.top = `${e.clientY}px`;
    box.style.width = '0px'; box.style.height = '0px';
    document.body.appendChild(box);
    ST.box.el = box;

    e.preventDefault(); e.stopPropagation();
  }

  function moveBox(e) {
    if (!ST.box.drawing || !ST.box.el) return;
    const [sx, sy] = ST.box.start;
    const cx = e.clientX, cy = e.clientY;
    const left = Math.min(sx, cx), top = Math.min(sy, cy);
    const w = Math.abs(cx - sx), h = Math.abs(cy - sy);
    ST.box.el.style.left = `${left}px`;
    ST.box.el.style.top = `${top}px`;
    ST.box.el.style.width = `${w}px`;
    ST.box.el.style.height = `${h}px`;
    if (w > 3 || h > 3) ST.box.moved = true;
    e.preventDefault(); e.stopPropagation();
  }

  function endBox(e) {
    if (!ST.box.drawing || !ST.box.el) return;
    ST.box.drawing = false;

    const rect = ST.box.el.getBoundingClientRect();
    ST.box.el.remove(); ST.box.el = null;

    if (!ST.box.moved) return;

    try {
      const count = deleteVerticesInsidePixelRect(rect);
      toast(count > 0 ? `${count} vertex silindi` : 'Silinəcək vertex tapılmadı', 1700);
    } catch (err) {
      warn('Rectangle delete error:', err);
      toast('Silinmə zamanı xəta', 1700);
    }

    e.preventDefault(); e.stopPropagation();
  }

  function wireBoxHandlers(on) {
    const vp = map.getViewport();
    if (on) {
      if (!ST.handlers.mdown) { ST.handlers.mdown = beginBox; vp.addEventListener('mousedown', ST.handlers.mdown, true); }
      if (!ST.handlers.mmove) { ST.handlers.mmove = moveBox;  vp.addEventListener('mousemove', ST.handlers.mmove, true); }
      if (!ST.handlers.mup)   { ST.handlers.mup   = endBox;   vp.addEventListener('mouseup',   ST.handlers.mup,   true); }
    } else {
      if (ST.handlers.mdown) { vp.removeEventListener('mousedown', ST.handlers.mdown, true); ST.handlers.mdown = null; }
      if (ST.handlers.mmove) { vp.removeEventListener('mousemove', ST.handlers.mmove, true); ST.handlers.mmove = null; }
      if (ST.handlers.mup)   { vp.removeEventListener('mouseup',   ST.handlers.mup,   true); ST.handlers.mup   = null; }
      if (ST.box.el) { try { ST.box.el.remove(); } catch {} ST.box.el = null; ST.box.drawing = false; ST.box.moved = false; }
    }
  }

  function pointInsideRect(absX, absY, rect) {
    return (absX >= rect.left && absX <= rect.right && absY >= rect.top && absY <= rect.bottom);
  }

  function deleteVerticesInsidePixelRect(pixelRect) {
    // 1) Seçilmişlərdən başla, yoxdursa altındakı bir poliqonu götür
    let features = ST.getSelectedFeatures().filter(f => !!f.getGeometry);
    if (features.length === 0) {
      try {
        const cx = (pixelRect.left + pixelRect.right) / 2;
        const cy = (pixelRect.top + pixelRect.bottom) / 2;
        const targetRect = map.getTargetElement().getBoundingClientRect();
        const hitPixel = [cx - targetRect.left, cy - targetRect.top];
        let hf = null;
        map.forEachFeatureAtPixel(hitPixel, (f) => {
          const t = f.getGeometry()?.getType?.() || '';
          if (/(Polygon|MultiPolygon)/i.test(t)) { hf = f; return true; }
          return false;
        }, { hitTolerance: 4 });
        if (hf) features = [hf];
      } catch {}
    }
    if (features.length === 0) return 0;

    let totalDeleted = 0;

    features.forEach(f => {
      const g = f.getGeometry(); if (!g) return;
      const t = g.getType();

      const targetRect = map.getTargetElement().getBoundingClientRect();

      // yardımçı: koordinat → ekran
      const toAbs = (coord) => {
        const [px, py] = map.getPixelFromCoordinate(coord) || [];
        return [targetRect.left + px, targetRect.top + py];
      };

      const src = findSourceOfFeature(f); // lazımdırsa feature-i siləcəyik

      if (t === 'Polygon') {
        const rings = g.getCoordinates();
        const res = deleteFromRings(rings, pixelRect, toAbs);
        if (res.removedFeature) {
          src && src.removeFeature(f);
        } else if (res.changed) {
          g.setCoordinates(rings);
          totalDeleted += res.deleted;
        }
      } else if (t === 'MultiPolygon') {
        const parts = g.getCoordinates();
        let deleted = 0, changed = false;

        // Hər part üçün işləyək; outer ring tam gedərsə — partı sil
        for (let p = parts.length - 1; p >= 0; p--) {
          const rings = parts[p];
          const res = deleteFromRings(rings, pixelRect, toAbs);
          deleted += res.deleted;
          if (res.removedFeature) {
            parts.splice(p, 1); // həmin partı tam ləğv et
            changed = true;
          } else if (res.changed) {
            changed = true;
          }
        }

        if (parts.length === 0) {
          src && src.removeFeature(f);
        } else if (changed) {
          g.setCoordinates(parts);
          totalDeleted += deleted;
        }
      }
    });

    return totalDeleted;
  }

  // rings: [outer, hole1, hole2, ...]
  function deleteFromRings(rings, pixelRect, toAbs) {
    if (!Array.isArray(rings) || rings.length === 0) return { deleted: 0, changed: false, removedFeature: false };
    let deleted = 0, changed = false;

    // əvvəlcə hər ring üçün silinəcək indeksləri topla
    const ringPlans = rings.map(r => []);
    for (let r = 0; r < rings.length; r++) {
      const ring = rings[r];
      for (let i = 0; i < ring.length; i++) {
        const c = ring[i];
        const [ax, ay] = toAbs(c);
        if (pointInsideRect(ax, ay, pixelRect)) ringPlans[r].push(i);
      }
    }

    // indi tətbiq et
    // holes üçün: valid qalmasa (len<4), bütövlükdə deşiyi siləcəyik
    const holesToRemove = [];

    // outer ring
    if (ringPlans[0] && ringPlans[0].length) {
      const r0 = rings[0];
      const out = hardDeleteIndicesFromRing(r0, ringPlans[0]);
      deleted += out.deleted;
      changed = changed || (out.deleted > 0);
      if (!out.valid) {
        // outer ring düşdüsə: bu poliqon partı tam ləğv olunmalıdır
        return { deleted, changed: true, removedFeature: true };
      }
      rings[0] = r0;
    }

    // holes
    for (let r = 1; r < rings.length; r++) {
      if (!ringPlans[r] || ringPlans[r].length === 0) continue;
      const rr = rings[r];
      const out = hardDeleteIndicesFromRing(rr, ringPlans[r]);
      deleted += out.deleted;
      changed = changed || (out.deleted > 0);
      if (!out.valid) holesToRemove.push(r);
      else rings[r] = rr;
    }

    // deşikləri indeks azalan qaydada sil
    if (holesToRemove.length) {
      holesToRemove.sort((a, b) => b - a).forEach(idx => rings.splice(idx, 1));
      changed = true;
    }

    return { deleted, changed, removedFeature: false };
  }

  // ================= TOOL BEHAVIOR =================
  function activateTool(tool) {
    deactivateTool();
    ST.currentTool = tool;
    setButtonsActive(tool);

    if (tool === 'delete') {
      freezeMap();              // xəritəni dondur
      wireBoxHandlers(true);    // rectangle
      tip('Siçanla düzbucaq çək – içində qalan vertexlər silinəcək');
    } else if (tool === 'add') {
      // mövcud mexanizm (A düyməsi)
      try { document.dispatchEvent(new KeyboardEvent('keydown', { key: 'a', code: 'KeyA' })); } catch {}
      tip('Poliqon sərhədinə klik – vertex əlavə olunur');
    } else if (tool === 'select') {
      tip('Seçim üçün mövcud alətlərdən istifadə edin');
    }
  }

  function deactivateTool() {
    wireBoxHandlers(false);
    unfreezeMap();

    try {
      document.dispatchEvent(new KeyboardEvent('keyup', { key: 'a', code: 'KeyA' }));
      document.dispatchEvent(new KeyboardEvent('keyup', { key: 'd', code: 'KeyD' }));
    } catch {}

    ST.currentTool = null;
    setButtonsActive(null);
  }

  // ================= INTEGRATION =================
  function hijackVertexButton() {
    const btn = document.getElementById('rtEditVertices');
    if (!btn) { setTimeout(hijackVertexButton, 400); return; }
    if (btn._rtBound) return;

    const cloned = btn.cloneNode(true);
    btn.parentNode.replaceChild(cloned, btn);

    cloned.addEventListener('click', (e) => {
      e.preventDefault(); e.stopPropagation();
      if (window.TEKUIS_VERTEX_EDIT && typeof window.TEKUIS_VERTEX_EDIT.toggle === 'function') {
        const before = !!window.TEKUIS_VERTEX_EDIT.isEnabled?.();
        window.TEKUIS_VERTEX_EDIT.toggle();
        if (!before) showToolbar(); else hideToolbar();
      } else {
        if (ST.toolbar?.classList.contains('visible')) hideToolbar(); else showToolbar();
      }
    });

    cloned._rtBound = true;
  }

  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && ST.toolbar?.classList.contains('visible')) hideToolbar();
  });

  function init() {
    if (ST.inited) return;
    if (!window.map || !window.ol) { setTimeout(init, 200); return; }
    ST.inited = true;
    createToolbar();
    hijackVertexButton();
    log('ready');
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else setTimeout(init, 100);

  // Public
  window.VertexToolbar = {
    show: showToolbar,
    hide: hideToolbar,
    isVisible: () => !!ST.toolbar?.classList.contains('visible')
  };
})();
