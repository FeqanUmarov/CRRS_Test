(() => {
// === Topologiya Modalı + TEKUİS: validate → (modal) → save =================
const PAGE_TICKET = window.PAGE_TICKET || null;
const mapContext = window.MapContext || {};
const map = window.map || mapContext.map;
const tekuisSource = mapContext.tekuisSource || window.tekuisSource;
const topoErrorSource = mapContext.topoErrorSource;
const topoFocusSource = mapContext.topoFocusSource;
const renderTopoErrorsOnMap = mapContext.renderTopoErrorsOnMap || window.renderTopoErrorsOnMap;
const zoomAndHighlightTopoGeometry = mapContext.zoomAndHighlightTopoGeometry || window.zoomAndHighlightTopoGeometry;
const saveTekuisToLocal = () => window.saveTekuisToLS?.();
const clearTekuisCache = () => window.tekuisCache?.clearTekuisCache?.();

// TEKUİS GeoJSON FeatureCollection (EPSG:4326) çıxarır
function getTekuisFeatureCollection() {
  const src = getTekuisSourceSmart();
  const features = (src?.getFeatures?.() || []);
  const gjFmt = new ol.format.GeoJSON();
  return gjFmt.writeFeaturesObject(features, {
    dataProjection: 'EPSG:4326',
    featureProjection: 'EPSG:3857'
  });
}


// --- Topologiya ikonları (istəyə görə dəyişin)
window.TOPO_ICONS = Object.assign({
  zoom:     '/static/icons/images/visual.png',
  close:    '/static/icons/images/close.png',
  ignore:   '/static/icons/images/eye-off.png',
  unignore: '/static/icons/images/undo.png'
}, window.TOPO_ICONS || {});


// --- Modal UI (dinamik yaradılır) ------------------------------------------
let _topoModal = null;
function ensureTopologyModal(){
  if (_topoModal) return _topoModal;

  const style = document.createElement('style');
  style.textContent = `
    .topo-overlay{
      position:fixed;inset:0;background:rgba(0,0,0,.25);
      z-index:9998;display:none;pointer-events:none; /* arxadakı xəritəyə kliklər keçsin */
    }
    .topo-modal{
      position:fixed;left:50%;top:72px;transform:translateX(-50%);
      width:min(820px,calc(100vw - 32px));max-height:calc(100vh - 120px);
      overflow:auto;background:#fff;border-radius:12px;box-shadow:0 12px 40px rgba(0,0,0,.25);
      z-index:9999;display:none;font-family:sans-serif;
      resize: both; /* diaqonal ölçü dəyiş */
    }
    .topo-head{
      position:sticky;top:0;display:flex;align-items:center;justify-content:space-between;
      padding:12px 16px;background:#2e463c;color:#fff;border-top-left-radius:12px;border-top-right-radius:12px;
      cursor:move; user-select:none; /* başlıqdan tutub daşı */
    }
    .topo-title{font-weight:600}
    .topo-close{border:0;background:transparent;color:#fff;font-size:18px;cursor:pointer}
    .topo-body{padding:14px 16px;display:grid;gap:10px}
    .topo-section{border:1px solid #e6e6e6;border-radius:10px;padding:10px}
    .topo-section h4{margin:0 0 8px 0;font-size:14px}
    .topo-list{display:grid;gap:8px}
    .topo-item{display:flex;align-items:center;justify-content:space-between;padding:8px;border:1px dashed #d9d9d9;border-radius:8px}
    .topo-actions{display:flex;gap:8px}
    .btn.link{background:transparent;border:0;color:#0b5ed7;text-decoration:underline}
    .topo-foot{display:flex;justify-content:flex-end;gap:8px;padding:10px 16px;border-top:1px solid #eee}
    .topo-item.ignored{ opacity:.55; }
    .badge-ignored{ margin-left:8px;padding:2px 6px;border-radius:6px;background:#fef3c7;color:#92400e;font-size:12px; }
    .hidden{ display:none; }
    .btn .ico{ width:16px;height:16px;object-fit:contain;vertical-align:middle;margin-right:6px; }
    .btn.icon-only .ico{ margin-right:0; }
    .topo-close.icon-only{ padding:6px; }

    .topo-actions .btn {
      background: transparent;
      border: 1px solid #d1d5db;
      color: #374151;
    }
    .topo-actions .btn:hover {
      background: #f3f4f6;
      border-color: #9ca3af;
    }

    .swal2-container{ z-index:11000 !important; }

  `;

  document.head.appendChild(style);

  const overlay = document.createElement('div');
  overlay.className = 'topo-overlay';

  const modal = document.createElement('div');
  modal.className = 'topo-modal';
  modal.innerHTML = `
    <div class="topo-head">
      <div class="topo-title">Topologiya xətaları tapıldı</div>
      <button class="topo-close" title="Bağla">✕</button>
    </div>
    <div class="topo-body">
      <div class="topo-section">
        <h4>Ümumi məlumat</h4>
        <div id="topo-summary"></div>
      </div>
      <div class="topo-section" id="topo-overlaps-sec" style="display:none">
        <h4>Kəsişmələr (overlap)</h4>
        <div class="topo-list" id="topo-overlaps"></div>
      </div>
      <div class="topo-section" id="topo-gaps-sec" style="display:none">
        <h4>Boşluqlar (gap)</h4>
        <div class="topo-list" id="topo-gaps"></div>
      </div>
    </div>
    <div class="topo-foot">
      <button class="btn primary" id="btnTopoClose">
        <img class="ico" src="${window.TOPO_ICONS.close}" alt=""> Bağla
      </button>
    </div>
  `;
  modal.querySelector('.topo-close').addEventListener('click', closeTopologyModal);
modal.querySelector('#btnTopoClose').addEventListener('click', async () => {
  const v = window._lastTopoValidation || {};
  const eff = computeEffective(v);
  
  // Əgər həll edilməmiş xətalar varsa xəbərdarlıq göstər
  if (eff.overlapsLeft > 0 || eff.gapsLeft > 0) {
    const ask = await Swal.fire({
      title: 'Xətalar həll edilməyib',
      html: `Hələ də <b>${eff.overlapsLeft} overlap</b> və <b>${eff.gapsLeft} gap</b> xətası var.<br><br>Pəncərəni bağlasanız, növbəti dəfə <b>yenidən yoxlanacaq</b>. Pəncərə bağlansın?`,
      icon: 'warning',
      showCancelButton: true,
      confirmButtonText: 'Bəli, bağla',
      cancelButtonText: 'Xeyr, geri qayıt'
    });
    if (!ask.isConfirmed) return;
    // Xətalar qalıbsa, OK bayrağını sıfırla
    window._topoLastOk = null;
  }
  
  closeTopologyModal();
});


  document.body.append(overlay, modal);
  _topoModal = { overlay, modal };
  return _topoModal;
}

// === Topologiya: "Bunu xəta kimi sayma" dəstəyi ===
window._ignoredTopo = window._ignoredTopo || { overlaps: new Set(), gaps: new Set() };

function _djb2(str){ let h=5381,i=str.length; while(i) h=(h*33) ^ str.charCodeAt(--i); return (h>>>0).toString(36); }
// Koordinatları/obyekti deterministik yuvarlaqlaşdır (6 rəqəm)
function roundDeep(x, d=6){
  if (Array.isArray(x)) return x.map(v => roundDeep(v, d));
  if (typeof x === 'number') return +x.toFixed(d);
  if (x && typeof x === 'object'){
    const out = {};
    Object.keys(x).sort().forEach(k => { out[k] = roundDeep(x[k], d); });
    return out;
  }
  return x;
}

// (Əvvəlki cavabda artıq var idisə) roundDeep-dən istifadə edirik
function normalizeFC(fc, d = 6){
  return roundDeep(fc, d);
}

function fcHash(fc){
  try { return 'h' + _djb2(JSON.stringify(normalizeFC(fc, 6))); }
  catch { return 'h' + Math.random().toString(36).slice(2); }
}

// Son uğurlu yoxlama bayrağı
window._topoLastOk = window._topoLastOk || null;  // {hash, ts, eff}


// Stabil topo açarı: geom-u 6 rəqəm dəqiqliklə normallaşdırıb hashla
function topoKey(obj){
  try {
    const g = obj?.geom ?? obj;                 // GeoJSON geometry və ya obyekt
    const norm = JSON.stringify(roundDeep(g, 6));
    return 'k' + _djb2(norm);
  } catch {
    return 'k' + Math.random().toString(36).slice(2);
  }
}


function computeEffective(validation){
  const ovs = validation?.overlaps || [];
  const gps = validation?.gaps || [];
  let ignoredO = 0, ignoredG = 0;
  for (const it of ovs) if (window._ignoredTopo.overlaps.has(topoKey(it))) ignoredO++;
  for (const it of gps) if (window._ignoredTopo.gaps.has(topoKey(it)))    ignoredG++;
  return {
    overlapsTotal: ovs.length, gapsTotal: gps.length,
    overlapsIgnored: ignoredO, gapsIgnored: ignoredG,
    overlapsLeft: ovs.length - ignoredO, gapsLeft: gps.length - ignoredG
  };
}

function buildIgnoredPayloadFromValidation(validation){
  const ovSet = window._ignoredTopo?.overlaps || new Set();
  const gpSet = window._ignoredTopo?.gaps || new Set();

  const overlapsAll = validation?.overlaps || [];
  const gapsAll     = validation?.gaps || [];

  const ignoredOverlaps = [];
  const ignoredOverlapKeys = [];
  overlapsAll.forEach(it => {
    const key = topoKey(it);
    if (ovSet.has(key)) {
      ignoredOverlapKeys.push(key);
      if (it?.geom) ignoredOverlaps.push(it.geom); // GeoJSON geometry 4326
    }
  });

  const ignoredGaps = [];
  const ignoredGapKeys = [];
  gapsAll.forEach(it => {
    const key = topoKey(it);
    if (gpSet.has(key)) {
      ignoredGapKeys.push(key);
      if (it?.geom) ignoredGaps.push(it.geom);
    }
  });

  return {
    overlaps: ignoredOverlaps,
    gaps: ignoredGaps,
    overlap_keys: ignoredOverlapKeys,
    gap_keys: ignoredGapKeys
  };
}


// Kontekst menyusu
function showTopoContextMenu(ev, kind, itemKey, isIgnored){
  const { modal } = ensureTopologyModal();
  let menu = modal.querySelector('.topo-ctx');
  if (!menu){
    menu = document.createElement('div');
    menu.className = 'topo-ctx';
    menu.style.cssText = 'position:absolute;z-index:10000;background:#fff;border:1px solid #eee;border-radius:8px;box-shadow:0 10px 30px rgba(0,0,0,.12);padding:6px;display:none;';
    modal.appendChild(menu);
    // modal daxilində boş yerə klik → menyunu gizlət
    modal.addEventListener('click', () => { menu.style.display = 'none'; }, true);
    modal.addEventListener('contextmenu', (e) => {
      // yalnız item-lərdə göstəririk; başqa yerdə sağ klik menyunu gizlət
      if (!e.target.closest('.topo-item')) menu.style.display = 'none';
    });
  }
  menu.innerHTML = '';
  const btn = document.createElement('button');
  btn.className = 'btn';
  btn.style.width = '240px';
  btn.innerHTML = isIgnored
    ? `<img class="ico" src="${window.TOPO_ICONS.unignore}" alt=""> Xəta kimi qeyd et`
    : `<img class="ico" src="${window.TOPO_ICONS.ignore}" alt="">`;

    btn.addEventListener('click', () => {
      const set = (kind === 'overlap') ? window._ignoredTopo.overlaps : window._ignoredTopo.gaps;
      if (isIgnored) set.delete(itemKey); else set.add(itemKey);

      // UI-ni yenilə
      const el = modal.querySelector(`.topo-item[data-kind="${kind}"][data-key="${itemKey}"]`);
      if (el) {
        const on = !isIgnored;
        el.classList.toggle('ignored', on);
        el.querySelector('.badge-ignored')?.classList.toggle('hidden', !on);
      }

      // ✅ YENİ: Dərhal effektiv say yenilə
      const v = window._lastTopoValidation || {};
      const eff = computeEffective(v);
      
      if (eff.overlapsLeft === 0 && eff.gapsLeft === 0) {
        const fc = getTekuisFeatureCollection();
        window._topoLastOk = { hash: fcHash(fc), ts: Date.now(), eff };
      } else {
        window._topoLastOk = null;
      }

      menu.style.display = 'none';
    });
  menu.appendChild(btn);

  const r = modal.getBoundingClientRect();
  const x = Math.min(ev.clientX - r.left, r.width - 230);
  const y = Math.min(ev.clientY - r.top,  r.height - 60);
  menu.style.left = x + 'px';
  menu.style.top  = y + 'px';
  menu.style.display = 'block';
}

// --- Drag (title-dan tutub sürüşdür) ---
(() => {
  // modal/overlay DOM-da yoxdursa, yaradacaq; varsa, eyni obyektləri qaytaracaq
  const { modal } = ensureTopologyModal();
  const head = modal.querySelector('.topo-head');
  let dragging = false, sx=0, sy=0, sl=0, st=0;

  function clamp(v,min,max){ return Math.max(min, Math.min(max, v)); }

  head.addEventListener('mousedown', (e) => {
    if (e.button !== 0) return;
    dragging = true;
    const rect = modal.getBoundingClientRect();
    sx = e.clientX; sy = e.clientY; sl = rect.left; st = rect.top;
    modal.style.transform = 'none';
    document.body.style.userSelect = 'none';
    e.preventDefault();
  });

  window.addEventListener('mousemove', (e) => {
    if (!dragging) return;
    const dx = e.clientX - sx;
    const dy = e.clientY - sy;
    const nw = modal.offsetWidth;
    const nh = modal.offsetHeight;
    const L = clamp(sl + dx, 0, window.innerWidth  - Math.min(nw, window.innerWidth));
    const T = clamp(st + dy, 0, window.innerHeight - Math.min(nh, window.innerHeight));
    modal.style.left = `${L}px`;
    modal.style.top  = `${T}px`;
  });

  window.addEventListener('mouseup', () => {
    if (!dragging) return;
    dragging = false;
    document.body.style.userSelect = '';
  });
})();



function openTopologyModal(validation){
  window._lastTopoValidation = validation;
  const { overlay, modal } = ensureTopologyModal();

  const s = validation?.stats || {};
  const eff = computeEffective(validation);
  modal.querySelector('#topo-summary').innerHTML =
    `Feature sayı: <b>${s.n_features ?? 0}</b> &nbsp; | &nbsp; ` +
    `Overlap: <b>${eff.overlapsLeft}</b> / ${eff.overlapsTotal} (sayılmayan: ${eff.overlapsIgnored}) &nbsp; | &nbsp; ` +
    `Gap: <b>${eff.gapsLeft}</b> / ${eff.gapsTotal} (sayılmayan: ${eff.gapsIgnored})`;

// Overlaps
  const ovSec  = modal.querySelector('#topo-overlaps-sec');
  const ovList = modal.querySelector('#topo-overlaps');
  ovList.innerHTML = '';
  const overlaps = validation?.overlaps || [];
  if (overlaps.length){
    ovSec.style.display = '';
    overlaps.forEach((o, i) => {
      const key = topoKey(o);
      const ignored = window._ignoredTopo.overlaps.has(key);
      const el = document.createElement('div');
      el.className = 'topo-item' + (ignored ? ' ignored' : '');
      el.dataset.kind = 'overlap';
      el.dataset.key  = key;
      el.innerHTML = `
        <div>
          #${i+1} — sahə: <b>${o.area_sqm ?? '—'}</b> m²
          <span class="badge-ignored ${ignored ? '' : 'hidden'}">sayılmır</span>
        </div>
        <div class="topo-actions">
          <button class="btn" data-act="zoom" title="Xəritədə göstər">
            <img class="ico" src="${window.TOPO_ICONS.zoom}" alt="">
          </button>
        </div>`;
      // Zoom
      el.querySelector('[data-act=zoom]')?.addEventListener('click', () => {
        zoomAndHighlightTopoGeometry(o.geom);
      });
      // Toggle ignore (sol düymə ilə)
      el.querySelector('[data-act=toggleIgnore]')?.addEventListener('click', () => {
        const set = window._ignoredTopo.overlaps;
        const nowIgnored = set.has(key);
        if (nowIgnored) set.delete(key); else set.add(key);
        
        // UI-ni yenilə
        el.classList.toggle('ignored', !nowIgnored);
        el.querySelector('.badge-ignored')?.classList.toggle('hidden', nowIgnored);
        const btn = el.querySelector('[data-act=toggleIgnore]');
        if (btn) {
          btn.title = (!nowIgnored ? 'Xəta kimi qeyd et' : '');
          btn.innerHTML = `
            <img class="ico" src="${!nowIgnored ? window.TOPO_ICONS.unignore : window.TOPO_ICONS.ignore}" alt="">
            ${!nowIgnored ? 'Xəta kimi qeyd et' : ''}
          `;
        }
        
        // ✅ YENİ: Dərhal effektiv say yenilə və OK bayrağını qoy
        const v = window._lastTopoValidation || {};
        const eff = computeEffective(v);
        
        if (eff.overlapsLeft === 0 && eff.gapsLeft === 0) {
          // Bütün xətalar ignore edilib → OK bayrağını qoy
          const fc = getTekuisFeatureCollection();
          window._topoLastOk = { hash: fcHash(fc), ts: Date.now(), eff };
        } else {
          // Hələ xətalar qalıb → sıfırla
          window._topoLastOk = null;
        }
      });
      // Sağ klik kontekst menyusu da işləsin
      el.addEventListener('contextmenu', (e) => {
        e.preventDefault();
        const nowIgnored = window._ignoredTopo.overlaps.has(key);
        showTopoContextMenu(e, 'overlap', key, nowIgnored);
      });
      ovList.appendChild(el);
    });
  } else {
    ovSec.style.display = 'none';
  }


// Gaps
  const gpSec  = modal.querySelector('#topo-gaps-sec');
  const gpList = modal.querySelector('#topo-gaps');
  gpList.innerHTML = '';
  const gaps = validation?.gaps || [];
  if (gaps.length){
    gpSec.style.display = '';
    gaps.forEach((g, i) => {
      const key = topoKey(g);
      const ignored = window._ignoredTopo.gaps.has(key);
      const el = document.createElement('div');
      el.className = 'topo-item' + (ignored ? ' ignored' : '');
      el.dataset.kind = 'gap';
      el.dataset.key  = key;
      el.innerHTML = `
        <div>
          #${i+1} — boşluq sahəsi: <b>${g.area_sqm ?? '—'}</b> m²
          <span class="badge-ignored ${ignored ? '' : 'hidden'}">sayılmır</span>
        </div>
        <div class="topo-actions">
          <button class="btn" data-act="zoom" title="Xəritədə göstər">
            <img class="ico" src="${window.TOPO_ICONS.zoom}" alt="">
          </button>
          <button class="btn" data-act="toggleIgnore" title="${ignored ? 'Xəta kimi qeyd et' : ''}">
            <img class="ico" src="${ignored ? window.TOPO_ICONS.unignore : window.TOPO_ICONS.ignore}" alt="">
            ${ignored ? 'Xəta kimi qeyd et' : ''}
          </button>
        </div>`;
      // Zoom
      el.querySelector('[data-act=zoom]')?.addEventListener('click', () => {
        zoomAndHighlightTopoGeometry(g.geom);
      });
      // Gaps bölməsində
      el.querySelector('[data-act=toggleIgnore]')?.addEventListener('click', () => {
        const set = window._ignoredTopo.gaps;
        const nowIgnored = set.has(key);
        if (nowIgnored) set.delete(key); else set.add(key);
        
        // UI-ni yenilə
        el.classList.toggle('ignored', !nowIgnored);
        el.querySelector('.badge-ignored')?.classList.toggle('hidden', nowIgnored);
        const btn = el.querySelector('[data-act=toggleIgnore]');
        if (btn) {
          btn.title = (!nowIgnored ? 'Xəta kimi qeyd et' : '');
          btn.innerHTML = `
            <img class="ico" src="${!nowIgnored ? window.TOPO_ICONS.unignore : window.TOPO_ICONS.ignore}" alt="">
            ${!nowIgnored ? 'Xəta kimi qeyd et' : ''}
          `;
        }
        
        // ✅ YENİ: Dərhal effektiv say yenilə və OK bayrağını qoy
        const v = window._lastTopoValidation || {};
        const eff = computeEffective(v);
        
        if (eff.overlapsLeft === 0 && eff.gapsLeft === 0) {
          const fc = getTekuisFeatureCollection();
          window._topoLastOk = { hash: fcHash(fc), ts: Date.now(), eff };
        } else {
          window._topoLastOk = null;
        }
      });
      // Sağ klik kontekst menyusu da işləsin
      el.addEventListener('contextmenu', (e) => {
        e.preventDefault();
        const nowIgnored = window._ignoredTopo.gaps.has(key);
        showTopoContextMenu(e, 'gap', key, nowIgnored);
      });
      gpList.appendChild(el);
    });
  } else {
    gpSec.style.display = 'none';
  }

  renderTopoErrorsOnMap(validation);
  
  overlay.style.display = 'block';
  modal.style.display   = 'block';

}


function closeTopologyModal(){
  if (!_topoModal) return;
  _topoModal.overlay.style.display = 'none';
  _topoModal.modal.style.display   = 'none';


  try { topoFocusSource.clear(true); } catch {}
  try { topoErrorSource?.clear(true); } catch {}

  const v = window._lastTopoValidation || {};
  const eff = computeEffective(v);
  if (eff.overlapsLeft > 0 || eff.gapsLeft > 0) {
    window._topoLastOk = null;
  }
}




async function validateTekuisBothKinds(featureCollection){
  if (!window.tv || typeof window.tv.run !== 'function') {
    return { ok:true, validation:{ stats:{}, overlaps:[], gaps:[] } };
  }
  
  try{
    // Validator-da 'only' parametri işləmirsə, iki ayrı çağırış edirik
    let allOverlaps = [];
    let allGaps = [];
    let stats = {};
    
    // 1) Sadəcə overlap yoxlaması üçün konfiqurasiya
    try {
      const resOverlap = await window.tv.run({
        geojson: featureCollection,
        checkGaps: false,        // Gap-ları söndür
        checkOverlaps: true      // Yalnız overlap-ları yoxla
      });
      const vOv = normalizeValidation(resOverlap?.validation);
      allOverlaps = vOv.overlaps || [];
      stats = { ...stats, ...vOv.stats };
    } catch(e) {
      console.warn('Overlap yoxlaması xətası:', e);
    }
    
    // 2) Sadəcə gap yoxlaması üçün konfiqurasiya  
    try {
      const resGap = await window.tv.run({
        geojson: featureCollection,
        checkGaps: true,         // Yalnız gap-ları yoxla
        checkOverlaps: false     // Overlap-ları söndür
      });
      const vGap = normalizeValidation(resGap?.validation);
      allGaps = vGap.gaps || [];
      stats = { ...stats, ...vGap.stats };
    } catch(e) {
      console.warn('Gap yoxlaması xətası:', e);
    }
    
    // 3) Birləşdir
    const merged = {
      stats: {
        ...stats,
        n_features: stats?.n_features || 0
      },
      overlaps: allOverlaps,
      gaps: allGaps
    };

    console.log('✅ Validation nəticəsi:', {
      overlaps: allOverlaps.length,
      gaps: allGaps.length
    });

    return { ok:true, validation: merged };
  }catch(e){
    console.warn('validateTekuisBothKinds ümumi xətası:', e);
    return { ok:false, validation:{ stats:{}, overlaps:[], gaps:[] }, error: e?.message || 'validate error' };
  }
}


// Köhnə funksiya indi wrapper-i çağırır
async function validateTekuisLocal(featureCollection){
  return validateTekuisBothKinds(featureCollection);
}

function isValidFeatureCollection(fc){
  return !!fc && fc.type === 'FeatureCollection' && Array.isArray(fc.features);
}



// --- Serverdə yadda saxla ---------------------------------------------------
async function saveTekuisOnServer(featureCollection, { ignored, skipValidation, originalGeojson } = {}) {
  // Lokal header-lar
  const headers = { 'Content-Type': 'application/json', 'Accept': 'application/json' };

  // Ticket və meta_id-ni mövcud qlobalardan götür
  const ticket = (typeof PAGE_TICKET !== 'undefined' && PAGE_TICKET) ? PAGE_TICKET : '';
  const metaRaw = (typeof window.META_ID !== 'undefined') ? window.META_ID : null;
  const metaInt = metaRaw != null && String(metaRaw).trim() !== '' ? parseInt(metaRaw, 10) : null;

  const originalFc = originalGeojson || window.tekuisCache?.getOriginalTekuis?.();
  if (!isValidFeatureCollection(originalFc)) {
    return {
      ok: false,
      status: 400,
      data: { error: 'original_geojson FeatureCollection tələb olunur' }
    };
  }

  const body = { geojson: featureCollection, original_geojson: originalFc, ticket };
  if (Number.isFinite(metaInt)) body.meta_id = metaInt;

  // Eyni geometriyadırsa serverdə də validasiyanı ötür
  if (skipValidation) body.skip_validation = true;

  // İstifadəçinin “sayılmır” seçimi
  if (ignored && (
      (ignored.overlaps && ignored.overlaps.length) ||
      (ignored.gaps && ignored.gaps.length) ||
      (ignored.overlap_keys && ignored.overlap_keys.length) ||
      (ignored.gap_keys && ignored.gap_keys.length)
    )) {
    body.ignored = ignored;
    body.ignored_overlaps     = ignored.overlaps;
    body.ignored_gaps         = ignored.gaps;
    body.ignored_overlap_keys = ignored.overlap_keys;
    body.ignored_gap_keys     = ignored.gap_keys;
  }

  const resp = await fetch('/api/save-tekuis-parcels/', {
    method: 'POST',
    headers,
    body: JSON.stringify(body)
  });

  let data = null;
  const txt = await resp.text();
  try { data = JSON.parse(txt); } catch { data = { raw: txt }; }
  return { ok: resp.ok, status: resp.status, data };
}






// Xəritədə TEKUİS layını tap (başqa fayldan yaradılsa belə)
function findExternalTekuisLayer() {
  let found = null;
  map.getLayers().forEach(l => {
    if (found) return;
    if (!(l instanceof ol.layer.Vector)) return;
    const title = (l.get('title') || '').toString().toLowerCase();
    // bizim layda set etdiyimiz flag-lar da yoxlanır
    if (title.includes('tekuis') || l.get('isTekuisEditable') === true) {
      found = l;
    }
  });
  return found;
}

// TEKUİS mənbəyini ağıllı seç (əvvəl özümüzünkünü, yoxdursa xaricidən)
function getTekuisSourceSmart() {

  if (tekuisSource && tekuisSource.getFeatures &&
      tekuisSource.getFeatures().length > 0) {
    return tekuisSource;
  }

  const ext = findExternalTekuisLayer();
  return ext ? ext.getSource() : (tekuisSource || null);
}



async function tryValidateAndSaveTekuis(){
  if (!window.EDIT_ALLOWED) { 
    Swal.fire('Diqqət', 'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'warning');
    return; 
  }

  // ✅ YENİ: Attributes panel məlumatlarını feature-ə tətbiq et
  try {
    if (window.AttributesPanel && typeof window.AttributesPanel.applyUIToSelectedFeature === 'function') {
      const applied = window.AttributesPanel.applyUIToSelectedFeature();
      if (applied) {
        console.log('✅ Attributes panel dəyişiklikləri feature-ə tətbiq edildi');
      }
    }
  } catch (e) {
    console.warn('Attributes panel sync xətası (davam edirik):', e);
  }

  // Yenilənmiş feature-ləri LocalStorage-ə yaz
  try { saveTekuisToLocal(); } catch {}

  console.log('tryValidateAndSaveTekuis başladı, _topoLastOk:', window._topoLastOk);

  if (window._lastTopoValidation) {
    const eff = computeEffective(window._lastTopoValidation);
    if (eff.overlapsLeft > 0 || eff.gapsLeft > 0) {
      window._topoLastOk = null; // Məcburi yenidən yoxlama
    }
  }
  
  if (!PAGE_TICKET || !String(PAGE_TICKET).trim()){
    Swal.fire('Diqqət','Ticket tapılmadı. Node tətbiqindən yenidən "Xəritəyə keç" edin.','warning');
    return;
  }

  const originalFc = window.tekuisCache?.getOriginalTekuis?.();
  if (!isValidFeatureCollection(originalFc)) {
    Swal.fire('Xəta', 'Köhnə TEKUİS məlumatı tapılmadı. Zəhmət olmasa tekuis_parcel_old məlumatını yeniləyin.', 'error');
    return;
  }


  const src = getTekuisSourceSmart();
  const feats = src?.getFeatures?.() || [];
  if (feats.length === 0){
    Swal.fire('Info', 'Yadda saxlanacaq TEKUİS parseli yoxdur.', 'info');
    return;
  }

  const fc = getTekuisFeatureCollection();
  const curHash = fcHash(fc);

  let validationResult = null;
  let shouldSkipValidation = false;
  
// 1) Əgər _topoLastOk varsa və hash eynidir → validasiya SKIP et
  if (window._topoLastOk && window._topoLastOk.hash === curHash) {
    console.debug('✅ Topo skip: eyni geometriya, əvvəlki yoxlama OK idi');
    shouldSkipValidation = true;
    validationResult = window._lastTopoValidation || { stats: {}, overlaps: [], gaps: [] };
    
    // ✅ YENİ: Skip olunduqda da effektiv sayı yenilə (ignore dəyişə biləcəyi üçün)
    const eff = computeEffective(validationResult);
    
    // Əgər ignore-lardan sonra xəta qalıbsa, yenidən yoxla
    if (eff.overlapsLeft > 0 || eff.gapsLeft > 0) {
      console.warn('⚠️ Skip ediləcəkdi, amma ignore dəyişiklik var - yenidən yoxlanır');
      shouldSkipValidation = false; // Məcburi yoxlama
      window._topoLastOk = null;    // Köhnə hash sıfırla
    }
  }
  
  // 2) Skip edilməzsə → yenidən validate et
  if (!shouldSkipValidation) {
    const res = await validateTekuisLocal(fc);
    validationResult = res?.validation || {};
    window._lastTopoValidation = validationResult;
    
    const eff = computeEffective(validationResult);
    const hasErr = (eff.overlapsLeft > 0) || (eff.gapsLeft > 0);

    if (hasErr){
      openTopologyModal(validationResult);
      return;
    }
    
    // Xəta yoxdur → hash saxla
    window._topoLastOk = { hash: curHash, ts: Date.now(), eff };
  }

  // ===== TƏSDIQ =====
  const ask = await Swal.fire({
    title: 'Əminsiniz?',
    html: `<b>${feats.length}</b> TEKUİS parseli bazaya yazılacaq.`,
    icon: 'question',
    showCancelButton: true,
    confirmButtonText: 'Bəli, yadda saxla',
    cancelButtonText: 'İmtina'
  });
  
  if (!ask.isConfirmed) return;

  const ignoredPayload = buildIgnoredPayloadFromValidation(validationResult || {});

  // ===== SERVER SAVE =====
  try {
    const s = await saveTekuisOnServer(fc, {
      ignored: ignoredPayload,
      skipValidation: shouldSkipValidation,
      originalGeojson: originalFc
    });

    if (!s.ok){
      if (s.status === 422 && s.data?.validation){
        openTopologyModal(s.data.validation);
        return;
      }
      Swal.fire('Xəta', s.data?.error || 'TEKUİS parsellərini yadda saxlanılıb', 'error');
      return;
    }

    closeTopologyModal();
    clearTekuisCache();
    window.tekuisNecasApi?.markTekuisSaved?.(true);
    
    // State təmizlə
    window._ignoredTopo = { overlaps: new Set(), gaps: new Set() };
    window._topoLastOk = null;
    window._lastTopoValidation = null;

    if (s.data?.meta_id != null) {
      window.CURRENT_META_ID = s.data.meta_id;
    }

    try {
      const metaId = s.data?.meta_id ?? window.CURRENT_META_ID ?? null;
      await window.TekuisSwitch?.showSource?.('current', metaId);
    } catch (e) {
      console.warn('TEKUİS cari mənbə yenilənmədi:', e);
    }
    
    Swal.fire('Uğurlu', `${s.data?.saved_count ?? feats.length} TEKUİS parseli bazaya yazıldı.`, 'success');
    
  } catch(e) {
    console.error('Save error:', e);
    Swal.fire('Xəta', e.message || 'Şəbəkə xətası baş verdi.', 'error');
  }
}

window.tryValidateAndSaveTekuis = tryValidateAndSaveTekuis;

})();