/* =========================
   TICKET
   ========================= */
const PAGE_TICKET = window.PAGE_TICKET || null;
const {
  map,
  basemapApi,
  mapOverlays,
  tekuisSource,
  tekuisLayer,
  necasSource,
  necasLayer,
  infoHighlightSource,
  topoErrorSource,
  topoFocusSource,
  topoFocusLayer,
  renderTopoErrorsOnMap,
  zoomAndHighlightTopoGeometry,
  pulseTopoHighlight,
  setInfoHighlight
} = window.MapContext || {};

window.map = map;
window.basemapApi = basemapApi;
window.mapOverlays = mapOverlays;


window.tv = TekuisValidator.init({
  map,
  ticket: PAGE_TICKET || '',
  metaId: (typeof window.META_ID !== 'undefined' ? window.META_ID : null)
});


const authFetchTicketStatus = window.fetchTicketStatus;
const authApplyEditPermissions = window.applyEditPermissions;


const applyNoDataCardState = (...args) => window.LayersPanel?.applyNoDataCardState?.(...args);
const setCardDisabled = (...args) => window.LayersPanel?.setCardDisabled?.(...args);
const updateTicketDeleteState = (...args) => window.LayersPanel?.updateTicketDeleteState?.(...args);
const renderLayersPanel = (...args) => window.LayersPanel?.renderLayersPanel?.(...args);
window.renderLayersPanel = renderLayersPanel;


let fetchTekuisByBboxForLayer = () => {};
let fetchTekuisByAttachTicket = () => Promise.resolve();
let refreshTekuisFromAttachIfAny = () => Promise.resolve();
let refreshNecasFromAttachIfAny = () => Promise.resolve();
let clearTekuisCache = () => {};
let tekuisHasCache = () => false;

// === Feature ownership map (feature → source) ===
const trackFeatureOwnership = window.FeatureOwnership?.trackFeatureOwnership;
const getFeatureOwner = window.FeatureOwnership?.getOwner;


['addfeature','removefeature','changefeature'].forEach(ev => {
  tekuisSource.on(ev, () => {
    saveTekuisToLS();
    // Geometriya dəyişdi → həm OK, həm də əvvəlki ignore-ları sıfırla
    window._topoLastOk = null;
    window._lastTopoValidation = null;
    window._ignoredTopo = { overlaps: new Set(), gaps: new Set() };
  });
});


/* =========================
   PANEL/INDIKATOR
   ========================= */
const {
  panelEl,
  panelBodyEl,
  indicatorEl,
  openPanel,
  closePanel,
  moveIndicatorToButton
} = window.PanelUI || {};

/* =========================
   Lay idarəsi (import olunan laylar üçün)
   ========================= */

const uploadLayerApi = window.setupUploadedLayer?.({
  map,
  registerSnapSource,
  onResetTekuis: () => {
    tekuisSource.clear(true);
    tekuisCount = 0;
    const lblT = document.getElementById('lblTekuisCount');
    if (lblT) lblT.textContent = '(0)';
  }
});

const styleByGeom = window.styleByGeom;
const styleTicketDefault = window.styleTicketDefault;
const styleAttachDefault = window.styleAttachDefault;



const uploadHandlers = window.setupUploadHandlers?.({
  ticket: PAGE_TICKET,
  uploadLayerApi,
  updateAllSaveButtons
});


const lastUploadState = uploadHandlers?.lastUploadState || window.lastUploadState;

/* =========================
   ATTACH upload helper (save zamanı çağırılır)
   ========================= */
async function uploadAttachmentToBackend(file, crs){

  if (!window.EDIT_ALLOWED) {
    return { ok:false, message:'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!' };
  }


  if (!file || !PAGE_TICKET) return { ok:false, message:'Fayl və ya ticket yoxdur' };
  try{
    const fd = new FormData();
    fd.append('file', file);
    fd.append('ticket', PAGE_TICKET);
    if (crs) fd.append('crs', crs); // Backend CSV/TXT üçün coordinate_system sütununa insanoxunan dəyəri yazacaq

    const resp = await fetch('/api/attach/upload/', { method: 'POST', body: fd });
    if (!resp.ok) throw new Error((await resp.text()) || `HTTP ${resp.status}`);
    const data = await resp.json();
    return { ok:true, data };
  }catch(e){
    console.error(e);
    return { ok:false, message: e && e.message ? e.message : 'Attach yükləmə alınmadı' };
  }
}

/* =========================
   REDAKTƏ
   ========================= */
const editSource = new ol.source.Vector();
trackFeatureOwnership(editSource);
const editLayer  = new ol.layer.Vector({
  source: editSource,
  style: new ol.style.Style({
    fill:   new ol.style.Fill({ color: 'rgba(245, 158, 11, 0.20)' }),
    stroke: new ol.style.Stroke({ color: '#f59e0b', width: 2 })
  })
});
map.addLayer(editLayer);

const selectInteraction = new ol.interaction.Select({
  layers: (layer) => layer === editLayer,
  style: new ol.style.Style({
    fill:   new ol.style.Fill({ color: 'rgba(220,38,38,0.15)' }),
    stroke: new ol.style.Stroke({ color: '#dc2626', width: 3 })
  })
});
map.addInteraction(selectInteraction);

const vertexStyle = new ol.style.Style({
  image: new ol.style.Circle({
    radius: 5,
    fill:   new ol.style.Fill({ color: '#ffffff' }),
    stroke: new ol.style.Stroke({ color: '#dc2626', width: 2 })
  })
});
const modifyInteraction = new ol.interaction.Modify({
  features: selectInteraction.getFeatures(),
  style: vertexStyle,
  insertVertexCondition: ol.events.condition.never
});
map.addInteraction(modifyInteraction);

function redSelectStyleFn(feature){
  const t = feature.getGeometry().getType();
  if (t === 'Point' || t === 'MultiPoint'){
    return new ol.style.Style({
      image: new ol.style.Circle({
        radius: 6,
        fill: new ol.style.Fill({ color: 'rgba(220,38,38,0.9)' }),
        stroke: new ol.style.Stroke({ color: '#ffffff', width: 1.5 })
      })
    });
  } else if (t === 'LineString' || t === 'MultiLineString'){
    return new ol.style.Style({
      stroke: new ol.style.Stroke({ color: '#dc2626', width: 3 })
    });
  }
  return new ol.style.Style({
    fill: new ol.style.Fill({ color: 'rgba(220,38,38,0.15)' }),
    stroke: new ol.style.Stroke({ color: '#dc2626', width: 3 })
  });
}
const selectAny = new ol.interaction.Select({
  layers: (layer) => layer instanceof ol.layer.Vector && !layer.get('selectIgnore'),
  style: redSelectStyleFn,
  hitTolerance: 3
});
map.addInteraction(selectAny);

// --- BÜTÜN SEÇİLƏ BİLƏN laylar üçün vertex modify (TEKUİS də daxil)
const modifyAnyInteraction = new ol.interaction.Modify({
  features: selectAny.getFeatures(),
  style: vertexStyle,
  insertVertexCondition: ol.events.condition.never
});
map.addInteraction(modifyAnyInteraction);

// --- INFO MODE üçün edit/select interaction-ları pauza/bərpa helperləri ---
let _preInfoActive = null;
function pauseEditingInteractions(){
  _preInfoActive = {
    selectAny:           selectAny.getActive?.() ?? true,
    selectInteraction:   selectInteraction.getActive?.() ?? true,
    modifyAny:           modifyAnyInteraction.getActive?.() ?? true,
    modifyInteraction:   modifyInteraction.getActive?.() ?? true
  };
  try { selectAny.setActive(false); } catch {}
  try { selectInteraction.setActive(false); } catch {}
  try { modifyAnyInteraction.setActive(false); } catch {}
  try { modifyInteraction.setActive(false); } catch {}
}

function resumeEditingInteractions(){
  if (_preInfoActive){
    try { selectAny.setActive(_preInfoActive.selectAny); } catch {}
    try { selectInteraction.setActive(_preInfoActive.selectInteraction); } catch {}
    try { modifyAnyInteraction.setActive(_preInfoActive.modifyAny); } catch {}
    try { modifyInteraction.setActive(_preInfoActive.modifyInteraction); } catch {}
    _preInfoActive = null;
  } else {
    // fallback
    try { selectAny.setActive(true); } catch {}
    try { selectInteraction.setActive(true); } catch {}
    try { modifyAnyInteraction.setActive(true); } catch {}
    try { modifyInteraction.setActive(true); } catch {}
  }
}


// TEKUİS dəyişirsə, LS-ə yaz və UI-ni yenilə
modifyAnyInteraction.on('modifyend', () => {
  try { saveTekuisToLS(); } catch {}
  updateAllSaveButtons?.();
});


/* ---------- SNAP ---------- */
const snapState = { enabled: false, interactions: [] };
const snapSources = new Set();

function addSnapForSource(src){
  const snap = new ol.interaction.Snap({
    source: src, pixelTolerance: 12, edge: true, vertex: true
  });
  map.addInteraction(snap);
  snapState.interactions.push(snap);
}

/* NEW: Snap interaction-ları stack-in SONUNA at (Draw/Modify-dən sonra olsun) */
function refreshSnapOrder(){
  if (!snapState.enabled) return;
  const snaps = snapState.interactions.slice();
  snaps.forEach(i => map.removeInteraction(i));
  snapState.interactions = [];
  snapSources.forEach(addSnapForSource);  // yenidən əlavə → ən sonda olurlar
}

function registerSnapSource(src){
  if (!src || snapSources.has(src)) return;
  snapSources.add(src);
  if (snapState.enabled){
    addSnapForSource(src);
    refreshSnapOrder(); // NEW: draw aktivdirsə sıra düzəlsin
  }
}

function enableSnap(){
  if (snapState.enabled) return;
  snapState.enabled = true;
  snapSources.forEach(addSnapForSource);
  refreshSnapOrder();                   // NEW
  updateSnapBtnUI && updateSnapBtnUI();
}

function disableSnap(){
  if (!snapState.enabled && snapState.interactions.length === 0) return;
  snapState.enabled = false;
  snapState.interactions.forEach(i => map.removeInteraction(i));
  snapState.interactions = [];
  updateSnapBtnUI && updateSnapBtnUI();
}

function toggleSnap(){ snapState.enabled ? disableSnap() : enableSnap(); }

registerSnapSource(editSource);


// Draw (Polygon)
let drawInteraction = null;

function startDraw(){
  if (drawInteraction) return;


  drawInteraction = new ol.interaction.Draw({ source: editSource, type: 'Polygon' });
  map.addInteraction(drawInteraction);


  if (snapState.enabled) {
    refreshSnapOrder();
  } else {
    enableSnap();
  }

  drawInteraction.on('drawend', (e) => {
    const f = e.feature;
    const sel = selectInteraction.getFeatures();
    sel.clear(); sel.push(f);
    updateEditStatus && updateEditStatus('Yeni poliqon əlavə edildi. Bitirmək üçün double-click.');
    updateDeleteButtonState && updateDeleteButtonState();
    updateAllSaveButtons();
  });

  updateDrawBtnUI && updateDrawBtnUI(true);
  updateEditStatus && updateEditStatus('Çəkmə aktivdir. Snap açıqdır.');
}

function stopDraw(silent=false){
  if (drawInteraction) {
    map.removeInteraction(drawInteraction);
    drawInteraction = null;
  }
  disableSnap();
  if (!silent) {
    updateDrawBtnUI && updateDrawBtnUI(false);
    updateEditStatus && updateEditStatus('Çəkmə dayandırıldı. Snap bağlıdır.');
  }
}



/* =============== YADDA SAXLA (frontend) =============== */
function getUnifiedSelectedFeatures(){
  const a = selectAny.getFeatures().getArray();
  const b = selectInteraction.getFeatures().getArray();
  const set = new Set([...a, ...b]);
  return Array.from(set);
}

// Seçilmiş feature hansı laydandır? (yalnız Vector laylar)
function findVectorLayerAndSourceOfFeature(f){
  if (!f) return null;

  // 1) Əvvəlcə birbaşa ownership xəritəsindən götür
  const ownedSrc = getFeatureOwner?.(f);
  if (ownedSrc) {
    // Həmin source-u daşıyan layer-i tapırıq
    let ownedLayer = null;
    (map.getLayers()?.getArray?.() || []).forEach(layer => {
      if (ownedLayer || !(layer instanceof ol.layer.Vector)) return;
      if (layer.getSource && layer.getSource() === ownedSrc) ownedLayer = layer;
      if (!ownedLayer && layer instanceof ol.layer.Group) {
        (layer.getLayers()?.getArray?.() || []).forEach(l => {
          if (!ownedLayer && l instanceof ol.layer.Vector && l.getSource() === ownedSrc) ownedLayer = l;
        });
      }
    });
    return { layer: ownedLayer, source: ownedSrc };
  }

  // 2) Fallback: xəritədə skan et (köhnə məntiq)
  let hit = null;
  const scan = (layer) => {
    if (hit || !layer) return;
    if (layer instanceof ol.layer.Group) {
      const arr = layer.getLayers()?.getArray?.() || [];
      arr.forEach(scan);
      return;
    }
    if (!(layer instanceof ol.layer.Vector)) return;
    const src = layer.getSource && layer.getSource();
    if (!src?.hasFeature) return;
    if (src.hasFeature(f)) hit = { layer, source: src };
  };
  (map.getLayers()?.getArray?.() || []).forEach(scan);
  return hit;
}



function isTekuisLayer(layer){
  if (!(layer instanceof ol.layer.Vector)) return false;
  const title = (layer.get('title') || '').toString().toLowerCase();
  // Bizim marker: isTekuisEditable və ya adında "tekuis" keçməsi
  return layer.get('isTekuisEditable') === true || title.includes('tekuis');
}

function isTekuisSource(src){
  if (!src) return false;
  // Lokal tekuisSource-dursa
  if (typeof tekuisSource !== 'undefined' && src === tekuisSource) return true;

  // Xarici/yeni yaradılmış TEKUİS layını tapıb mənbə ilə müqayisə et
  try {
    const ext = findExternalTekuisLayer && findExternalTekuisLayer();
    return !!(ext && src === ext.getSource());
  } catch { return false; }
}




// --- YENİ: istənilən sayda poliqon seçimi
function getSelectedPolygons(){
  const arr = getUnifiedSelectedFeatures();
  return arr.filter(f=>{
    const g = f.getGeometry();
    if (!g) return false;
    const t = g.getType();
    return t === 'Polygon' || t === 'MultiPolygon';
  });
}
function hasAtLeastOnePolygonSelected(){
  return getSelectedPolygons().length >= 1;
}

function updateAllSaveButtons(){
  const hasPoly = hasAtLeastOnePolygonSelected();
  const btn1 = document.getElementById('btnSaveDataPanel');
  if (btn1) btn1.disabled = !hasPoly;
  const btn2 = document.getElementById('btnSaveEditPanel');
  if (btn2) btn2.disabled = !hasPoly;

  if (rtEditUI && rtEditUI.btnSave) {
    rtEditUI.btnSave.disabled = !hasPoly;
  }
}

const wktWriter = new ol.format.WKT();


function composeSelectedPolygonsWKT(){
  const selected = getSelectedPolygons();
  if (selected.length === 0) return null;


  const polyCoords3857 = [];
  selected.forEach(f=>{
    const g = f.getGeometry();
    if (!g) return;
    const t = g.getType();
    if (t === 'Polygon'){
      polyCoords3857.push(g.getCoordinates());
    } else if (t === 'MultiPolygon'){
      const parts = g.getCoordinates();
      parts.forEach(rings => polyCoords3857.push(rings));
    }
  });

  if (polyCoords3857.length === 0) return null;

  let geom;
  if (polyCoords3857.length === 1){
    geom = new ol.geom.Polygon(polyCoords3857[0]);
  } else {
    geom = new ol.geom.MultiPolygon(polyCoords3857);
  }

  const geom4326 = geom.clone().transform('EPSG:3857','EPSG:4326');
  return wktWriter.writeGeometry(geom4326, { decimals: 8 });
}


async function saveSelected({ alsoAttach=true } = {}){

  // <-- əvvəlcə sürətli pre-check-lər (overlay açmadan)
  if (!window.EDIT_ALLOWED) {
    Swal.fire('Diqqət', 'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'warning');
    return;
  }

  const wkt = composeSelectedPolygonsWKT();
  if (!wkt) {
    Swal.fire('Diqqət', 'Yadda saxlamaq üçün ekranda <b>ən azı 1 poliqon</b> seçilməlidir.', 'warning');
    return;
  }

  if (!PAGE_TICKET){
    Swal.fire('Diqqət', 'Ticket tapılmadı. Zəhmət olmasa Node tətbiqindən yenidən “Xəritəyə keç” edin.', 'warning');
    return;
  }

  // <-- indi overlay-i açırıq və HƏR HALDA bağlamaq üçün try/finally qoyuruq
  let hideLoading = () => {};
  try {
    hideLoading = (window.RTLoading ? RTLoading.show('Məlumat yadda saxlanır…') : () => {});

    // 1) Poliqonu saxla
    let polyId = null;
    try {
      const resp = await fetch('/api/save-polygon/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Accept':'application/json' },
        body: JSON.stringify({ wkt: wkt, ticket: PAGE_TICKET })
      });

      if (resp.status === 409) {
        let msg = 'Məlumatlar artıq yadda saxlanılıb!';
        try { msg = (await resp.json())?.message || msg; } catch {}
        Swal.fire('Info', msg, 'info');
        return;
      }



      if (!resp.ok) throw new Error(await resp.text() || `HTTP ${resp.status}`);
      const data = await resp.json();
      polyId = data?.id ?? null;
    } catch (e) {
      console.error(e);
      Swal.fire('Xəta', e.message || 'Bazaya yazmaq alınmadı.', 'error');
      return; // finally işləyəcək və overlay bağlanacaq
    }

    // 2) Qoşmaları (varsə) yüklə
    let attachOk = false;
    if (alsoAttach && lastUploadState.file){
      RTLoading.set('Qoşmalar saxlanır…'); // mətnini dəyiş
      try {
        const up = await uploadAttachmentToBackend(lastUploadState.file, lastUploadState.crs);
        attachOk = !!(up && up.ok);
        if (attachOk){
          await loadAttachLayer({ fit:false });
          updateTicketDeleteState();
        }
      } catch (e) {
        console.error(e);
        attachOk = false;
      }
    }

    // 3) Nəticə mesajı
    if (attachOk){
      Swal.fire('Uğurlu', `Poliqon və qoşmalar yadda saxlandı.`, 'success');
    } else if (!lastUploadState.file){
      Swal.fire('Uğurlu', `Poliqon yadda saxlandı.`, 'success');
    } else {
      Swal.fire('Qismən uğurlu', `Poliqon yadda saxlandı, lakin qoşmalar saxlanmadı.`, 'warning');
    }

    updateAllSaveButtons();

  } finally {
    // OVERLAY-İ HƏR HALDA BAĞLA
    try { hideLoading(); } catch {}
  }
}


// Seçim event-ləri
selectAny.getFeatures().on('add', updateAllSaveButtons);
selectAny.getFeatures().on('remove', updateAllSaveButtons);
selectInteraction.getFeatures().on('add', updateAllSaveButtons);
selectInteraction.getFeatures().on('remove', updateAllSaveButtons);
selectInteraction.getFeatures().on('add', updateDeleteButtonState);
selectInteraction.getFeatures().on('remove', updateDeleteButtonState);

selectAny.getFeatures().on('add',    updateDeleteButtonState);
selectAny.getFeatures().on('remove', updateDeleteButtonState);


map.on('click', updateAllSaveButtons);

selectInteraction.on('select', () => { updateDeleteButtonState(); updateAllSaveButtons(); });

/* =========================
   “Məlumatlar” – INFO MODE
   ========================= */
const infoModeApi = window.setupInfoMode?.({
  map,
  tekuisLayer,
  necasLayer,
  infoHighlightSource,
  openPanel,
  moveIndicatorToButton,
  setInfoHighlight,
  stopDraw,
  selectAny,
  selectInteraction,
  pauseEditingInteractions,
  resumeEditingInteractions
});
const enableInfoMode = infoModeApi?.enableInfoMode;
const disableInfoMode = infoModeApi?.disableInfoMode;
const toggleInfoMode = infoModeApi?.toggleInfoMode;




/* =========================
   Sağ toolbar: BÜTÜN düymələr
   ========================= */
const rtEditUI = {
  btnInfo:   null,
  btnErase:  null,
  btnDraw:   null,
  btnSnap:   null,
  btnDelete: null,
  btnClear:  null,
  btnSave:   null
};

function injectRightEditButtons(){
  const host = document.getElementById('rightTools');
  if (!host) return;

  // Köməkçi: PNG ikonlu rt-btn yarat
  const mkBtn = (id, title, iconKey) => {
    if (document.getElementById(id)) return document.getElementById(id);
    const b = document.createElement('button');
    b.id = id;
    b.className = 'rt-btn';
    b.title = title || '';
    const img = document.createElement('img');
    img.className = 'rt-icon-img';
    img.alt = title || id;
    img.src = (window.RT_ICONS && window.RT_ICONS[iconKey]) || '';
    b.appendChild(img);
    host.appendChild(b);
    return b;
  };


  rtEditUI.btnInfo   = mkBtn('rtInfo',      'İnformasiya (obyektə kliklə)', 'info');
  rtEditUI.btnDraw   = mkBtn('rtDraw',      'Poliqon çək / dayandır',       'draw');
  rtEditUI.btnSnap   = mkBtn('rtSnap',      'Snap aç/bağla',                'snap');
  rtEditUI.btnDelete = mkBtn('rtDeleteSel', 'Seçiləni sil',                 'deleteSel');
  rtEditUI.btnClear  = mkBtn('rtClearAll',  'Hamısını sil',                 'clearAll');
  rtEditUI.btnSave   = mkBtn('rtSave',      'Yadda saxla',                  'save');
  rtEditUI.btnErase  = mkBtn('rtErase',     'Tədqiqat daxilini kəs & sil',  'erase');

  // ===== EVENT LISTENER-LƏR =====

  // 1) Info düyməsi
  rtEditUI.btnInfo.addEventListener('click', toggleInfoMode);

  // 2) Erase düyməsi
  rtEditUI.btnErase.addEventListener('click', () => {
    if (!window.EDIT_ALLOWED) {
      Swal.fire('Diqqət', 'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'info');
      return;
    }
    const btn = document.getElementById('btnEraseTekuisInsideTicket');
    if (btn && typeof btn.click === 'function') {
      btn.click();
    } else {
      Swal.fire('Info', 'Erase funksiyası hazırda əlçatan deyil. "Laylar" panelini açın.', 'info');
    }
  });

  // 3) Draw düyməsi
  rtEditUI.btnDraw.addEventListener('click', () => {
    if (!window.EDIT_ALLOWED) {
      Swal.fire('Diqqət', 'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'info');
      return;
    }
    if (drawInteraction) { stopDraw(); } else { startDraw(); }
  });

  // 4) Snap düyməsi
  rtEditUI.btnSnap.addEventListener('click', () => {
    if (!window.EDIT_ALLOWED) {
      Swal.fire('Diqqət', 'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'info');
      return;
    }
    toggleSnap();
    rtEditUI.btnSnap.classList.toggle('active', !!snapState.enabled);
  });

  // 5) Delete düyməsi — Tədqiqat layı + TEKUİS layı üçün işləsin
  rtEditUI.btnDelete.addEventListener('click', () => {
    if (!window.EDIT_ALLOWED) {
      Swal.fire('Diqqət', 'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'info');
      return;
    }

    // Hər iki selection-dan BİRGƏ siyahı (təkrarsız)
    const arrA = selectAny.getFeatures().getArray();
    const arrB = selectInteraction.getFeatures().getArray();
    const unified = Array.from(new Set([...arrA, ...arrB]));

    if (unified.length === 0) {
      Swal.fire('Info', 'Seçilmiş obyekt yoxdur.', 'info');
      return;
    }

    // Yalnız bu iki laydan silirik:
    // - Tədqiqat layı (editLayer/editSource)
    // - TEKUİS Parsellər (tekuisLayer/tekuisSource)
    let removed = 0;
    unified.forEach(f => {
      const hit = findVectorLayerAndSourceOfFeature(f);
      if (!hit) return;

      const isTicket = (hit.layer === editLayer || hit.source === editSource);
      const isTekuis = isTekuisLayer(hit.layer) || isTekuisSource(hit.source);


      if (isTicket || isTekuis) {
        try { hit.source.removeFeature(f); removed++; } catch {}
        try { selectInteraction.getFeatures().remove(f); } catch {}
        try { selectAny.getFeatures().remove(f); } catch {}
      }
    });

    if (removed > 0) {
      updateDeleteButtonState();
      updateEditStatus && updateEditStatus('Seçilmiş obyekt(lər) silindi.');
      updateAllSaveButtons();
    } else {
      Swal.fire('Info', 'Seçilənlər arasında silinə bilən obyekt yoxdur.', 'info');
    }
  });


  // 6) Clear düyməsi
  rtEditUI.btnClear.addEventListener('click', () => {
    if (!window.EDIT_ALLOWED) {
      Swal.fire('Diqqət', 'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'info');
      return;
    }
    editSource.clear();
    selectInteraction.getFeatures().clear();
    updateDeleteButtonState();
    updateEditStatus && updateEditStatus('Bütün obyektlər silindi.');
    updateAllSaveButtons();
  });

  // 7) Save düyməsi
  rtEditUI.btnSave.addEventListener('click', () => {
    if (!window.EDIT_ALLOWED) {
      Swal.fire('Diqqət', 'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'info');
      return;
    }
    saveSelected({ alsoAttach:true });
  });


  rtEditUI.btnMove = (function mk() {
    if (document.getElementById('rtMove')) return document.getElementById('rtMove');
    const b = document.createElement('button');
    b.id = 'rtMove';
    b.className = 'rt-btn';
    b.title = 'Obyekti sürüşdür (Move)';
    const img = document.createElement('img');
    img.className = 'rt-icon-img';
    img.alt = 'Move';
    img.src = (window.RT_ICONS && window.RT_ICONS.move) || '';
    b.appendChild(img);
    document.getElementById('rightTools').appendChild(b);
    return b;
  })();


  // Move düyməsi
  rtEditUI.btnMove.addEventListener('click', () => {
    if (!window.EDIT_ALLOWED) {
      Swal.fire('Diqqət', 'Bu əməliyyatı yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'info');
      return;
    }
    if (window.RTMove && typeof RTMove.toggle === 'function') {
      RTMove.toggle();
    }
  });





  // Başlanğıc UI vəziyyəti
  rtEditUI.btnSnap.classList.toggle('active', !!snapState.enabled);
  rtEditUI.btnDelete.disabled = (selectInteraction.getFeatures().getLength() === 0);
  rtEditUI.btnSave.disabled   = !hasAtLeastOnePolygonSelected();
}

// Xəritə hazır olanda düymələri daxil et
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', injectRightEditButtons);
} else {
  injectRightEditButtons();
}


if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', () => {
    if (window.RTMove && typeof RTMove.init === 'function') {
      RTMove.init({ map });
    }
  });
} else {
  if (window.RTMove && typeof RTMove.init === 'function') {
    RTMove.init({ map });
  }
}



// Çək düyməsinin aktivliyi
function updateDrawBtnUI(isActive){
  if (rtEditUI && rtEditUI.btnDraw) {
    rtEditUI.btnDraw.classList.toggle('active', !!isActive);
  }
}

function updateDeleteButtonState(){
  // Birlikdə seçim: selectAny + selectInteraction
  const arrA = selectAny.getFeatures().getArray();
  const arrB = selectInteraction.getFeatures().getArray();
  const unified = Array.from(new Set([...arrA, ...arrB]));

  // Yalnız Tədqiqat və TEKUİS layına aid seçilmişlər sayılır
  const deletableCount = unified.filter(f => {
    const hit = findVectorLayerAndSourceOfFeature(f);
    if (!hit) return false;
    const isTicket = (hit.layer === editLayer || hit.source === editSource);
    const isTekuis = isTekuisLayer(hit.layer) || isTekuisSource(hit.source);
    return isTicket || isTekuis;

  }).length;

  if (rtEditUI && rtEditUI.btnDelete) {
    rtEditUI.btnDelete.disabled = (deletableCount === 0);
  }
}


// Status mətni (sağ toolbar-da xətti status göstərmirik)
function updateEditStatus(text){ /* no-op sağ toolbar üçün */ }

// Snap düyməsi
function updateSnapBtnUI(){
  const on = !!snapState.enabled;
  if (rtEditUI && rtEditUI.btnSnap) {
    rtEditUI.btnSnap.classList.toggle('active', on);
  }
}


/* =========================
   “Məlumat daxil et” paneli
   ========================= */

const dataPanelApi = window.setupDataPanel?.({
  openPanel,
  panelBodyEl,
  uploadHandlers
});

/* =========================
   Basemaps paneli
   ========================= */
const basemapsPanelApi = window.setupBasemapsPanel?.({
  openPanel,
  panelBodyEl,
  basemapApi: window.basemapApi
});





/* =========================
   LS: görünmə və TEKUİS keş
   ========================= */
const tekuisCache = window.setupTekuisCache?.({
  pageTicket: PAGE_TICKET,
  tekuisSource,
  selectAny,
  getFeatureOwner,
  onCountChange: (count) => {
    tekuisCount = count;
    const lbl = document.getElementById('lblTekuisCount');
    if (lbl) lbl.textContent = `(${tekuisCount})`;
  }
});
window.tekuisCache = tekuisCache;
const readVis = tekuisCache?.readVis;
const writeVis = tekuisCache?.writeVis;
const setVisFlag = tekuisCache?.setVisFlag;
const getVisFlag = tekuisCache?.getVisFlag;
const saveTekuisToLS = tekuisCache?.saveTekuisToLS;
const loadTekuisFromLS = tekuisCache?.loadTekuisFromLS;
window.saveTekuisToLS = saveTekuisToLS;

// === Topologiya Modalı + TEKUİS: validate → (modal) → save =================

const tryValidateAndSaveTekuis = (...args) => window.tryValidateAndSaveTekuis?.(...args);

/* =========================
   Laylar paneli
   ========================= */
let ticketLayer = null;
let ticketLayerSource = null;
let ticketLayerCount = 0;

async function loadTicketLayer({ fit=true } = {}){
  if (!PAGE_TICKET){
    Swal.fire('Diqqət', 'Ticket tapılmadı. Bu bölmə üçün ticket tələb olunur.', 'warning');
    return { ok:false, count:0 };
  }
  try {
    const resp = await fetch(`/api/layers/by-ticket/?ticket=${encodeURIComponent(PAGE_TICKET)}`, {
      headers: { 'Accept':'application/json' }
    });
    if (!resp.ok){
      throw new Error(await resp.text() || `HTTP ${resp.status}`);
    }
    const fc = await resp.json();

    const format   = new ol.format.GeoJSON();
    const features = format.readFeatures(fc, {
      dataProjection: 'EPSG:4326',
      featureProjection: 'EPSG:3857'
    });

    ticketLayerSource = new ol.source.Vector({ features });
    trackFeatureOwnership(ticketLayerSource);
    ticketLayerCount = features.length;

    if (ticketLayer) map.removeLayer(ticketLayer);
    ticketLayer = new ol.layer.Vector({
      source: ticketLayerSource,
      style:  styleTicketDefault,
      visible: true,
      zIndex: 5
    });
    ticketLayer.set('title', 'Tədqiqat layı');   // ⬅️ ƏLAVƏ
    window.ticketLayer = ticketLayer;            // ⬅️ ƏLAVƏ
    map.addLayer(ticketLayer);

    registerSnapSource(ticketLayerSource);

    if (fit && ticketLayerCount > 0){
      const ext = ticketLayerSource.getExtent();
      map.getView().fit(ext, { padding: [20,20,20,20], duration: 600, maxZoom: 18 });
    }if (document.getElementById('cardTicket')){
      setCardDisabled('cardTicket', ticketLayerCount === 0);
    }

    return { ok:true, count: ticketLayerCount };
  } catch(err){
    console.error(err);
    Swal.fire('Xəta', (err && err.message) || 'Ticket obyektləri yüklənmədi.', 'error');
    return { ok:false, count:0 };
  }
}

/* ---- Attach layı ---- */
let attachLayer = null;
let attachLayerSource = null;
let attachLayerCount = 0;

async function loadAttachLayer({ fit=false } = {}){
  if (!PAGE_TICKET){
    Swal.fire('Diqqət', 'Ticket tapılmadı.', 'warning');
    return { ok:false, count:0 };
  }
  try{
    const resp = await fetch(`/api/attach/geojson/by-ticket/?ticket=${encodeURIComponent(PAGE_TICKET)}`, {
      headers: { 'Accept':'application/json' }
    });
    if (!resp.ok) throw new Error((await resp.text()) || `HTTP ${resp.status}`);
    const fc = await resp.json();

    const format   = new ol.format.GeoJSON();
    const features = format.readFeatures(fc, {
      dataProjection: 'EPSG:4326',
      featureProjection: 'EPSG:3857'
    });

    attachLayerSource = new ol.source.Vector({ features });
    trackFeatureOwnership(attachLayerSource);
    attachLayerCount = features.length;

    if (attachLayer) map.removeLayer(attachLayer);

    // İlk görünmə dəyərini LS-dən götür (yoxdursa false)
    const visAttach = getVisFlag('attach', false);

    attachLayer = new ol.layer.Vector({
      source: attachLayerSource,
      style:  styleAttachDefault,
      visible: visAttach,  // <-- artıq ilk andaca düz görünəcək
      zIndex: 6
    });
    attachLayer.set('title', 'Qoşma lay');
    window.attachLayer = attachLayer;
    map.addLayer(attachLayer);

    // Panel checkbox-u ilə dərhal sinxronlaşdır
    const chkAttach = document.getElementById('chkAttachLayer');
    if (chkAttach) {
      chkAttach.checked = visAttach;
      if (!chkAttach._wired) {
        chkAttach.addEventListener('change', (e) => {
          const on = !!e.target.checked;
          setVisFlag('attach', on);
          attachLayer.setVisible(on);
        });
        chkAttach._wired = true;
      }
    }


    if (document.getElementById('cardAttach')){
      setCardDisabled('cardAttach', attachLayerCount === 0);
    }

    registerSnapSource(attachLayerSource);

    if (fit && attachLayerCount > 0){
      const ext = attachLayerSource.getExtent();
      map.getView().fit(ext, { padding:[20,20,20,20], duration:600, maxZoom:18 });
    }


    if (attachLayerCount > 0){
  // TEKUİS lokalda (LS) varsa, üstələmirik
      if (!tekuisHasCache()){
        await refreshTekuisFromAttachIfAny(false);
      }
      await refreshNecasFromAttachIfAny();
    } else {
      if (!tekuisHasCache()){
        tekuisSource.clear(true);
        tekuisCount = 0;
      }
      necasSource.clear(true);
      necasCount  = 0;
  }





    return { ok:true, count: attachLayerCount };
  }catch(err){
    console.error(err);
    Swal.fire('Xəta', (err && err.message) || 'Attach layı yüklənmədi.', 'error');
    return { ok:false, count:0 };
  }
}


let tekuisCount = 0;
let necasCount  = 0;

const tekuisNecasApi = window.TekuisNecas?.create({
  applyNoDataCardState,
  getPageTicket: () => PAGE_TICKET,
  getTekuisCount: () => tekuisCount,
  setTekuisCount: (val) => { tekuisCount = val; },
  getNecasCount: () => necasCount,
  setNecasCount: (val) => { necasCount = val; },
  getAttachLayer: () => attachLayer,
  getAttachLayerSource: () => attachLayerSource,
  getTekuisLayer: () => tekuisLayer,
  getTekuisSource: () => tekuisSource,
  getNecasLayer: () => necasLayer,
  getNecasSource: () => necasSource
});

if (tekuisNecasApi) {
  ({
    fetchTekuisByBboxForLayer,
    fetchTekuisByAttachTicket,
    refreshTekuisFromAttachIfAny,
    refreshNecasFromAttachIfAny,
    clearTekuisCache,
    tekuisHasCache
  } = tekuisNecasApi);
}

window.addEventListener('beforeunload', () => {
  clearTekuisCache();
});


// === 3s “glow” animasiyası ===
const _flashState = new WeakMap();

function flashLayer(layer, { duration = 1000, hz = 2 } = {}) {
  if (!layer || !layer.getVisible() || !layer.getSource()) return;
  const src = layer.getSource();
  const features = src.getFeatures ? src.getFeatures() : [];
  if (!features || features.length === 0) return;

 
  const stopOld = _flashState.get(layer);
  if (stopOld) stopOld();

  const original = new Map();
  features.forEach(f => original.set(f, f.getStyle())); 

  let running = true;
  let phase = 0; 
  const t0 = performance.now();


  const styleFn = (feat, res) => buildFlashStyle(feat, phase);
  features.forEach(f => f.setStyle(styleFn));

  let raf = null;
  function frame(now) {
    if (!running) return;
    const dt = now - t0;
    if (dt >= duration) return cleanup();

    phase = 0.5 + 0.5 * Math.sin((dt / 1000) * 2 * Math.PI * hz);
    layer.changed();
    raf = requestAnimationFrame(frame);
  }

  function cleanup() {
    if (!running) return;
    running = false;
    if (raf) cancelAnimationFrame(raf);
    features.forEach(f => f.setStyle(original.get(f) || null));
    layer.changed();
    _flashState.delete(layer);
  }

  _flashState.set(layer, cleanup);
  raf = requestAnimationFrame(frame);
}

// --- Tək FEATURE üçün mavi glow (1s) ---
const _flashFeatureState = new WeakMap();

// --- Tək FEATURE üçün mavi glow (1s) ---
// QAYTARIR: cleanup() – animasiyanı dərhal dayandırmaq üçün
function flashFeature(feature, { duration = 1000, hz = 2.5, baseColor = '#60a5fa' } = {}) {
  if (!feature || !feature.getGeometry) return () => {};
  // Köhnə animasiyanı dayandır
  const stopOld = _flashFeatureState.get(feature);
  if (stopOld) stopOld();

  const originalStyle = feature.getStyle ? feature.getStyle() : null;

  function hexToRgb(hex){ const m=/^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
    return m ? {r:parseInt(m[1],16), g:parseInt(m[2],16), b:parseInt(m[3],16)} : {r:96,g:165,b:250}; }
  const {r,g,b}=hexToRgb(baseColor), rgba=(a)=>`rgba(${r},${g},${b},${a})`;

  let running = true, phase = 0, raf = null, t0 = performance.now();

  const styleBuilder = (feat, ph) => {
    const t = feat.getGeometry().getType();
    const outerA=0.20+0.45*ph, fillA=0.05+0.12*ph, coreW=2+2*ph;
    if (/Point/i.test(t)) {
      return new ol.style.Style({
        image: new ol.style.Circle({
          radius: 5 + 3*ph,
          fill:   new ol.style.Fill({ color: rgba(0.25 + 0.35*ph) }),
          stroke: new ol.style.Stroke({ color: baseColor, width: coreW })
        })
      });
    }
    if (/LineString/i.test(t)) {
      return [
        new ol.style.Style({ stroke: new ol.style.Stroke({ color: rgba(outerA), width: 6 + 6*ph }) }),
        new ol.style.Style({ stroke: new ol.style.Stroke({ color: baseColor,   width: coreW }) })
      ];
    }
    // Polygon / MultiPolygon
    return [
      new ol.style.Style({ fill:   new ol.style.Fill({ color: rgba(fillA) }) }),
      new ol.style.Style({ stroke: new ol.style.Stroke({ color: rgba(outerA), width: 6 + 6*ph }) }),
      new ol.style.Style({ stroke: new ol.style.Stroke({ color: baseColor,    width: coreW }) })
    ];
  };
  const dynamicStyleFn = (feat) => styleBuilder(feat, phase);
  feature.setStyle(dynamicStyleFn);

  function frame(now){
    if (!running) return;
    const dt = now - t0;
    if (dt >= duration) { cleanup(); return; }
    phase = 0.5 + 0.5 * Math.sin((dt/1000) * 2 * Math.PI * hz);
    const layer = feature.getLayer ? feature.getLayer(map) : null;
    if (layer && layer.changed) layer.changed();
    raf = requestAnimationFrame(frame);
  }
  function cleanup(){
    if (!running) return;
    running = false;
    if (raf) cancelAnimationFrame(raf);
    feature.setStyle(originalStyle || null);
    const layer = feature.getLayer ? feature.getLayer(map) : null;
    if (layer && layer.changed) layer.changed();
    _flashFeatureState.delete(feature);
  }
  _flashFeatureState.set(feature, cleanup);
  raf = requestAnimationFrame(frame);
  return cleanup;
}
window.flashFeature = flashFeature;



// Hər geometriya üçün parlaq “glow” stili
function buildFlashStyle(feature, phase) {
  const t = feature.getGeometry().getType();
  const outerA = 0.15 + 0.35 * phase;
  const fillA  = 0.06 + 0.12 * phase;
  const glowRGBA = (a) => `rgba(255, 223, 0, ${a})`;
  const coreHex  = '#0bdaf5'; 

  if (t === 'Point' || t === 'MultiPoint') {
    return new ol.style.Style({
      image: new ol.style.Circle({
        radius: 5 + 4 * phase,
        fill:   new ol.style.Fill({ color: glowRGBA(0.25 + 0.35 * phase) }),
        stroke: new ol.style.Stroke({ color: '#0bdaf5', width: 1.5 + 1.0 * phase })
      })
    });
  }

  if (t === 'LineString' || t === 'MultiLineString') {
    return [
      new ol.style.Style({ // kənar “glow”
        stroke: new ol.style.Stroke({ color: glowRGBA(outerA), width: 6 + 6 * phase })
      }),
      new ol.style.Style({ // parlaq nüvə
        stroke: new ol.style.Stroke({ color: coreHex, width: 2 + 2 * phase })
      })
    ];
  }

  // Polygon / MultiPolygon
  return [
    new ol.style.Style({ // doldurma parıltısı
      fill: new ol.style.Fill({ color: glowRGBA(fillA) })
    }),
    new ol.style.Style({ // kənar “glow”
      stroke: new ol.style.Stroke({ color: glowRGBA(outerA), width: 6 + 6 * phase })
    }),
    new ol.style.Style({ // parlaq nüvə xətti
      stroke: new ol.style.Stroke({ color: coreHex, width: 2 + 2 * phase })
    })
  ];
}


const layersPanelApi = window.setupLayersPanel?.({
  openPanel,
  map,
  pageTicket: PAGE_TICKET,
  getCSRFToken,
  flashLayer,
  getVisFlag,
  setVisFlag,
  loadTicketLayer,
  loadAttachLayer,
  refreshTekuisFromAttachIfAny,
  refreshNecasFromAttachIfAny,
  clearTekuisCache,
  tryValidateAndSaveTekuis,
  getTicketLayer: () => ticketLayer,
  getTicketLayerSource: () => ticketLayerSource,
  getTicketLayerCount: () => ticketLayerCount,
  getAttachLayer: () => attachLayer,
  getAttachLayerSource: () => attachLayerSource,
  getAttachLayerCount: () => attachLayerCount,
  getTekuisLayer: () => tekuisLayer,
  getTekuisSource: () => tekuisSource,
  getTekuisCount: () => tekuisCount,
  setTekuisCount: (val) => { tekuisCount = val; },
  getNecasLayer: () => necasLayer,
  getNecasSource: () => necasSource,
  getNecasCount: () => necasCount,
  setNecasCount: (val) => { necasCount = val; }

});



if (layersPanelApi) {
  window.LayersPanel = layersPanelApi;
}





/* =========================
   Sol düymələr
   ========================= */
document.querySelectorAll('.tool-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tool-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    moveIndicatorToButton(btn);

    const which = btn.dataset.panel;

    if (!window.EDIT_ALLOWED && (which === 'contents' || which === 'catalog')) {
      Swal.fire('Diqqət', 'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'info');
      return;
    }

    if (which === 'contents') {
      dataPanelApi?.renderDataPanel?.();
    } else if (which === 'catalog') {
      Swal.fire('Məlumat', 'Redaktə alətləri indi sağdakı toolbar-dadır.', 'info');
    } else if (which === 'symbology') {
      basemapsPanelApi?.renderBasemapsPanel?.();
    } else if (which === 'layers') {
      renderLayersPanel();
    } else if (which === 'info') {
      openPanel('Məlumatlar', '<div class="card"><div class="small">Sağdakı mavi düymə ilə “İnformasiya” modunu aktivləşdirin, sonra obyektə klik edin.</div></div>');
    } else {
      openPanel(btn.textContent.trim(), '');
    }
  });
});

window.addEventListener('resize', () => {
  map.updateSize(); // <-- ƏSAS
  const activeBtn = document.querySelector('.tool-btn.active');
  if (activeBtn && !indicatorEl.hidden && !panelEl.hidden) {
    moveIndicatorToButton(activeBtn);
  }
});

// Başlanğıc
window.basemapApi?.setBasemap('google');

// Ticket statusunu yüklə və icazələri tətbiq et
authFetchTicketStatus?.();

// --- İlk yükləmədə qoşma layını və TEKUİS/NECAS-ı serverdən gətir
(async () => {
  if (!PAGE_TICKET) return;

  // Qoşma layını yüklə (fit=false: avtomatik zoom etməsin)
  await loadAttachLayer({ fit: false });

  // TEKUİS/NECAS-ı qoşma geometriyasına görə serverdən yenilə
  await refreshTekuisFromAttachIfAny(true);  // LS-ə baxmadan serverdən gətir
  await refreshNecasFromAttachIfAny();

  // TEKUİS görünməsini ilkin qaydaya sal (panel hələ açılmamış ola bilər)
  const wantTekuisVisible = getVisFlag('tekuis', true);
  tekuisLayer.setVisible(
    wantTekuisVisible && tekuisSource.getFeatures().length > 0
  );

  // NECAS üçün də eyni (istəsəniz)
  const wantNecasVisible = getVisFlag('necas', false);
  necasLayer.setVisible(
    wantNecasVisible && necasSource.getFeatures().length > 0
  );
})();





// Map-i mənbə extent-inə sağlam şəkildə oturtmaq üçün helper
function fitMapToSource(source, opts = {}) {
  const { padding=[20,20,20,20], duration=600, maxZoom=18 } = opts;
  if (!source || source.getFeatures().length === 0) return false;

  const ext = source.getExtent();
  if (!ext || !isFinite(ext[0]) || !isFinite(ext[2])) return false;

  // Tək nöqtə olduqda / 0 ölçülü extentdə kiçik buffer
  const w = ext[2] - ext[0], h = ext[3] - ext[1];
  const ext2 = (w === 0 && h === 0 && ol.extent?.buffer) ? ol.extent.buffer(ext, 50) : ext;

  // Layout dəyişibsə (panel/ sidebar animasiyası) ölçünü yenilə
  map.updateSize();
  map.getView().fit(ext2, { padding, duration, maxZoom, size: map.getSize() });
  return true;
}

// Map render tamam olanda auto yoxlama
map.once('rendercomplete', () => {
  autoOpenLayersIfTicketHasData();
});


async function autoOpenLayersIfTicketHasData() {
  if (!PAGE_TICKET) return;

  try {

    const res = await loadTicketLayer({ fit: false });
    if (!(res && res.ok && res.count > 0)) return;


    renderLayersPanel();
    const btn = document.querySelector('.tool-btn[data-panel="layers"]');
    if (btn) {
      document.querySelectorAll('.tool-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      moveIndicatorToButton(btn);
    }


    let didAutoZoom = false;
    const runZoomOnce = () => {
      if (didAutoZoom) return;
      didAutoZoom = true;
      fitMapToSource(ticketLayerSource);
    };

    const onEnd = (e) => {
      if (e.propertyName === 'transform') {
        panelEl.removeEventListener('transitionend', onEnd);
        runZoomOnce();
      }
    };

    panelEl.addEventListener('transitionend', onEnd);

    // Fallback: panel animasiyası işləməsə, 800ms sonra CƏMİ BİR DƏFƏ fit et
    setTimeout(runZoomOnce, 800);
  } catch (e) {
    console.warn('Auto-open layers failed:', e);
  }
}



// Sidebar expand/collapse
const sidebarEl = document.querySelector('.sidebar');
const sidebarToggleBtn = document.getElementById('sidebarToggle');
function setSidebarExpanded(expanded){
  sidebarEl.classList.toggle('expanded', expanded);
  sidebarToggleBtn.textContent = expanded ? '«««' : '»»»';
  sidebarToggleBtn.title = expanded ? 'Yığ' : 'Genişləndir';
  const activeBtn = document.querySelector('.tool-btn.active');
  if (activeBtn && !panelEl.hidden) moveIndicatorToButton(activeBtn);
}
sidebarToggleBtn.addEventListener('click', ()=>{
  setSidebarExpanded(!sidebarEl.classList.contains('expanded'));
});
setSidebarExpanded(false);
sidebarEl.addEventListener('transitionend', (e)=>{
  if (e.propertyName === 'width'){
    map.updateSize(); // <-- ƏSAS
    const activeBtn = document.querySelector('.tool-btn.active');
    if (activeBtn && !panelEl.hidden) moveIndicatorToButton(activeBtn);
  }
});