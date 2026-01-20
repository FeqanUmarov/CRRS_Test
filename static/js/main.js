// --- Basemap: Google imagery ---
const googleImagery = new ol.layer.Tile({
  source: new ol.source.XYZ({
    url: 'https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',
    attributions: '© Google'
  })
});

const map = new ol.Map({
  target: 'map',
  layers: [googleImagery],
  view: new ol.View({ center: ol.proj.fromLonLat([47, 40]), zoom: 7 })
});

/* =========================
   TICKET
   ========================= */
function resolveTicket() {
  const fromApp = (window.APP && typeof APP.ticket === 'string') ? APP.ticket.trim() : '';
  const fromQS = (new URLSearchParams(window.location.search)).get('ticket') || '';
  const t = (fromApp || fromQS);
  return (t && t.length > 0) ? t.trim() : null;
}
const PAGE_TICKET = resolveTicket();


window.tv = TekuisValidator.init({
  map,
  ticket: PAGE_TICKET || '',
  metaId: (typeof window.META_ID !== 'undefined' ? window.META_ID : null)
});

// NEW: btnSaveTekuis klik handler-i (pre-check + save)
const btnSaveTekuis = document.getElementById('btnSaveTekuis');

if (btnSaveTekuis) {
  btnSaveTekuis.addEventListener('click', onSaveTekuisClick);
}

// btnSaveTekuis klik handler-i (pre-check + save + həmişə overlay-i bağla)
async function onSaveTekuisClick(){
  // ✅ YENİ: Attributes panel məlumatlarını saxla
  try {
    if (window.AttributesPanel && typeof window.AttributesPanel.applyUIToSelectedFeature === 'function') {
      window.AttributesPanel.applyUIToSelectedFeature();
    }
  } catch (e) {
    console.warn('Attributes sync xətası:', e);
  }

  // --- SÜRƏTLİ PRE-CHECKLƏR (overlay açmadan) ---
  if (!PAGE_TICKET) { showToast('Ticket tapılmadı.'); return; }

  const features = tekuisSource.getFeatures();
  if (!features || features.length === 0){
    showToast('Yadda saxlamaq üçün TEKUİS obyektləri tapılmadı.');
    return;
  }

  const gjFmt = new ol.format.GeoJSON();
  const fc = gjFmt.writeFeaturesObject(features, {
    featureProjection: 'EPSG:3857',
    dataProjection: 'EPSG:4326'
  });

  // Dissolve edilmiş tək (Multi)Polygon göndərmə
  if (fc.features.length === 1) {
    const t = fc.features[0]?.geometry?.type || '';
    if (t === 'Polygon' || t === 'MultiPolygon'){
      showToast('Dissolve edilmiş tək (Multi)Polygon göndərilə bilməz. Parselləri ayrı feature kimi saxla.');
      return;
    }
  }

  // Artıq saxlanıbsa, POST etmə (overlay açmadan)
  try{
    const r0 = await fetch(`/api/tekuis/exists?ticket=${encodeURIComponent(PAGE_TICKET)}`, {
      headers: { 'Accept':'application/json', 'X-Ticket': PAGE_TICKET }
    });
    if (r0.ok){
      const j0 = await r0.json();
      if (j0?.exists){
        showToast('TEKUİS parsellər local bazada yadda saxlanılıb');
        return;
      }
    }
  }catch{ /* şəbəkə xətası olsa da POST-da tutulacaq */ }

  // --- İNDİ OVERLAY-I AÇ VƏ HƏR HALDA BAĞLA ---
  let hideLoading = () => {};
  try{
    hideLoading = (window.RTLoading ? RTLoading.show('Məlumat yadda saxlanır…') : () => {});

    // 3) POST
    const resp = await fetch('/api/tekuis/save', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-CSRFToken': getCSRFToken(),
        'X-Ticket': PAGE_TICKET,
        'Accept': 'application/json'
      },
      body: JSON.stringify({ ticket: PAGE_TICKET, geojson: fc })
    });

    if (resp.status === 409) {
      let msg = 'TEKUİS parsellər local bazada yadda saxlanılıb';
      try { msg = (await resp.json())?.message || msg; } catch {}
      showToast(msg);
      return;
    }

    if (!resp.ok){
      const txt = await resp.text().catch(()=> '');
      showToast('Yadda saxlama xətası: ' + (txt || resp.status));
      return;
    }

    const out = await resp.json();
    if (out?.ok){
      showToast(`Saxlandı: ${out.saved_count || 0} parsel`);
    } else {
      showToast(out?.error || 'Bilinməyən xəta');
    }

  }catch(e){
    console.warn('Save TEKUIS error:', e);
    showToast('Şəbəkə xətası');
  }finally{
    // ✅ OVERLAY-İ HƏR HALDA BAĞLA
    try { hideLoading(); } catch {}
  }
}





const LOGIN_APP_BASE = 'http://10.11.1.73:8085/login';

// === CSRF helper (Django default: csrftoken cookie) ===
function getCSRFToken(){
  const m = document.cookie.match(/(?:^|;)\s*csrftoken=([^;]+)/);
  return m ? decodeURIComponent(m[1]) : '';
}

// toast helper
function showToast(msg, ms=2600){
  try{
    const el = document.createElement('div');
    el.textContent = msg;
    el.style.cssText = `
      position: fixed; left: 50%; top: 20px; transform: translateX(-50%);
      background: #111827; color:#fff; padding:10px 14px; border-radius:10px;
      box-shadow:0 10px 30px rgba(0,0,0,.18); z-index:100000;
      font: 14px/1.35 system-ui,-apple-system,Segoe UI,Roboto;
    `;
    document.body.appendChild(el);
    setTimeout(()=>{ el.style.transition='opacity .4s'; el.style.opacity='0'; }, ms);
    el.addEventListener('transitionend', ()=> el.remove());
  }catch(e){ alert(msg); }
}





// --- Auth Guard (ilk açılış + periodik yoxlama)
function showAuthGateAndRedirect(msg){
  try {
    const div = document.createElement('div');
    div.style.cssText = 'position:fixed;inset:0;background:#0b102033;backdrop-filter:blur(3px);display:flex;align-items:center;justify-content:center;z-index:99999;';
    div.innerHTML = `<div style="background:#fff;padding:20px 24px;border-radius:10px;box-shadow:0 10px 30px rgba(0,0,0,.12);font-family:sans-serif;max-width:520px;text-align:center">
      <h3 style="margin:0 0 8px">Sessiya bitmişdir</h3>
      <p style="margin:0 0 12px;color:#374151">${msg || 'Token etibarsız və ya vaxtı keçib.'}</p>
      <p style="margin:0;color:#6b7280">Login səhifəsinə yönləndirilirsiniz…</p>
    </div>`;
    document.body.appendChild(div);
  } catch {}
  setTimeout(()=>{ window.location.replace(LOGIN_APP_BASE); }, 1200);
}

// 401 (Unauthorized) düşən kimi avtomatik login səhifəsinə yönləndir
(function hardAuthRedirect(){
  const _fetch = window.fetch;
  window.fetch = async (...args) => {
    const r = await _fetch(...args);
    if (r.status === 401) {
      showAuthGateAndRedirect('Sessiya bitdi və ya token etibarsızdır.');
      // axınları dayandırmaq üçün error atırıq:
      throw new Error('Unauthorized');
    }
    return r;
  };
})();


async function authGuardOnce(){
  // Ticket ümumiyyətlə yoxdursa → birbaşa geri
  if (!PAGE_TICKET) {
    showAuthGateAndRedirect('Ticket tapılmadı.');
    return false;
  }
  try{
    const r = await fetch(`/api/ticket-status/?ticket=${encodeURIComponent(PAGE_TICKET)}`, {
      headers: { 'Accept':'application/json' }
    });
    if (!r.ok) {
      const txt = await r.text().catch(()=> '');
      showAuthGateAndRedirect(txt || 'Giriş tələb olunur.');
      return false;
    }
    const data = await r.json();
    // mövcud dəyişənlərin yenilənməsi (səndə artıq var)
    window.CURRENT_STATUS_ID = data?.status_id ?? null;
    window.EDIT_ALLOWED = !!data?.allow_edit;
    applyEditPermissions?.();
    updateTicketDeleteState?.();
    return true;
  }catch(e){
    console.warn('authGuard error:', e);
    showAuthGateAndRedirect('Şəbəkə xətası.');
    return false;
  }
}

// Səhifə açılan kimi yoxla:
authGuardOnce();

// Hər 30 saniyədən bir sessiyanı yoxla:
setInterval(() => { authGuardOnce(); }, 30000);




// === STATUS icazəsi (yalnız STATUS_ID 2 və 99 üçün) ===
window.EDIT_ALLOWED = false;
window.CURRENT_STATUS_ID = null;

async function fetchTicketStatus() {
  if (!PAGE_TICKET) return false;
  try {
    const resp = await fetch(`/api/ticket-status/?ticket=${encodeURIComponent(PAGE_TICKET)}`, {
      headers: { 'Accept':'application/json' }
    });
    if (!resp.ok) throw new Error(await resp.text());
    const data = await resp.json();
    window.CURRENT_STATUS_ID = data?.status_id ?? null;
    window.EDIT_ALLOWED = !!data?.allow_edit;
    applyEditPermissions();
    updateTicketDeleteState();
    return window.EDIT_ALLOWED;
  } catch (e) {
    console.warn('ticket-status error:', e);
    window.EDIT_ALLOWED = false;
    applyEditPermissions();
    return false;
  }
}

function applyEditPermissions(){
  const dataBtn = document.querySelector('.tool-btn[data-panel="contents"]');
  const editBtn = document.querySelector('.tool-btn[data-panel="catalog"]');
  [dataBtn, editBtn].forEach(btn=>{
    if (!btn) return;
    if (window.EDIT_ALLOWED){
      btn.classList.remove('locked');
      btn.removeAttribute('aria-disabled');
      btn.title = btn.dataset.title || btn.title || '';
    } else {
      btn.classList.add('locked');
      btn.setAttribute('aria-disabled','true');
      btn.title = 'Bu müraciətin statusuna görə (STATUS_ID ≠ 2/99) bu bölmə bağlıdır.';
    }
  });
}


/* =========================
   ƏLAVƏ BASEMAP LAYLARI
   ========================= */
const osmStd = new ol.layer.Tile({
  source: new ol.source.OSM({ attributions: '© OpenStreetMap contributors' }),
  visible: false
});
map.addLayer(osmStd);

const cartoDark = new ol.layer.Tile({
  source: new ol.source.XYZ({
    urls: [
      'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
      'https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
      'https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
      'https://d.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png'
    ],
    attributions: '© OpenStreetMap contributors, © CARTO',
    maxZoom: 20
  }),
  visible: false
});
map.addLayer(cartoDark);

const esriImagery = new ol.layer.Tile({
  source: new ol.source.XYZ({
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    attributions: 'Tiles © Esri'
  }),
  visible: false
});
map.addLayer(esriImagery);

const esriLabelsOverlay = new ol.layer.Tile({
  source: new ol.source.XYZ({
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}',
    attributions: '© Esri'
  }),
  visible: false,
  zIndex: 2
});
map.addLayer(esriLabelsOverlay);

const esriStreets = new ol.layer.Tile({
  source: new ol.source.XYZ({
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}',
    attributions: '© Esri'
  }),
  visible: false
});
map.addLayer(esriStreets);

const esriTopo = new ol.layer.Tile({
  source: new ol.source.XYZ({
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}',
    attributions: '© Esri'
  }),
  visible: false
});
map.addLayer(esriTopo);

const esriNavigation = new ol.layer.Tile({
  source: new ol.source.XYZ({
    url: 'https://services.arcgisonline.com/ArcGIS/rest/services/Specialty/World_Navigation_Charts/MapServer/tile/{z}/{y}/{x}',
    attributions: '© Esri, NOAA'
  }),
  visible: false
});
map.addLayer(esriNavigation);

// aktiv basemap
let currentBasemap = 'google';
function setBasemap(key){
  [googleImagery, osmStd, cartoDark, esriImagery, esriStreets, esriTopo, esriNavigation]
    .forEach(l => l.setVisible(false));
  esriLabelsOverlay.setVisible(false);

  switch(key){
    case 'google':         googleImagery.setVisible(true); break;
    case 'osm':            osmStd.setVisible(true); break;
    case 'streets':        esriStreets.setVisible(true); break;
    case 'streets_night':  cartoDark.setVisible(true); break;
    case 'imagery':        esriImagery.setVisible(true); break;
    case 'imagery_hybrid': esriImagery.setVisible(true); esriLabelsOverlay.setVisible(true); break;
    case 'topographic':    esriTopo.setVisible(true); break;
    case 'navigation':     esriNavigation.setVisible(true); break;
    default:               googleImagery.setVisible(true);
  }
  currentBasemap = key;
  highlightSelectedBasemap();
}
function highlightSelectedBasemap(){
  document.querySelectorAll('.basemap-item').forEach(it=>{
    if (it.dataset.key === currentBasemap){
      it.classList.add('selected');
      it.style.border = '2px solid #2563eb';
      it.style.boxShadow = '0 0 0 2px rgba(37,99,235,.35)';
      it.style.borderRadius = '5px';
    } else {
      it.classList.remove('selected');
      it.style.border = '2px solid transparent';
      it.style.boxShadow = 'none';
    }
  });
}

// === Feature ownership map (feature → source) ===
const FeatureOwner = new WeakMap();
function trackFeatureOwnership(source){
  if (!source) return;
  source.on('addfeature',  e => { try { FeatureOwner.set(e.feature, source); } catch {} });
  source.on('removefeature', e => { try { FeatureOwner.delete(e.feature); } catch {} });
}


/* =========================
   TEKUİS (M_G_PARSEL) LAY
   ========================= */
const tekuisSource = new ol.source.Vector();

trackFeatureOwnership(tekuisSource);


['addfeature','removefeature','changefeature'].forEach(ev => {
  tekuisSource.on(ev, () => {
    saveTekuisToLS();
    // Geometriya dəyişdi → həm OK, həm də əvvəlki ignore-ları sıfırla
    window._topoLastOk = null;
    window._lastTopoValidation = null;
    window._ignoredTopo = { overlaps: new Set(), gaps: new Set() };
  });
});



const tekuisLayer  = new ol.layer.Vector({
  source: tekuisSource,
  style: new ol.style.Style({
    fill:   new ol.style.Fill({ color: 'rgba(72, 163, 133, 0.15)' }),
    stroke: new ol.style.Stroke({ color: '#4d9bb8', width: 2 })
  }),
  zIndex: 4,
  visible: false
});
tekuisLayer.set('title', 'TEKUİS (M_G_PARSEL)');
tekuisLayer.set('isTekuisEditable', true);

tekuisLayer.set('selectIgnore', false);  // Seçim interaction-u bu layı GÖRSÜN
map.addLayer(tekuisLayer);
window.tekuisLayer = tekuisLayer;



/* =========================
   NECAS (NECASMAPUSER.PARCEL) LAY
   ========================= */
const necasSource = new ol.source.Vector();
trackFeatureOwnership(necasSource);
const necasLayer  = new ol.layer.Vector({
  source: necasSource,
  style: new ol.style.Style({
    fill:   new ol.style.Fill({ color: 'rgba(59,130,246,0.15)' }), // mavi ton
    stroke: new ol.style.Stroke({ color: '#3b82f6', width: 2 })
  }),
  zIndex: 4,
  visible: false 
});
necasLayer.set('title', 'NECAS (PARCEL)');
necasLayer.set('selectIgnore', true);
map.addLayer(necasLayer);
window.necasLayer = necasLayer;


// === Info-highlight overlay (sabit seçim görünüşü) ===
const infoHighlightSource = new ol.source.Vector();
const infoHighlightLayer  = new ol.layer.Vector({
  source: infoHighlightSource,
  zIndex: 99,
  style: (feature) => {
    const t = feature.getGeometry().getType();
    const sky = '#60a5fa'; // açıq mavi
    if (/Point/i.test(t)) {
      return new ol.style.Style({
        image: new ol.style.Circle({
          radius: 6,
          fill:   new ol.style.Fill({ color: 'rgba(96,165,250,0.25)' }),
          stroke: new ol.style.Stroke({ color: sky, width: 2 })
        })
      });
    }
    if (/LineString/i.test(t)) {
      return [
        new ol.style.Style({ stroke: new ol.style.Stroke({ color: 'rgba(96,165,250,0.35)', width: 8 }) }),
        new ol.style.Style({ stroke: new ol.style.Stroke({ color: sky, width: 3 }) })
      ];
    }
    // Polygon / MultiPolygon
    return [
      new ol.style.Style({ fill:   new ol.style.Fill({ color: 'rgba(96,165,250,0.10)' }) }),
      new ol.style.Style({ stroke: new ol.style.Stroke({ color: 'rgba(96,165,250,0.35)', width: 6 }) }),
      new ol.style.Style({ stroke: new ol.style.Stroke({ color: sky, width: 3 }) })
    ];
  }
});
// Overlay klikləri info axtarışına mane olmasın:
infoHighlightLayer.set('infoIgnore', true);
infoHighlightLayer.set('selectIgnore', true);
map.addLayer(infoHighlightLayer);

// === Topologiya xətası üçün QIRMIZI vurğulama layı ===
const topoErrorSource = new ol.source.Vector();
const topoErrorLayer  = new ol.layer.Vector({
  source: topoErrorSource,
  zIndex: 200,
  style: (feature) => {
    const t = feature.getGeometry().getType();
    const red = '#ef4444';
    if (/Point/i.test(t)) {
      return new ol.style.Style({
        image: new ol.style.Circle({
          radius: 6,
          fill:   new ol.style.Fill({ color: 'rgba(239,68,68,0.12)' }),
          stroke: new ol.style.Stroke({ color: red, width: 3 })
        })
      });
    }
    if (/LineString/i.test(t)) {
      return [
        new ol.style.Style({ stroke: new ol.style.Stroke({ color: 'rgba(239,68,68,0.35)', width: 8 }) }),
        new ol.style.Style({ stroke: new ol.style.Stroke({ color: red, width: 3 }) })
      ];
    }
    // Polygon / MultiPolygon
    return [
      new ol.style.Style({ fill:   new ol.style.Fill({ color: 'rgba(239,68,68,0.08)' }) }),
      new ol.style.Style({ stroke: new ol.style.Stroke({ color: 'rgba(239,68,68,0.35)', width: 6 }) }),
      new ol.style.Style({ stroke: new ol.style.Stroke({ color: red, width: 3 }) })
    ];
  }
});
topoErrorLayer.set('infoIgnore',   true);
topoErrorLayer.set('selectIgnore', true);
map.addLayer(topoErrorLayer);


// Xətaları xəritədə qırmızı layda göstərən helper
function renderTopoErrorsOnMap(validation){
  try{
    topoErrorSource.clear(true);
    if (!validation) return;
    const gj = new ol.format.GeoJSON();
    const add = (arr=[]) => arr.forEach(it=>{
      if (!it?.geom) return;
      const g = gj.readGeometry(it.geom, {
        dataProjection: 'EPSG:4326',
        featureProjection: 'EPSG:3857'
      });
      topoErrorSource.addFeature(new ol.Feature({ geometry: g }));
    });
    add(validation.overlaps);
    add(validation.gaps);
  }catch(e){ console.warn('renderTopoErrorsOnMap error:', e); }
}



// === Zoom edilən obyekt üçün FOKUS layı (xüsusi rəng) ===
const topoFocusSource = new ol.source.Vector();
const topoFocusLayer  = new ol.layer.Vector({
  source: topoFocusSource,
  zIndex: 201,
  style: (feature) => {
    const t = feature.getGeometry().getType();
    const fillCol = 'rgba(245,158,11,0.18)'; // yumşaq narıncı fill
    const glowCol = 'rgba(0,0,0,0.30)';      // kənarda yüngül “halo”
    const black   = '#111111';               // qara sərhəd

    if (/Point/i.test(t)) {
      return [
        new ol.style.Style({ // yumşaq halo
          image: new ol.style.Circle({
            radius: 9,
            fill:   new ol.style.Fill({ color: 'rgba(0,0,0,0.08)' }),
            stroke: new ol.style.Stroke({ color: glowCol, width: 8 })
          })
        }),
        new ol.style.Style({ // əsas marker
          image: new ol.style.Circle({
            radius: 7,
            fill:   new ol.style.Fill({ color: fillCol }),
            stroke: new ol.style.Stroke({ color: black, width: 3 })
          })
        })
      ];
    }

    if (/LineString/i.test(t)) {
      return [
        new ol.style.Style({ stroke: new ol.style.Stroke({ color: glowCol, width: 9 }) }), // halo
        new ol.style.Style({ stroke: new ol.style.Stroke({ color: black,   width: 3.5 }) }) // qara sərhəd
      ];
    }

    // Polygon / MultiPolygon
    return [
      new ol.style.Style({ fill:   new ol.style.Fill({ color: fillCol }) }),                  // fill
      new ol.style.Style({ stroke: new ol.style.Stroke({ color: glowCol, width: 7 }) }),      // halo
      new ol.style.Style({ stroke: new ol.style.Stroke({ color: black,   width: 3.5 }) })     // qara sərhəd
    ];
  }
});


topoFocusLayer.set('infoIgnore',   true);
topoFocusLayer.set('selectIgnore', true);
map.addLayer(topoFocusLayer);


function zoomAndHighlightTopoGeometry(geom4326){
  try{
    const gj = new ol.format.GeoJSON();
    const geom3857 = gj.readGeometry(geom4326, {
      dataProjection: 'EPSG:4326',
      featureProjection: 'EPSG:3857'
    });

    // ⬇️ QIRMIZI layı təmizləmə! Orada bütün xətalar qala bilər.
    // topoErrorSource.clear(true);  // <-- bunu SİLİN

    // ⬇️ Bunun əvəzinə yalnız fokus layını yenilə
    topoFocusSource.clear(true);
    const feat = new ol.Feature({ geometry: geom3857 });
    topoFocusSource.addFeature(feat);

    // Extent + fit
    let ext = geom3857.getExtent();
    const w = ol.extent.getWidth(ext);
    const h = ol.extent.getHeight(ext);

    let padM = 0;
    if (w === 0 && h === 0) padM = 30;
    else if (w < 15 && h < 15) padM = 30;
    else if (w < 60 && h < 60) padM = 20;
    else if (w < 200 && h < 200) padM = 10;
    if (padM > 0) ext = ol.extent.buffer(ext, padM);

    map.getView().fit(ext, {
      padding: [24, 24, 24, 24],
      duration: 650,
      maxZoom: 20,
      nearest: false,
      constrainOnlyCenter: false
    });

    // Fokus obyektə yumşaq “pulse” — rəngi də fokus rəngi ilə uyğunlaşdıraq
    try{
      if (typeof flashFeature === 'function') {
        flashFeature(feat, { duration: 950, hz: 3, baseColor: '#f59e0b' }); // narıncı
      }
    }catch{}
  }catch(e){
    console.warn('zoom/highlight error geom', e);
  }
}



// Topologiya xətası üçün qısa “pulse” effekti (vizual diqqət üçün)
function pulseTopoHighlight(feature, { duration = 950, hz = 3 } = {}){
  try{
    // Mövcud flashFeature-dən istifadə etmək istəsən:
    if (typeof flashFeature === 'function') {
      return flashFeature(feature, { duration, hz, baseColor: '#ef4444' });
    }
  }catch(e){ /* optional */ }
}




// Bir obyektin geometriyasını overlay-ə köçürüb “seçilmiş” göstər
function setInfoHighlight(feature) {
  infoHighlightSource.clear(true);
  if (!feature || !feature.getGeometry) return;
  const f = new ol.Feature({ geometry: feature.getGeometry().clone() }); // (EPSG:3857)
  infoHighlightSource.addFeature(f);
}



/* =========================
   PANEL/INDIKATOR
   ========================= */
const panelEl       = document.getElementById('side-panel');
const panelTitleEl  = panelEl.querySelector('.panel-title');
const panelBodyEl   = panelEl.querySelector('.panel-body');
const panelCloseBtn = document.getElementById('panel-close');
const indicatorEl   = document.getElementById('panel-indicator');
const workspaceEl   = document.querySelector('.workspace');

function openPanel(title, html){
  panelTitleEl.textContent = title || 'Panel';
  panelBodyEl.innerHTML = html || '';
  panelEl.hidden = false;
  void panelEl.offsetWidth;
  panelEl.classList.add('open');
  panelEl.setAttribute('aria-hidden', 'false');
}
function closePanel(){
  stopDraw(true);
  panelEl.classList.remove('open');
  panelEl.setAttribute('aria-hidden', 'true');
  const onEnd = (e) => {
    if (e.propertyName === 'transform') {
      panelEl.hidden = true;
      panelEl.removeEventListener('transitionend', onEnd);
    }
  };
  panelEl.addEventListener('transitionend', onEnd);
  indicatorEl.hidden = true;
  document.querySelectorAll('.tool-btn').forEach(b => b.classList.remove('active'));
}
panelCloseBtn.addEventListener('click', closePanel);

function moveIndicatorToButton(btn){
  const btnRect = btn.getBoundingClientRect();
  const wsRect  = workspaceEl.getBoundingClientRect();
  const top     = btnRect.top - wsRect.top;
  indicatorEl.style.top    = `${top}px`;
  indicatorEl.style.height = `${btnRect.height}px`;
  indicatorEl.hidden = false;
}

/* =========================
   Lay idarəsi (import olunan laylar üçün)
   ========================= */
function isFiniteExtent(ext){
  return Array.isArray(ext) && ext.length === 4 && ext.every(Number.isFinite);
}

// --- TEKUİS: uploaded layer-in BBOX-u ilə Oracle-dan kəsişən parselləri çək
function fetchTekuisByBboxForLayer(layer){
  if (!layer || !layer.getSource) return;
  const extent3857 = layer.getSource().getExtent?.();
  if (!isFiniteExtent(extent3857)) return;

  const [minx, miny, maxx, maxy] =
    ol.proj.transformExtent(extent3857, 'EPSG:3857', 'EPSG:4326');

  const url = `/api/tekuis/parcels/by-bbox/?minx=${minx}&miny=${miny}&maxx=${maxx}&maxy=${maxy}`; // ⬅ limit YOXDUR

  fetch(url, { headers: { 'Accept':'application/json' } })
    .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
    .then(showTekuis)
    .catch(err => console.error('TEKUİS BBOX error:', err));
}


function showTekuis(fc){
  try{
    const format = new ol.format.GeoJSON();
    const feats = format.readFeatures(fc, {
      dataProjection: 'EPSG:4326',
      featureProjection: 'EPSG:3857'
    });
    tekuisSource.clear(true);
    tekuisSource.addFeatures(feats);

    tekuisCount = feats.length;

    if (document.getElementById('cardTekuis')){
      const mode = (window.TekuisSwitch && typeof window.TekuisSwitch.getMode === 'function')
        ? window.TekuisSwitch.getMode()
        : 'live';

      const defaultText =
        (mode === 'db')
          ? (window.TEXT_TEKUIS_DB_DEFAULT || 'Tədqiqat nəticəsində dəyişiklik eilərək saxlanılan TEKUİS parselləri')
          : (window.TEXT_TEKUIS_DEFAULT    || 'TEKUİS sisteminin parsel məlumatları.');

      const suffix = (mode === 'db') ? ' (Mənbə: Local baza)' : ' (Mənbə: TEKUİS – canlı)';

      applyNoDataCardState('cardTekuis', tekuisCount === 0, TEXT_TEKUIS_EMPTY, defaultText + suffix);
    }


    const chk = document.getElementById('chkTekuisLayer');
    if (tekuisCount === 0){
      tekuisLayer.setVisible(false);
      if (chk) chk.checked = false;
    } else if (chk){
      tekuisLayer.setVisible(chk.checked);
    }
  }catch(e){
    console.error('TEKUİS parse error:', e);
  }

  saveTekuisToLS();

}



// === YENİ: TEKUİS-i qoşma fayllardan gətir
async function fetchTekuisByAttachTicket(){
  if (!PAGE_TICKET) return;
  try{
    const resp = await fetch(`/api/tekuis/parcels/by-attach-ticket/?ticket=${encodeURIComponent(PAGE_TICKET)}`, {
      headers: { 'Accept':'application/json' }
    });
    if (!resp.ok) throw new Error(await resp.text());
    const fc = await resp.json();
    showTekuis(fc);
  }catch(e){
    console.error('TEKUİS ATTACH error:', e);
  }
}


function tekuisHasCache(){
  try { return !!localStorage.getItem(LS_KEYS.tekuis); } catch { return false; }
}
function clearTekuisCache(){
  try { localStorage.removeItem(LS_KEYS.tekuis); } catch {}
}

// Səhifə bağlananda / refresh olanda kəsilmiş TEKUİS keşini sil
window.addEventListener('beforeunload', () => {
  clearTekuisCache();
});


/** TEKUİS-i Qoşma laydan fon üçün yenilə.
 *  force=true olduqda LS keşinə baxmadan yenidən çəkir.
 */
function refreshTekuisFromAttachIfAny(force=false){
  if (!force && tekuisHasCache()) {
    // Kəsilmiş / redaktə olunmuş TEKUİS LS-dədir – üstələməyək
    return Promise.resolve();
  }
  const n = attachLayerSource?.getFeatures()?.length || 0;
  if (n > 0){
    return attachLayer ? fetchTekuisByGeomForLayer(attachLayer) : Promise.resolve();
  } else {
    tekuisSource.clear(true);
    const lbl = document.getElementById('lblTekuisCount');
    if (lbl) lbl.textContent = '(0)';
    return Promise.resolve();
  }
}



function showNecas(fc){
  try{
    const format = new ol.format.GeoJSON();
    const feats = format.readFeatures(fc, {
      dataProjection: 'EPSG:4326',
      featureProjection: 'EPSG:3857'
    });
    necasSource.clear(true);
    necasSource.addFeatures(feats);

    necasCount = feats.length;

    if (document.getElementById('cardNecas')){
      applyNoDataCardState('cardNecas', necasCount === 0, TEXT_NECAS_EMPTY, TEXT_NECAS_DEFAULT);
    }

    const chk = document.getElementById('chkNecasLayer');
    if (necasCount === 0){
      necasLayer.setVisible(false);
      if (chk) chk.checked = false;
    } else if (chk){
      necasLayer.setVisible(chk.checked);
    }
  }catch(e){
    console.error('NECAS parse error:', e);
  }
}


function fetchNecasByBboxForLayer(layer){
  if (!layer || !layer.getSource) return;
  const extent3857 = layer.getSource().getExtent?.();
  if (!extent3857) return;
  const [minx,miny,maxx,maxy] = ol.proj.transformExtent(extent3857, 'EPSG:3857', 'EPSG:4326');
  const url = `/api/necas/parcels/by-bbox/?minx=${minx}&miny=${miny}&maxx=${maxx}&maxy=${maxy}`;
  fetch(url, { headers:{'Accept':'application/json'} })
    .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
    .then(showNecas)
    .catch(err => console.error('NECAS BBOX error:', err));
}

function fetchNecasByGeomForLayer(layer){
  const { wkt, bufferMeters } = composeLayerWKTAndSuggestBuffer(layer);
  if (!wkt) return fetchNecasByBboxForLayer(layer);

  return fetch('/api/necas/parcels/by-geom/', {
    method: 'POST',
    headers: { 'Content-Type':'application/json','Accept':'application/json' },
    body: JSON.stringify({ wkt, srid: 4326, buffer_m: bufferMeters })
  })
  .then(async r => {
    if (!r.ok) {
      const txt = await r.text();             // ⬅ xətanın mətnini götür
      throw new Error(`HTTP ${r.status} ${txt}`);
    }
    return r.json();
  })
  .then(fc => showNecas(fc))
  .catch(err => {
    console.error('NECAS GEOM error:', err);
    Swal.fire('NECAS xətası', (err && err.message) || 'Naməlum xəta', 'error');
  });
}


function refreshNecasFromAttachIfAny(){
  const n = attachLayerSource?.getFeatures()?.length || 0;
  if (n > 0){
    return attachLayer ? fetchNecasByGeomForLayer(attachLayer) : Promise.resolve();
  } else {
    necasSource.clear(true);
    return Promise.resolve();
  }
}








// --- Uploaded layer-dən WKT + avtomatik buffer seçimi
function composeLayerWKTAndSuggestBuffer(layer){
  if (!layer || !layer.getSource) return { wkt:null, bufferMeters: 0 };

  const feats = layer.getSource().getFeatures();
  if (!feats || feats.length === 0) return { wkt:null, bufferMeters: 0 };

  const polys = [], lines = [], points = [];
  feats.forEach(f=>{
    const g = f.getGeometry(); if (!g) return;
    const t = g.getType();
    if (t === 'Polygon'){
      polys.push(g.clone().transform('EPSG:3857','EPSG:4326').getCoordinates());
    } else if (t === 'MultiPolygon'){
      const gm = g.clone().transform('EPSG:3857','EPSG:4326').getCoordinates();
      gm.forEach(p=>polys.push(p));
    } else if (t === 'LineString'){
      lines.push(g.clone().transform('EPSG:3857','EPSG:4326').getCoordinates());
    } else if (t === 'MultiLineString'){
      const gl = g.clone().transform('EPSG:3857','EPSG:4326').getCoordinates();
      gl.forEach(l=>lines.push(l));
    } else if (t === 'Point'){
      points.push(g.clone().transform('EPSG:3857','EPSG:4326').getCoordinates());
    } else if (t === 'MultiPoint'){
      const gp = g.clone().transform('EPSG:3857','EPSG:4326').getCoordinates();
      gp.forEach(p=>points.push(p));
    }
  });

  const wktWriterLocal = new ol.format.WKT();

  // Prioritet: Polygon > Line > Point
  if (polys.length > 0){
    const mp = new ol.geom.MultiPolygon(polys);
    return { wkt: wktWriterLocal.writeGeometry(mp, { decimals: 8 }), bufferMeters: 5 }; // sərhəd “snap” üçün kiçik buffer
  }
  if (lines.length > 0){
    const ml = new ol.geom.MultiLineString(lines);
    return { wkt: wktWriterLocal.writeGeometry(ml, { decimals: 8 }), bufferMeters: 8 }; // xətlər üçün defolt 8 m
  }
  if (points.length > 0){
    const mp = new ol.geom.MultiPoint(points);
    return { wkt: wktWriterLocal.writeGeometry(mp, { decimals: 8 }), bufferMeters: 12 }; // nöqtələr üçün 12 m
  }

  return { wkt:null, bufferMeters: 0 };
}



// --- Uploaded layer-dən (yalnız Polygon/MultiPolygon) MultiPolygon WKT düzəlt
function composeLayerMultiPolygonWKT(layer){
  if (!layer || !layer.getSource) return null;
  const feats = layer.getSource().getFeatures();
  if (!feats || feats.length === 0) return null;

  const multiCoords = [];
  feats.forEach(f=>{
    const g = f.getGeometry();
    if (!g) return;
    const t = g.getType();
    if (t === 'Polygon'){
      const gp = g.clone().transform('EPSG:3857','EPSG:4326');
      multiCoords.push(gp.getCoordinates());
    } else if (t === 'MultiPolygon'){
      const gm = g.clone().transform('EPSG:3857','EPSG:4326');
      const parts = gm.getCoordinates();
      parts.forEach(c => multiCoords.push(c));
    } else {
      // Point/Line-ları indi nəzərə almırıq; ehtiyac olsa, backend buffer_m ilə tutarıq
    }
  });

  if (multiCoords.length === 0) return null;
  const mp = new ol.geom.MultiPolygon(multiCoords);
  const wktWriterLocal = new ol.format.WKT();
  return wktWriterLocal.writeGeometry(mp, { decimals: 8 });
}


function fetchTekuisByGeomForLayer(layer){
  const { wkt, bufferMeters } = composeLayerWKTAndSuggestBuffer(layer);
  if (!wkt){
    // Heç nə formalaşmadısa — son çarə BBOX
    return fetchTekuisByBboxForLayer(layer);
  }
  return fetch('/api/tekuis/parcels/by-geom/', {
    method: 'POST',
    headers: { 'Content-Type':'application/json', 'Accept':'application/json' },
    body: JSON.stringify({ wkt, srid: 4326, buffer_m: bufferMeters }) // ⬅ limit YOXDUR
  })
  .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
  .then(fc => showTekuis(fc))
  .catch(err => console.error('TEKUİS GEOM error:', err));
}








let uploadedLayer = null;
function addGeoJSONToMap(geojson){
  const format   = new ol.format.GeoJSON();
  const features = format.readFeatures(geojson, {
    dataProjection: 'EPSG:4326',
    featureProjection: 'EPSG:3857'
  });
  const source = new ol.source.Vector({ features });

  if (uploadedLayer) map.removeLayer(uploadedLayer);

  uploadedLayer = new ol.layer.Vector({
    source,
    style: styleByGeom
  });
  map.addLayer(uploadedLayer);

  registerSnapSource(source);

  const extent = source.getExtent();
  map.getView().fit(extent, { padding: [20,20,20,20], duration: 600, maxZoom: 18 });

  tekuisSource.clear(true);
  tekuisCount = 0;
  const lblT = document.getElementById('lblTekuisCount');
  if (lblT) lblT.textContent = '(0)';


}

function styleByGeom(feature){
  const t = feature.getGeometry().getType();
  if (t === 'Point' || t === 'MultiPoint') {
    return new ol.style.Style({
      image: new ol.style.Circle({
        radius: 5,
        fill: new ol.style.Fill({ color: 'rgba(37,99,235,0.9)' }),
        stroke: new ol.style.Stroke({ color: '#ffffff', width: 1.5 })
      })
    });
  }
  if (t === 'LineString' || t === 'MultiLineString') {
    return new ol.style.Style({
      stroke: new ol.style.Stroke({ color: '#2563eb', width: 2 })
    });
  }
  return new ol.style.Style({
    fill: new ol.style.Fill({ color: 'rgba(37,99,235,0.25)' }),
    stroke: new ol.style.Stroke({ color: '#2563eb', width: 2 })
  });
}



// -- Tədqiqat layı üçün default yaşıl stil
function styleTicketDefault(feature){
  const t = feature.getGeometry().getType();
  if (t === 'Point' || t === 'MultiPoint') {
    return new ol.style.Style({
      image: new ol.style.Circle({
        radius: 5,
        fill:  new ol.style.Fill({ color: 'rgba(16,185,129,0.9)' }), // #10b981
        stroke:new ol.style.Stroke({ color: '#ffffff', width: 1.5 })
      })
    });
  }
  if (t === 'LineString' || t === 'MultiLineString') {
    return new ol.style.Style({
      stroke: new ol.style.Stroke({ color: '#10b981', width: 2 })
    });
  }
  return new ol.style.Style({
    fill:   new ol.style.Fill({ color: 'rgba(16,185,129,0.25)' }),
    stroke: new ol.style.Stroke({ color: '#10b981', width: 2 })
  });
}

// -- Qoşma lay üçün default narıncı stil
function styleAttachDefault(feature){
  const t = feature.getGeometry().getType();
  if (t === 'Point' || t === 'MultiPoint') {
    return new ol.style.Style({
      image: new ol.style.Circle({
        radius: 5,
        fill:  new ol.style.Fill({ color: 'rgba(234,88,12,0.9)' }), // #ea580c
        stroke:new ol.style.Stroke({ color: '#ffffff', width: 1.5 })
      })
    });
  }
  if (t === 'LineString' || t === 'MultiLineString') {
    return new ol.style.Style({
      stroke: new ol.style.Stroke({ color: '#ea580c', width: 2 })
    });
  }
  return new ol.style.Style({
    fill:   new ol.style.Fill({ color: 'rgba(234,88,12,0.25)' }),
    stroke: new ol.style.Stroke({ color: '#ea580c', width: 2 })
  });
}


/* =========================
   Upload helpers (SHAPE & POINTS)
   ========================= */



const lastUploadState = (window.lastUploadState ??= {
  type: null,   // 'zip' | 'csvtxt'
  file: null,   // File obyekti
  crs:  null    // 'wgs84' | 'utm38' | 'utm39' (yalnız csv/txt üçün)
});


async function uploadArchiveToBackend(file){

  if (!window.EDIT_ALLOWED) {
    Swal.fire('Diqqət', 'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'warning');
    return;
  }

  try {
    const fd = new FormData();
    fd.append('file', file);

    fd.append('ticket', PAGE_TICKET || '');
    const resp = await fetch('/api/upload-shp/', { method: 'POST', body: fd });
    if (!resp.ok) {
      throw new Error((await resp.text()) || `HTTP ${resp.status}`);
    }
    const geojson = await resp.json();
    addGeoJSONToMap(geojson);
    lastUploadState.type = 'zip';
    lastUploadState.file = file;
    lastUploadState.crs  = null; // SHP üçün koordinat sistemi DB-yə yazılmır
    Swal.fire('OK', 'Shapefile xəritəyə əlavə olundu.', 'success');
    updateAllSaveButtons();
  } catch (err) {
    console.error(err);
    Swal.fire('Xəta', (err && err.message) || 'Yükləmə və ya çevirmə alınmadı.', 'error');
  }
}

async function uploadPointsToBackend(file, crs){
  if (!window.EDIT_ALLOWED) {
    Swal.fire('Diqqət', 'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'warning');
    return;
  }
  try {
    const fd = new FormData();
    fd.append('file', file);
    fd.append('crs', crs); // wgs84 | utm38 | utm39

    fd.append('ticket', PAGE_TICKET || '');
    const resp = await fetch('/api/upload-points/', { method: 'POST', body: fd });
    if (!resp.ok) {
      throw new Error((await resp.text()) || `HTTP ${resp.status}`);
    }
    const geojson = await resp.json();
    addGeoJSONToMap(geojson);
    lastUploadState.type = 'csvtxt';
    lastUploadState.file = file;
    lastUploadState.crs  = crs; // CSV/TXT üçün seçilən CRS saxlanır (attach zamanı DB-yə yazılacaq)
    Swal.fire('OK', 'Koordinatlar xəritəyə əlavə olundu.', 'success');
    updateAllSaveButtons();
  } catch (err) {
    console.error(err);
    Swal.fire('Xəta', (err && err.message) || 'Yükləmə və ya çevirmə alınmadı.', 'error');
  }
}

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
  const ownedSrc = FeatureOwner.get(f);
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
let infoMode = false;
let infoClickKey = null;

function setLeftButtonActiveForInfo(){
  const btn = document.querySelector('.tool-btn[data-panel="info"]');
  document.querySelectorAll('.tool-btn').forEach(b=>b.classList.remove('active'));
  if (btn){
    btn.classList.add('active');
    moveIndicatorToButton(btn);
  }
}



function renderInfoPanelLoading(){
  openPanel('Məlumatlar', `
    <div class="card">
      <div class="small">Məlumat yüklənir...</div>
    </div>
  `);
  setLeftButtonActiveForInfo();
}


const INFO_LABELS_SINGLE = {
  REQUEST_NUMBER: "Müraciət nümrəsi",
  ORG_ID: "Qurum",
  RE_TYPE_ID: "Əmlakın tipi",
  RE_CATEGORY_ID: "Əmlakın kateqoriyası",
  RE_ADDRESS: "Əmlakın ünvanı",
  RE_FACTUAL_USE: "Faktiki istifadə",
  ILLEGAL_BUILDS: "Qanununsuz tikililər",
  NOTES: "Qeyd",
  CONCLUSION: "Nəticə",
  OPINION: "Rəy",
};


const INFO_LABELS_PAIRS = [
  { left: "LAND_AREA_D",  right: "LAND_AREA_F",  label: "Torpaq sahəsi" },
  { left: "TOTAL_AREA_D", right: "TOTAL_AREA_F", label: "Ümumi sahə" },
  { left: "MAIN_AREA_D",  right: "MAIN_AREA_F",  label: "Əsas sahə" },
  { left: "AUX_AREA_D",   right: "AUX_AREA_F",   label: "Köməkçi sahə" },
  { left: "ROOM_COUNT_D", right: "ROOM_COUNT_F", label: "Otaq sayı" }
];

// Case-insensitive dəyər götürmək üçün helper
function recVal(rec, key){
  if (!rec) return null;
  const direct = rec[key];
  if (direct !== undefined) return direct;
  const k = Object.keys(rec).find(k => String(k).toUpperCase() === String(key).toUpperCase());
  return k ? rec[k] : null;
}


// TEKUİS sahələri (DB sütunu → göstərəcəyimiz etiket)
const TEKUIS_LABELS = {
  LAND_CATEGORY2ENUM: "Uqodiya",
  LAND_CATEGORY4ENUM: "Alt uqodiya",
  LAND_CATEGORY_ENUM: "Kateqoriya",
  LAND_CATEGORY3ENUM: "Alt kateqoriya",
  NAME:               "Qeyd",
  OWNER_TYPE_ENUM:    "Mülkiyyət",
  SUVARILMA_NOVU_ENUM:"Suvarma",
  EMLAK_NOVU_ENUM:    "Emlak növü",
  OLD_LAND_CATEGORY2ENUM: "İslahat uqodiyası",
  TERRITORY_NAME:     "Ünvan",
  RAYON_ADI:          "Rayonun adı",
  IED_ADI:            "İƏD adı",
  BELEDIYE_ADI:       "Bələdiyyə adı",
  AREA_HA:            "Sahə (hektarla)"
};

function renderTekuisInfo(props){
  if (!props) {
    openPanel('Məlumatlar', `<div class="card"><div class="small">Məlumat tapılmadı.</div></div>`);
    setLeftButtonActiveForInfo();
    return;
  }
  // geometry property-ni göstərmə
  const rec = Object.assign({}, props);
  delete rec.geometry;

  const rows = Object.entries(TEKUIS_LABELS).map(([col, label]) => {
    const v = recVal(rec, col);
    if (v === null || v === undefined || v === '') return '';
    return `<div class="k">${label}</div><div class="v">${String(v)}</div>`;
  }).join('');

  openPanel('Məlumatlar', `
    <div class="card">
      <div class="kv">
        <div class="h">TEKUİS parsel məlumatları</div>
        <div class="sep"></div>
        ${rows || '<div class="small">Bu parsel üçün göstəriləcək atribut yoxdur.</div>'}
      </div>
    </div>
  `);
  setLeftButtonActiveForInfo();
}




const NECAS_LABELS = {
  CADASTER_NUMBER: "Kadastr nömrəsi",
  KATEQORIYA:      "Kateqoriya",
  UQODIYA:         "Uqodiyası"
};

function renderNecasInfo(props){
  if (!props){
    openPanel('Məlumatlar', `<div class="card"><div class="small">Məlumat tapılmadı.</div></div>`);
    setLeftButtonActiveForInfo();
    return;
  }
  const rec = Object.assign({}, props);
  delete rec.geometry;
  const rows = Object.entries(NECAS_LABELS).map(([col,label])=>{
    const v = recVal(rec, col);
    if (v===null || v===undefined || v==='') return '';
    return `<div class="k">${label}</div><div class="v">${String(v)}</div>`;
  }).join('');
  openPanel('Məlumatlar', `
    <div class="card">
      <div class="kv">
        <div class="h">NECAS parsel məlumatları</div>
        <div class="sep"></div>
        ${rows || '<div class="small">Bu parsel üçün atribut yoxdur.</div>'}
      </div>
    </div>
  `);
  setLeftButtonActiveForInfo();
}






function renderInfoPanel(record, fk){

  if (!record || Object.keys(record).length === 0){
    openPanel('Məlumatlar', `
      <div class="card"><div class="small">Məlumat tapılmadı.</div></div>
    `);
    setLeftButtonActiveForInfo();
    return;
  }


  const singleRows = Object.entries(INFO_LABELS_SINGLE).map(([k,label])=>{
    const v = recVal(record, k);
    if (v === null || v === undefined || v === '') return '';
    return `
      <div class="k">${label}</div>
      <div class="v">${String(v)}</div>
    `;
  }).join('');

  // Cüt sahələr (sol-sağ)
  const pairRows = INFO_LABELS_PAIRS.map(p=>{
    const vL = recVal(record, p.left);
    const vR = recVal(record, p.right);
    if ((vL===null || vL===undefined || vL==='') &&
        (vR===null || vR===undefined || vR==='')) return '';
    return `
      <div class="pair">
        <div class="pair-label">${p.label}</div>
        <div class="pair-val">${vL===null||vL===undefined?'—':String(vL)}</div>
        <div class="pair-val">${vR===null||vR===undefined?'—':String(vR)}</div>
      </div>
    `;
  }).join('');

  openPanel('Məlumatlar', `
    <div class="card">
      <div class="small" style="margin-bottom:8px;"></div>

      <div class="kv">
        <div class="h">Torpaq məlumatları</div>
        <div class="sep"></div>
        ${singleRows}
      </div>

      <div class="kv kv-pairs" style="margin-top:10px;">
        <div class="pairs-header">
          <div class="ph-label">Texniki göstəricilər</div>
          <div class="ph-col">Sənəd</div>
          <div class="ph-col">Faktiki</div>
        </div>
        <div class="sep"></div>
        <div class="pairs-wrap">
          ${pairRows}
        </div>
      </div>
    </div>
  `);

  setLeftButtonActiveForInfo();
}

function getFkFromFeature(feature){
  const props = feature.getProperties() || {};
  const keys = Object.keys(props);
  const wanted = ['fk_metadata','fkmeta','metadata_id','rowid','fk','request_id'];
  for (const k of keys){
    const kk = k.toString().toLowerCase();
    if (wanted.includes(kk)) return props[k];
  }
  for (const k of keys){
    if (/^FK_?METADATA$/i.test(k)) return props[k];
    if (/^ROW_?ID$/i.test(k)) return props[k];
  }
  return null;
}
async function fetchFeatureInfoByFk(fk){
  const fki = parseInt(String(fk), 10);
  if (!Number.isFinite(fki)) {
    Swal.fire('Diqqət', 'Bu obyekt üçün etibarlı fk_metadata tapılmadı.', 'warning');
    return;
  }
  try {
    renderInfoPanelLoading();
    const resp = await fetch(`/api/info/by-fk/${encodeURIComponent(fki)}/`, {
      headers: { 'Accept':'application/json' }
    });
    if (resp.status === 404){
      renderInfoPanel(null, fki);
      return;
    }
    if (!resp.ok){
      throw new Error(await resp.text() || `HTTP ${resp.status}`);
    }
    const data = await resp.json();
    if (data && data.ok && data.data){
      renderInfoPanel(data.data, fki);
    } else {
      renderInfoPanel(null, fki);
    }
  } catch(err){
    console.error(err);
    Swal.fire('Xəta', (err && err.message) || 'Məlumatı almaq olmadı.', 'error');
  }
}
function onMapClickForInfo(evt){
  let hitFeature = null, hitLayer = null;
  map.forEachFeatureAtPixel(evt.pixel, (feat, layer) => {
    if (layer instanceof ol.layer.Vector && !layer.get('infoIgnore')) {
      hitFeature = feat;
      hitLayer = layer;
      return true;
    }
    return false;
  }, { hitTolerance: 5 });

  if (!hitFeature){
    Swal.fire('Diqqət', 'Obyekt tapılmadı. Zəhmət olmasa vektor obyektinin üzərinə klik edin.', 'info');
    return;
  }

// 1s flash + sabit seçim
  if (window._currentFlashCleanup) { try { window._currentFlashCleanup(); } catch {} }
  window._currentFlashCleanup = flashFeature(hitFeature, { duration: 1000, hz: 2.5, baseColor: '#60a5fa' });
  setInfoHighlight(hitFeature);



  // TEKUİS parselləri üçün fk_metadata tələb etmədən properties-dən göstər
  if (hitLayer === tekuisLayer){
    const props = hitFeature.getProperties() || {};
    renderTekuisInfo(props);
    return;
  }


  // onMapClickForInfo(evt) içində:
if (hitLayer === necasLayer){
  const props = hitFeature.getProperties() || {};
  renderNecasInfo(props);
  return;
}


  // Əvvəlki: fk_metadata ilə MSSQL məlumatlarını gətir
  const fk = getFkFromFeature(hitFeature);
  if (!fk && fk !== 0){
    Swal.fire('Diqqət', 'Bu obyekt üçün fk_metadata tapılmadı.', 'warning');
    return;
  }
  fetchFeatureInfoByFk(fk);
}

function enableInfoMode(){
  if (infoMode) return;
  infoMode = true;
  document.getElementById('rtInfo')?.classList.add('active');

  try { stopDraw(true); } catch {}
  try { selectAny.getFeatures().clear(); } catch {}
  try { selectInteraction.getFeatures().clear(); } catch {}

  pauseEditingInteractions();
  infoClickKey = map.on('singleclick', onMapClickForInfo);
  renderInfoPanelLoading();
}
function disableInfoMode(){
  if (!infoMode) return;
  infoMode = false;
  document.getElementById('rtInfo')?.classList.remove('active');
  if (infoClickKey) { ol.Observable.unByKey(infoClickKey); infoClickKey = null; }


  infoHighlightSource?.clear(true);
  if (window._currentFlashCleanup) { try { window._currentFlashCleanup(); } catch {} window._currentFlashCleanup = null; }


  resumeEditingInteractions();
}


function toggleInfoMode(){
  infoMode ? disableInfoMode() : enableInfoMode();
}




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

function renderDataPanel(){
  if (!window.EDIT_ALLOWED) {
    Swal.fire('Diqqət', 'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'info');
    return;
  }
  const html = `
    <div class="tabs">
      <div class="tab active" data-tab="shp">Shapefile (.zip)</div>
      <div class="tab" data-tab="pts">Koordinatlar (.csv/.txt)</div>
    </div>
    <div id="tabContent"></div>
  `;
  openPanel('Məlumat daxil et', html);
  const tabContent = document.getElementById('tabContent');
  const tabs = panelBodyEl.querySelectorAll('.tab');

  const loadTab = (which)=>{
    tabs.forEach(t => t.classList.toggle('active', t.dataset.tab === which));
    if (which === 'shp') {
      tabContent.innerHTML = `
        <div class="card">
          <div class="upload-box" id="uploadBoxShp">
            <div class="upload-title">Shapefile arxivi (.zip)</div>
            <div class="hint">Arxivdə .shp, .shx, .dbf (və varsa .prj) olmalıdır</div>
            <input type="file" id="shpArchiveInput" accept=".zip,.rar" hidden />
            <button id="chooseArchiveBtn" class="btn">Arxiv seç və yüklə</button>
            <div class="filename" id="archiveName"></div>
          </div>
        </div>
      `;
      const input   = document.getElementById('shpArchiveInput');
      const choose  = document.getElementById('chooseArchiveBtn');
      const box     = document.getElementById('uploadBoxShp');
      const nameLbl = document.getElementById('archiveName');
      const pick = (file)=>{
        if (!file) return;
        const low = file.name.toLowerCase();
        if (!(low.endsWith('.zip') || low.endsWith('.rar'))) {
          Swal.fire('Xəta', 'Zəhmət olmasa .zip və ya .rar shapefile arxivi seçin.', 'error');
          return;
        }
        nameLbl.textContent = file.name;
        uploadArchiveToBackend(file);
      };
      choose.addEventListener('click', () => input.click());
      input.addEventListener('change', e => pick(e.target.files?.[0]));
      box.addEventListener('dragover', e => { e.preventDefault(); box.classList.add('drag'); });
      box.addEventListener('dragleave', () => box.classList.remove('drag'));
      box.addEventListener('drop', e => { e.preventDefault(); box.classList.remove('drag'); pick(e.dataTransfer.files?.[0]); });
    } else {
      tabContent.innerHTML = `
        <div class="card">
          <div class="upload-title">Koordinatlar (.csv / .txt)</div>
          <div class="small">CSV üçün ayırıcı avtomatik tanınır (<code>,</code> <code>;</code> <code>\\t</code> və s.). Başlıq yoxdursa ilk iki sütun X,Y kimi qəbul ediləcək.</div>
          <div class="form-row">
            <div class="radio-group" id="crsRadios">
              <label class="radio"><input type="radio" name="crs" value="wgs84" checked> WGS84 (lon/lat)</label>
              <label class="radio"><input type="radio" name="crs" value="utm38"> UTM 38N</label>
              <label class="radio"><input type="radio" name="crs" value="utm39"> UTM 39N</label>
            </div>
          </div>
          <div class="upload-box" id="uploadBoxCsv" style="margin-top:10px;">
            <input type="file" id="pointsFileInput" accept=".csv,.txt" hidden />
            <button id="choosePointsBtn" class="btn">Fayl seç və yüklə</button>
            <div class="filename" id="pointsFileName"></div>
          </div>
        </div>
      `;
      const input   = document.getElementById('pointsFileInput');
      const choose  = document.getElementById('choosePointsBtn');
      const box     = document.getElementById('uploadBoxCsv');
      const nameLbl = document.getElementById('pointsFileName');
      const pick = (file)=>{
        if (!file) return;
        const low = file.name.toLowerCase();
        if (!(low.endsWith('.csv') || low.endsWith('.txt'))) {
          Swal.fire('Xəta', 'Zəhmət olmasa .csv və ya .txt faylı seçin.', 'error');
          return;
        }
        nameLbl.textContent = file.name;
        const crs = (document.querySelector('input[name="crs"]:checked')?.value) || 'wgs84';
        uploadPointsToBackend(file, crs);
      };
      choose.addEventListener('click', () => input.click());
      input.addEventListener('change', e => pick(e.target.files?.[0]));
      box.addEventListener('dragover', e => { e.preventDefault(); box.classList.add('drag'); });
      box.addEventListener('dragleave', () => box.classList.remove('drag'));
      box.addEventListener('drop', e => { e.preventDefault(); box.classList.remove('drag'); pick(e.dataTransfer.files?.[0]); });
    }
  };
  tabs.forEach(t => t.addEventListener('click', ()=> loadTab(t.dataset.tab)));
  loadTab('shp');

  const btnSave = document.getElementById('btnSaveDataPanel');
  // Data panelində "Yadda saxla" → poliqon + (əgər fayl seçilibsə) attach
  btnSave.addEventListener('click', () => saveSelected({ alsoAttach:true }));
  updateAllSaveButtons();
}

/* =========================
   Basemaps paneli
   ========================= */
function renderBasemapsPanel(){
  const thumbs = [
    { key:'google',          title:'Imagery',          img:'https://mt1.google.com/vt/lyrs=s&x=18&y=12&z=5' },
    { key:'imagery_hybrid',  title:'Imagery Hybrid',   img:'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/5/12/18' },
    { key:'streets',         title:'Streets',          img:'https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/5/12/18' },
    { key:'osm',             title:'OSM',              img:'https://tile.openstreetmap.org/5/17/11.png' },
    { key:'streets_night',   title:'Streets (Night)',  img:'https://a.basemaps.cartocdn.com/dark_all/5/17/11.png' },
    { key:'topographic',     title:'Topographic',      img:'https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/5/12/18' },
    { key:'navigation',      title:'Navigation',       img:'https://services.arcgisonline.com/ArcGIS/rest/services/Specialty/World_Navigation_Charts/MapServer/tile/5/12/18' }
  ];
  const html = `
    <div class="card">
      <div class="upload-title" style="margin-bottom:10px;">Basemaps</div>
      <div class="basemap-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px;padding:8px;">
        ${thumbs.map(t => `
          <div class="basemap-item" data-key="${t.key}"
               style="border:2px solid transparent;border-radius:5px;overflow:hidden;cursor:pointer;background:#f3f4f6;">
            <img src="${t.img}" alt="${t.title}" style="width:100%;height:110px;object-fit:cover;display:block;" />
            <div class="bm-title" style="padding:8px 10px;font-size:13px;color:#111827;">${t.title}</div>
          </div>
        `).join('')}
      </div>
    </div>
  `;
  openPanel('Basemaps', html);
  panelBodyEl.querySelectorAll('.basemap-item').forEach(el=>{
    el.addEventListener('click', ()=> setBasemap(el.dataset.key));
  });
  highlightSelectedBasemap();
}





/* =========================
   LS: görünmə və TEKUİS keş
   ========================= */
const LS_KEYS = {
  vis: 'map_layer_visibility_v1',
  tekuis: (PAGE_TICKET ? `tekuis_fc_${PAGE_TICKET}` : 'tekuis_fc_global')
};

function readVis() {
  try { return JSON.parse(localStorage.getItem(LS_KEYS.vis)) || {}; }
  catch { return {}; }
}
function writeVis(v) {
  try { localStorage.setItem(LS_KEYS.vis, JSON.stringify(v || {})); } catch{}
}
function setVisFlag(key, val){
  const v = readVis(); v[key] = !!val; writeVis(v);
}
function getVisFlag(key, fallback){
  const v = readVis(); return (key in v) ? !!v[key] : !!fallback;
}

/* --- TEKUİS-i LS-ə yaz/oxu --- */
const _geojsonFmt = new ol.format.GeoJSON();
function saveTekuisToLS(){
  try{
    const feats = tekuisSource.getFeatures() || [];
    const fcObj = _geojsonFmt.writeFeaturesObject(feats, {
      dataProjection: 'EPSG:4326',
      featureProjection: 'EPSG:3857'
    });
    localStorage.setItem(LS_KEYS.tekuis, JSON.stringify(fcObj));
  }catch{}
}
function loadTekuisFromLS(){
  try{
    const raw = localStorage.getItem(LS_KEYS.tekuis);
    if (!raw) return false;
    const fcObj = JSON.parse(raw);
    const feats = _geojsonFmt.readFeatures(fcObj, {
      dataProjection: 'EPSG:4326',
      featureProjection: 'EPSG:3857'
    });
    // TEKUİS mənbəyi yenilənmədən əvvəl köhnə seçilmiş TEKUİS feature-lərini təmizlə
    try {
      const selA = selectAny.getFeatures();
      const arr  = selA.getArray().slice();
      arr.forEach(f => {
        if (FeatureOwner.get(f) === tekuisSource) { // bu feature əvvəl TEKUİS-dən idi
          selA.remove(f);
        }
      });
    } catch {}

    tekuisSource.clear(true);
    tekuisSource.addFeatures(feats);
    tekuisCount = feats.length;
    const lbl = document.getElementById('lblTekuisCount');
    if (lbl) lbl.textContent = `(${tekuisCount})`;
    return tekuisCount > 0;
  }catch{ return false; }
}


// === Topologiya Modalı + TEKUİS: validate → (modal) → save =================

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
  try { topoErrorSource.clear(true); } catch {}

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


// --- Serverdə yadda saxla ---------------------------------------------------
async function saveTekuisOnServer(featureCollection, { ignored, skipValidation } = {}) {
  // Lokal header-lar
  const headers = { 'Content-Type': 'application/json', 'Accept': 'application/json' };

  // Ticket və meta_id-ni mövcud qlobalardan götür
  const ticket = (typeof PAGE_TICKET !== 'undefined' && PAGE_TICKET) ? PAGE_TICKET : '';
  const metaRaw = (typeof window.META_ID !== 'undefined') ? window.META_ID : null;
  const metaInt = metaRaw != null && String(metaRaw).trim() !== '' ? parseInt(metaRaw, 10) : null;

  const body = { geojson: featureCollection, ticket };
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
  try { saveTekuisToLS(); } catch {}

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
      skipValidation: shouldSkipValidation
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
    
    // State təmizlə
    window._ignoredTopo = { overlaps: new Set(), gaps: new Set() };
    window._topoLastOk = null;
    window._lastTopoValidation = null;
    
    Swal.fire('Uğurlu', `${s.data?.saved_count ?? feats.length} TEKUİS parseli bazaya yazıldı.`, 'success');
    
  } catch(e) {
    console.error('Save error:', e);
    Swal.fire('Xəta', e.message || 'Şəbəkə xətası baş verdi.', 'error');
  }
}



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



function setCardDisabled(cardId, disabled){
  const card = document.getElementById(cardId);
  if (!card) return;
  card.style.opacity = disabled ? '0.55' : '1';
  card.style.filter  = disabled ? 'grayscale(10%)' : 'none';
  card.querySelectorAll('input, button').forEach(el => { el.disabled = !!disabled; });
  const chk = card.querySelector('input[type="checkbox"]');
  if (chk && disabled) chk.checked = false;
}



function updateTicketDeleteState(){
  const delBtn = document.getElementById('btnDeleteTicket');
  if (!delBtn) return;

  const showByStatus = (window.CURRENT_STATUS_ID === 2 || window.CURRENT_STATUS_ID === 99);
  // Yalnız STATUS_ID 2 və 99 olduqda göstər
  delBtn.style.display = showByStatus ? '' : 'none';
  if (!showByStatus) return;

  // Görünəndə də icazəyə görə aktiv/deaktiv et
  const allowed = (window.CURRENT_STATUS_ID === 2 || window.CURRENT_STATUS_ID === 99);
  delBtn.disabled = !allowed;
  delBtn.title = allowed ? '' : 'Bu əməliyyat yalnız STATUS 2 və ya 99 üçün mümkündür.';

}






// --- TEKUİS / NECAS kartlarının "no data" vizualı ---
const TEXT_TEKUIS_DEFAULT = 'TEKUİS sisteminin parsel məlumatları.';
const TEXT_NECAS_DEFAULT  = 'NECAS sistemində qeydiyyatdan keçmiş parsellər.';
const TEXT_TEKUIS_EMPTY   = 'TEKUİS məlumat bazasında heç bir məlumat tapılmadı.';
const TEXT_NECAS_EMPTY    = 'NECAS məlumat bazasında heç bir məlumat tapılmadı.';

function applyNoDataCardState(cardId, isEmpty, emptyText, defaultText){
  const card = document.getElementById(cardId);
  if (!card) return;
  const small = card.querySelector('.small');

  // setCardDisabled-dən gələn solğunluğu ləğv edirik
  card.style.opacity = '1';
  card.style.filter  = 'none';

  if (isEmpty){
    // yumşaq qırmızı fon + nazik sərhəd
    card.style.background = 'rgba(239,68,68,0.06)';           // red-500 ~ 6% opacity
    card.style.border     = '1px solid rgba(239,68,68,0.25)'; // nazik qırmızı sərhəd
    card.style.boxShadow  = 'inset 0 0 0 1px rgba(239,68,68,0.10)';

    if (small) small.textContent = emptyText || '';

    // idarələri söndür və checkbox-ı söndürülmüş vəziyyətə gətir
    card.querySelectorAll('input, button').forEach(el=>{
      if (el.type === 'checkbox') el.checked = false;
      el.disabled = true;
    });
  } else {
    // normal hala qaytar
    card.style.background = '';
    card.style.border     = '';
    card.style.boxShadow  = '';
    if (small && defaultText) small.textContent = defaultText;

    card.querySelectorAll('input, button').forEach(el=>{ el.disabled = false; });
  }
}




function renderLayersPanel(){
  const canDelete = (window.CURRENT_STATUS_ID === 2 || window.CURRENT_STATUS_ID === 99);


  const html = `
  <div class="card layer-card" id="cardTicket">
    <div class="card-head">
      <div class="layer-left">
        <div class="checkbox-wrapper-18">
          <div class="round">
            <input type="checkbox" id="chkTicketLayer" checked />
            <label for="chkTicketLayer"></label>
          </div>
        </div>
        <label class="layer-title" for="chkTicketLayer">
          <span>Tədqiqat layı</span>
        </label>
      </div>
      <div class="card-actions">
        <button id="btnDeleteTicket" class="icon-btn ico-delete" title="Ləğv et" disabled></button>
        <button id="btnZoomTicket"   class="icon-btn ico-zoom zoombtn"   title="Laya yaxınlaşdır"></button>
      </div>
    </div>
    <div class="small" style="color:#6b7280;">Bu lay istifadəçi tərəfidən yadda saxlanılan <b>torpaq sahələrini</b> göstərir.</div>
  </div>

  <div style="height:10px;"></div>

  <div class="card layer-card" id="cardAttach">
    <div class="card-head">
      <div class="layer-left">
        <div class="checkbox-wrapper-18">
          <div class="round">
            <input type="checkbox" id="chkAttachLayer"/>
            <label for="chkAttachLayer"></label>
          </div>
        </div>
        <label class="layer-title" for="chkAttachLayer">
          <span>Qoşma lay</span>
        </label>
      </div>

      <div class="card-actions">
        <button id="btnZoomAttach" class="icon-btn ico-zoom zoombtn" title="Laya yaxınlaşdır"></button>
      </div>
    </div>
    <div class="small" style="color:#6b7280;">Bu lay istifadəçi tərəfidən əlavə edilən qoşa məlumatlarını göstərir</div>
  </div>

  <div style="height:10px;"></div>

  <div class="card layer-card" id="cardTekuis">
    <div class="card-head">
      <div class="layer-left">
        <div class="checkbox-wrapper-18">
          <div class="round">
            <input type="checkbox" id="chkTekuisLayer" />
            <label for="chkTekuisLayer"></label>
          </div>
        </div>
        <label class="layer-title" for="chkTekuisLayer">
          <span>TEKUİS Parsellər</span>
        </label>
      </div>

      <div class="card-actions">
        <button id="btnSaveTekuis"               class="icon-btn ico-save" title="TEKUİS parsellərini yadda saxla"></button>
        <button id="btnEraseTekuisInsideTicket"  class="icon-btn ico-erase" title="Tədqiqat daxilini kəs & sil"></button>
        <button id="btnZoomTekuis"               class="icon-btn ico-zoom zoombtn"  title="Laya yaxınlaşdır"></button>
      </div>
    </div>
    <div class="small" style="color:#6b7280;">TEKUİS sisteminin parsel məlumatları.</div>
  </div>

  <div style="height:10px;"></div>

  <div class="card layer-card" id="cardNecas">
    <div class="card-head">
      <div class="layer-left">
        <div class="checkbox-wrapper-18">
          <div class="round">
            <input type="checkbox" id="chkNecasLayer" />
            <label for="chkNecasLayer"></label>
          </div>
        </div>
        <label class="layer-title" for="chkNecasLayer">
          <span>NECAS Parsellər</span>
        </label>
      </div>

      <div class="card-actions">
        <button id="btnZoomNecas" class="icon-btn ico-zoom zoombtn" title="Laya yaxınlaşdır"></button>
      </div>
    </div>
    <div class="small" style="color:#6b7280;">NECAS sistemində qeydiyyatdan keçmiş parsellər.</div>
  </div>
`;

  openPanel('Laylar', html);

  setCardDisabled('cardTicket', ticketLayerCount === 0);
  setCardDisabled('cardAttach', attachLayerCount === 0);


  updateTicketDeleteState();
  



  


  // === Ticket (LS ilə) ===
  const chkTicket = document.getElementById('chkTicketLayer');
  const btnZoomTicket = document.getElementById('btnZoomTicket');

  // === TEKUİS: "Yadda saxla" → əvvəl topologiya, sonra save
  const btnSaveTekuis = document.getElementById('btnSaveTekuis');
  if (btnSaveTekuis) {
    btnSaveTekuis.addEventListener('click', async () => {
      // ✅ Attributes panel sinxi
      try {
        if (window.AttributesPanel && typeof window.AttributesPanel.applyUIToSelectedFeature === 'function') {
          window.AttributesPanel.applyUIToSelectedFeature();
        }
      } catch (e) {
        console.warn('Attributes sync:', e);
      }
      
      await tryValidateAndSaveTekuis();
    });
  }


  (async () => {
    const alreadyLoaded = !!ticketLayerSource;
    if (!alreadyLoaded) await loadTicketLayer({ fit:false });
    // init from LS
    const visTicket = getVisFlag('ticket', true);
    chkTicket.checked = visTicket;
    if (ticketLayer) ticketLayer.setVisible(chkTicket.checked);
    updateTicketDeleteState();
  })();

  chkTicket.addEventListener('change', () => {
    if (ticketLayer) ticketLayer.setVisible(chkTicket.checked);
    setVisFlag('ticket', chkTicket.checked);
    if (chkTicket.checked) flashLayer(ticketLayer);
  });


  btnZoomTicket.addEventListener('click', () => {
    if (ticketLayerSource && ticketLayerSource.getFeatures().length > 0){
      const ext = ticketLayerSource.getExtent();
      map.getView().fit(ext, { padding:[20,20,20,20], duration:600, maxZoom:18 });
    } else {
      Swal.fire('Info','Zoom ediləcək obyekt yoxdur.','info');
    }
  });

  if (canDelete) {
    document.getElementById('btnDeleteTicket').addEventListener('click', async () => {
      const allowed = (window.CURRENT_STATUS_ID === 2 || window.CURRENT_STATUS_ID === 99);

      const hasTicketData = (ticketLayerCount > 0);

      if (!hasTicketData) {
        Swal.fire('Info','Tədqiqat layında ləğv ediləcək obyekt yoxdur.','info');
        return;
      }
      if (!allowed) {
        Swal.fire('Diqqət','Bu əməliyyat yalnız 99 statusu və ya redaktə (qaralama) rejimində mümkündür.','info');
        return;
      }

      const ask = await Swal.fire({
        title: 'Əminsiniz?',
        html: 'Bütün məlumatlar ləğv ediləcək.',
        icon: 'warning',
        showCancelButton: true,
        confirmButtonText: 'Ləğv et',
        cancelButtonText: 'İmtina et'
      });
      if (!ask.isConfirmed) return;

      try {
        const resp = await fetch(`/api/layers/soft-delete-by-ticket/?ticket=${encodeURIComponent(PAGE_TICKET)}`, {
          method: 'POST',
          headers: {
            'Accept': 'application/json',
            'X-CSRFToken': getCSRFToken()
          },
          credentials: 'include'
        });

        if (!resp.ok) throw new Error((await resp.text()) || `HTTP ${resp.status}`);
        const data = await resp.json();

        // Layları yenilə
        await loadTicketLayer({ fit:false });
        await loadAttachLayer({ fit:false });

        // UI/state təmizləmələri
        updateTicketDeleteState();
        clearTekuisCache();
        await refreshTekuisFromAttachIfAny(true); // attach boşdursa TEKUİS də təmizlənəcək
        necasSource.clear(true); necasCount = 0;

        // Nəticəni göstər (serverdən gələn saylara uyğun)
        const info = [
          (data?.meta_id != null ? `meta_id: <b>${data.meta_id}</b>` : ''),
          (data?.affected_parcel != null ? `TEKUİS (parcel): <b>${data.affected_parcel}</b>` : ''),
          (data?.affected_gis != null ? `GIS data: <b>${data.affected_gis}</b>` : ''),
          (data?.affected_attach != null ? `Attach: <b>${data.affected_attach}</b>` : ''),
          (data?.objectid_nullified ? `TBL_REQUEST_REG.OBJECTID <b>NULL</b> edildi` : '')
        ].filter(Boolean).join('<br>');

        Swal.fire('OK','Ləğv etmə əməliyyatı tamamlandı','success');

        // Paneli təzələ (kartların aktivlik vəziyyəti yenilənsin)
        renderLayersPanel();
      } catch (e) {
        console.error(e);
        Swal.fire('Xəta', e.message || 'Soft delete əməliyyatı alınmadı.','error');
      }
    });
  }


  // === Attach (LS ilə) ===
  const chkAttach  = document.getElementById('chkAttachLayer');
  const btnZoomA   = document.getElementById('btnZoomAttach');

  (async () => {
    await loadAttachLayer({ fit:false });
    // init from LS
    const visAttach = getVisFlag('attach', true);
    chkAttach.checked = visAttach;
    if (attachLayer) attachLayer.setVisible(chkAttach.checked);

    if (attachLayerCount > 0) {
      await Promise.all([
        refreshTekuisFromAttachIfAny(),
        refreshNecasFromAttachIfAny()
      ]);
    } else {
      tekuisSource.clear(true);
      necasSource.clear(true);
      tekuisCount = 0; necasCount = 0;
      if (document.getElementById('cardTekuis')) setCardDisabled('cardTekuis', true);
      if (document.getElementById('cardNecas'))  setCardDisabled('cardNecas',  true);
    }
  })();

  chkAttach.addEventListener('change', async () => {
  if (attachLayer) attachLayer.setVisible(chkAttach.checked);
  setVisFlag('attach', chkAttach.checked);

  // >>> IMPORTANT: Qoşma layı sadəcə görünən/gizli edirik.
  // TEKUİS lokalda kəsilibsə, onu üstələməyək – heç nə fetch etmirik.
  // Əgər həqiqətən yeniləmək istəsəniz, başqa yerdə refreshTekuisFromAttachIfAny(true) çağıracağıq.

  if (chkAttach.checked) flashLayer(attachLayer);
});








  btnZoomA.addEventListener('click', () => {
    if (attachLayerSource && attachLayerSource.getFeatures().length > 0){
      const ext = attachLayerSource.getExtent();
      map.getView().fit(ext, { padding:[20,20,20,20], duration:600, maxZoom:18 });
    } else {
      Swal.fire('Info','Zoom ediləcək attach obyekti yoxdur.','info');
    }
  });

  // === TEKUİS (LS ilə) ===
  const chkT  = document.getElementById('chkTekuisLayer');
  const btnZT = document.getElementById('btnZoomTekuis');

  // init from LS
  const visTekuis = getVisFlag('tekuis', false);
  chkT.checked = visTekuis;
  tekuisLayer.setVisible(chkT.checked && tekuisCount > 0);

  chkT.addEventListener('change', () => {
    setVisFlag('tekuis', chkT.checked);
    tekuisLayer.setVisible(chkT.checked && tekuisCount > 0);
    if (chkT.checked) flashLayer(tekuisLayer);
  });

  if (btnZT){
    btnZT.addEventListener('click', () => {
      if (tekuisSource && tekuisSource.getFeatures().length > 0){
        const ext = tekuisSource.getExtent();
        map.getView().fit(ext, { padding:[20,20,20,20], duration:600, maxZoom:18 });
      } else {
        Swal.fire('Info','Zoom ediləcək TEKUİS obyekti yoxdur.','info');
      }
    });
  }




  // === NECAS (LS ilə) ===
  const chkN  = document.getElementById('chkNecasLayer');
  const btnZN = document.getElementById('btnZoomNecas');

  // init from LS
  const visNecas = getVisFlag('necas', false);
  chkN.checked = visNecas;
  necasLayer.setVisible(chkN.checked && necasCount > 0);

  chkN.addEventListener('change', () => {
    setVisFlag('necas', chkN.checked);
    necasLayer.setVisible(chkN.checked && necasCount > 0);
    if (chkN.checked) flashLayer(necasLayer);
  });

  if (btnZN){
    btnZN.addEventListener('click', () => {
      if (necasSource && necasSource.getFeatures().length > 0){
        const ext = necasSource.getExtent();
        map.getView().fit(ext, { padding:[20,20,20,20], duration:600, maxZoom:18 });
      } else {
        Swal.fire('Info','Zoom ediləcək NECAS obyekti yoxdur.','info');
      }
    });
  }


  // Qoşma lay checkbox wiring (təhlükəsizlik üçün panel renderdən sonra da)
  (function syncAttachCheckbox(){
    const chkAttach = document.getElementById('chkAttachLayer');
    if (!chkAttach) return;

    const vis = getVisFlag('attach', false);
    chkAttach.checked = vis;
    if (window.attachLayer) window.attachLayer.setVisible(vis);

    if (!chkAttach._wired) {
      chkAttach.addEventListener('change', (e) => {
        const on = !!e.target.checked;
        setVisFlag('attach', on);
        if (window.attachLayer) window.attachLayer.setVisible(on);
      });
      chkAttach._wired = true;
    }
  })();


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
      renderDataPanel();
    } else if (which === 'catalog') {
      Swal.fire('Məlumat', 'Redaktə alətləri indi sağdakı toolbar-dadır.', 'info');
    } else if (which === 'symbology') {
      renderBasemapsPanel();
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
setBasemap('google');

// Ticket statusunu yüklə və icazələri tətbiq et
fetchTicketStatus();

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
