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
      it.style.borderRadius = '12px';
    } else {
      it.classList.remove('selected');
      it.style.border = '2px solid transparent';
      it.style.boxShadow = 'none';
    }
  });
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

/* =========================
   REDAKTƏ
   ========================= */
const editSource = new ol.source.Vector();
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
  layers: (layer) => layer instanceof ol.layer.Vector,
  style: redSelectStyleFn,
  hitTolerance: 3
});
map.addInteraction(selectAny);

/* ---------- SNAP ---------- */
const snapState = { enabled: false, interactions: [] };
const snapSources = new Set();
function addSnapForSource(src){
  const snap = new ol.interaction.Snap({ source: src, pixelTolerance: 12, edge: true, vertex: true });
  map.addInteraction(snap);
  snapState.interactions.push(snap);
}
function registerSnapSource(src){
  if (!src || snapSources.has(src)) return;
  snapSources.add(src);
  if (snapState.enabled) addSnapForSource(src);
}
function enableSnap(){
  if (snapState.enabled) return;
  snapState.enabled = true;
  snapSources.forEach(addSnapForSource);
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
  enableSnap();
  drawInteraction = new ol.interaction.Draw({ source: editSource, type: 'Polygon' });
  map.addInteraction(drawInteraction);
  drawInteraction.on('drawend', (e) => {
    const f = e.feature;
    const sel = selectInteraction.getFeatures();
    sel.clear(); sel.push(f);
    updateEditStatus && updateEditStatus('Yeni poliqon əlavə edildi. Bitirmək üçün double-click.');
    updateDeleteButtonState && updateDeleteButtonState();
    updateAllSaveButtons();
  });
  updateDrawBtnUI && updateDrawBtnUI(true);
  updateEditStatus && updateEditStatus('Çəkmə aktivdir. Snap AÇIQdır.');
}
function stopDraw(silent=false){
  if (drawInteraction) { map.removeInteraction(drawInteraction); drawInteraction = null; }
  disableSnap();
  if (!silent) {
    updateDrawBtnUI && updateDrawBtnUI(false);
    updateEditStatus && updateEditStatus('Çəkmə dayandırıldı. Snap BAĞLIdır.');
  }
}

/* =============== YADDA SAXLA (frontend) =============== */
function getUnifiedSelectedFeatures(){
  const a = selectAny.getFeatures().getArray();
  const b = selectInteraction.getFeatures().getArray();
  const set = new Set([...a, ...b]);
  return Array.from(set);
}
function getSinglePolygonSelection(){
  const arr = getUnifiedSelectedFeatures();
  if (arr.length !== 1) return null;
  const g = arr[0].getGeometry();
  const t = g && g.getType();
  if (t === 'Polygon' || t === 'MultiPolygon') return arr[0];
  return null;
}
function updateAllSaveButtons(){
  const hasPoly = !!getSinglePolygonSelection();
  const btn1 = document.getElementById('btnSaveDataPanel');
  if (btn1) btn1.disabled = !hasPoly;
  const btn2 = document.getElementById('btnSaveEditPanel');
  if (btn2) btn2.disabled = !hasPoly;
}

const wktWriter = new ol.format.WKT();
async function saveSelectedPolygon(){
  const f = getSinglePolygonSelection();
  if (!f) {
    Swal.fire('Diqqət', 'Yadda saxlamaq üçün ekranda dəqiq 1 poliqon seçilməlidir.', 'warning');
    return;
  }
  if (!PAGE_TICKET){
    Swal.fire('Diqqət', 'Ticket tapılmadı. Zəhmət olmasa Node tətbiqindən yenidən “Xəritəyə keç” edin.', 'warning');
    return;
  }

  const geom4326 = f.getGeometry().clone().transform('EPSG:3857', 'EPSG:4326');
  const wkt = wktWriter.writeGeometry(geom4326, { decimals: 8 });

  try {
    const resp = await fetch('/api/save-polygon/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept':'application/json' },
      body: JSON.stringify({ wkt: wkt, ticket: PAGE_TICKET })
    });
    if (!resp.ok) throw new Error(await resp.text() || `HTTP ${resp.status}`);
    const data = await resp.json();
    Swal.fire('Uğurlu', `Poliqon bazaya yazıldı. ID: ${data.id}`, 'success');
  } catch (e) {
    console.error(e);
    Swal.fire('Xəta', e.message || 'Bazaya yazmaq alınmadı.', 'error');
  }
}
selectAny.getFeatures().on('add', updateAllSaveButtons);
selectAny.getFeatures().on('remove', updateAllSaveButtons);
selectInteraction.getFeatures().on('add', updateAllSaveButtons);
selectInteraction.getFeatures().on('remove', updateAllSaveButtons);
map.on('click', updateAllSaveButtons);

/* =========================
   Redaktə paneli (UI)
   ========================= */
let editUI = { btnDraw:null, btnDelete:null, btnClear:null, btnSnap:null, status:null, btnSave:null };

function renderEditPanel(){
  const html = `
    <div class="card">
      <div class="upload-title" style="margin-bottom:10px;">Redaktə alətləri</div>
      <div class="toolbar" style="margin-bottom:8px; display:flex; flex-wrap:wrap; gap:6px;">
        <button id="btnDrawPoly"   class="btn">Poliqon çək</button>
        <button id="btnSnapToggle" class="btn secondary">Snap: Bağlı</button>
        <button id="btnDeleteSel"  class="btn danger" disabled>Seçiləni sil</button>
        <button id="btnClearAll"   class="btn secondary">Hamısını sil</button>
        <button id="btnSaveEditPanel" class="btn primary" disabled>Yadda saxla</button>
      </div>
      <div id="editStatus" class="small">
        İpuçları: Poliqonu bitirmək üçün <b>double-click</b>. Seç və yalnız <b>vertex</b>ləri sürüşdür.
      </div>
    </div>
  `;
  openPanel('Redaktə', html);

  editUI.btnDraw   = document.getElementById('btnDrawPoly');
  editUI.btnSnap   = document.getElementById('btnSnapToggle');
  editUI.btnDelete = document.getElementById('btnDeleteSel');
  editUI.btnClear  = document.getElementById('btnClearAll');
  editUI.btnSave   = document.getElementById('btnSaveEditPanel');
  editUI.status    = document.getElementById('editStatus');

  editUI.btnDraw.addEventListener('click', () => { if (drawInteraction) stopDraw(); else startDraw(); });
  editUI.btnSnap.addEventListener('click', () => toggleSnap());
  editUI.btnDelete.addEventListener('click', () => {
    const sel = selectInteraction.getFeatures();
    const toRemove = sel.getArray().slice();
    toRemove.forEach(f => editSource.removeFeature(f));
    sel.clear();
    updateDeleteButtonState();
    updateEditStatus('Seçilmiş obyekt silindi.');
    updateAllSaveButtons();
  });
  editUI.btnClear.addEventListener('click', () => {
    editSource.clear();
    selectInteraction.getFeatures().clear();
    updateDeleteButtonState();
    updateEditStatus('Bütün obyektlər silindi.');
    updateAllSaveButtons();
  });
  editUI.btnSave.addEventListener('click', saveSelectedPolygon);

  updateDeleteButtonState();
  updateSnapBtnUI();
  updateAllSaveButtons();
}
function updateDrawBtnUI(isActive){
  if (!editUI.btnDraw) return;
  editUI.btnDraw.textContent = isActive ? 'Çəkimi dayandır' : 'Poliqon çək';
}
function updateDeleteButtonState(){
  if (!editUI.btnDelete) return;
  const count = selectInteraction.getFeatures().getLength();
  editUI.btnDelete.disabled = count === 0;
}
function updateEditStatus(text){
  if (editUI.status) editUI.status.textContent = text;
}
function updateSnapBtnUI(){
  if (!editUI.btnSnap) return;
  if (snapState.enabled) {
    editUI.btnSnap.textContent = 'Snap: Açıq';
    editUI.btnSnap.classList.remove('secondary');
  } else {
    editUI.btnSnap.textContent = 'Snap: Bağlı';
    editUI.btnSnap.classList.add('secondary');
  }
}
selectInteraction.on('select', () => { updateDeleteButtonState(); updateAllSaveButtons(); });

/* =========================
   “Məlumatlar” – INFO MODE
   ========================= */
let infoMode = false;
let infoClickKey = null;

function setLeftButtonActiveForInfo(){
  // sol “Məlumatlar” düyməsini aktiv göstər, panel indikatorunu ora köçür
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
function renderInfoPanel(record, fk){
  if (!record){
    openPanel('Məlumatlar', `
      <div class="card"><div class="small">Məlumat tapılmadı.</div></div>
    `);
    setLeftButtonActiveForInfo();
    return;
  }
  // səliqəli key-value grid
  const entries = Object.entries(record);
  const rows = entries.map(([k,v])=>`
     <div class="k">${k}</div><div class="v">${v==null?'—':String(v)}</div>
  `).join('');
  openPanel('Məlumatlar', `
    <div class="card">
      <div class="small" style="margin-bottom:8px;">fk_metadata: <b>${fk}</b></div>
      <div class="kv">
        <div class="h">TBL_REQUEST_REG</div>
        <div class="sep"></div>
        ${rows}
      </div>
    </div>
  `);
  setLeftButtonActiveForInfo();
}
function getFkFromFeature(feature){
  // property adlarını case-insensitive axtar
  const props = feature.getProperties() || {};
  const keys = Object.keys(props);
  const wanted = ['fk_metadata','fkmeta','metadata_id','rowid','fk','request_id'];
  for (const k of keys){
    const kk = k.toString().toLowerCase();
    if (wanted.includes(kk)) return props[k];
  }
  // bəzi shapefile-lərdə DBF sütun adları böyük ola bilər
  for (const k of keys){
    if (/^FK_?METADATA$/i.test(k)) return props[k];
    if (/^ROW_?ID$/i.test(k)) return props[k];
  }
  return null;
}
async function fetchFeatureInfoByFk(fk){
  // fk-nı ədədə çevirməyə cəhd (server int gözləyir)
  const fki = parseInt(String(fk), 10);
  if (!Number.isFinite(fki)) {
    Swal.fire('Diqqət', 'Bu obyekt üçün etibarlı fk_metadata tapılmadı.', 'warning');
    return;
  }
  try {
    renderInfoPanelLoading();
    const resp = await fetch(`/api/feature-info/?fk=${encodeURIComponent(fki)}`, {
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
    if (data && data.found && data.record){
      renderInfoPanel(data.record, fki);
    } else {
      renderInfoPanel(null, fki);
    }
  } catch(err){
    console.error(err);
    Swal.fire('Xəta', (err && err.message) || 'Məlumatı almaq olmadı.', 'error');
  }
}
function onMapClickForInfo(evt){
  let hitFeature = null;
  // bütün vektor laylarında feature tap (editLayer + uploadedLayer + digər vektorlar)
  map.forEachFeatureAtPixel(evt.pixel, (feat, layer) => {
    if (layer instanceof ol.layer.Vector) {
      hitFeature = feat;
      return true;
    }
    return false;
  }, { hitTolerance: 5 });

  if (!hitFeature){
    Swal.fire('Diqqət', 'Obyekt tapılmadı. Zəhmət olmasa vektor obyektinin üzərinə klik edin.', 'info');
    return;
  }
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
  infoClickKey = map.on('singleclick', onMapClickForInfo);
  // paneli öncədən aç
  renderInfoPanelLoading();
}
function disableInfoMode(){
  if (!infoMode) return;
  infoMode = false;
  document.getElementById('rtInfo')?.classList.remove('active');
  if (infoClickKey) {
    ol.Observable.unByKey(infoClickKey);
    infoClickKey = null;
  }
}
function toggleInfoMode(){
  infoMode ? disableInfoMode() : enableInfoMode();
}

/* =========================
   Sağ toolbar düymələri
   ========================= */
document.getElementById('rtInfo')?.addEventListener('click', toggleInfoMode);
// placeholder-lar:
document.getElementById('rtStyle')?.addEventListener('click', ()=> Swal.fire('Info','Bu düymə hələ aktiv deyil.','info'));
document.getElementById('rtDraw')?.addEventListener('click', ()=> {
  const btn = document.querySelector('.tool-btn[data-panel="catalog"]');
  btn?.click(); // soldakı “Redaktə” panelini aç
});

/* =========================
   “Məlumat daxil et” paneli (sənin mövcud kodun)
   ========================= */
function renderDataPanel(){
  const html = `
    <div class="tabs">
      <div class="tab active" data-tab="shp">Shapefile (.zip/.rar)</div>
      <div class="tab" data-tab="pts">Koordinatlar (.csv/.txt)</div>
    </div>
    <div id="tabContent"></div>

    <div class="card" style="margin-top:10px;">
      <div class="small" style="margin-bottom:6px;">
        Yadda saxlamaq üçün ekranda <b>yalnız 1 poliqon</b> seçili olmalıdır.
      </div>
      <button id="btnSaveDataPanel" class="btn primary" disabled>Yadda saxla</button>
    </div>
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
            <div class="upload-title">Shapefile arxivi (.zip / .rar)</div>
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
  btnSave.addEventListener('click', saveSelectedPolygon);
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
               style="border:2px solid transparent;border-radius:12px;overflow:hidden;cursor:pointer;background:#f3f4f6;">
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
   Sol düymələr
   ========================= */
document.querySelectorAll('.tool-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tool-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    moveIndicatorToButton(btn);

    const which = btn.dataset.panel;
    if (which === 'contents') {
      renderDataPanel();
    } else if (which === 'catalog') {
      renderEditPanel();
    } else if (which === 'symbology') {
      renderBasemapsPanel();
    } else if (which === 'info') {
      // info panelini aç, info-modu OFF saxla (sağ düymədən aktivləşdiriləcək)
      openPanel('Məlumatlar', '<div class="card"><div class="small">Sağdakı mavi düymə ilə “İnformasiya” modunu aktivləşdirin, sonra obyektə klik edin.</div></div>');
    } else {
      openPanel(btn.textContent.trim(), '');
    }
  });
});

window.addEventListener('resize', () => {
  const activeBtn = document.querySelector('.tool-btn.active');
  if (activeBtn && !indicatorEl.hidden && !panelEl.hidden) {
    moveIndicatorToButton(activeBtn);
  }
});

// Başlanğıc
setBasemap('google');

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
    const activeBtn = document.querySelector('.tool-btn.active');
    if (activeBtn && !panelEl.hidden) moveIndicatorToButton(activeBtn);
  }
});
