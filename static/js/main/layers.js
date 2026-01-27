/* =========================
   LAYERS & PANELS
   ========================= */
window.MainLayers = window.MainLayers || {};

window.MainLayers.init = function initLayers(state = {}) {
  const {
    map,
    PAGE_TICKET,
    tekuisSource,
    tekuisLayer,
    necasSource,
    necasLayer,
    panelEl,
    panelBodyEl,
    indicatorEl,
    openPanel,
    moveIndicatorToButton,
    applyNoDataCardState,
    setCardDisabled,
    renderLayersPanel,
    updateTicketDeleteState,
    styleTicketDefault,
    styleAttachDefault,
    trackFeatureOwnership
  } = state;

  if (!map) return {};

  /* =========================
     “Məlumat daxil et” paneli
     ========================= */

  const dataPanelApi = window.setupDataPanel?.({
    openPanel,
    panelBodyEl,
    uploadHandlers: state.uploadHandlers
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
    selectAny: state.selectAny,
    getFeatureOwner: state.getFeatureOwner,
    onCountChange: (count) => {
      state.tekuisCount = count;
      const lbl = document.getElementById('lblTekuisCount');
      if (lbl) lbl.textContent = `(${state.tekuisCount})`;
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

  Object.assign(state, {
    tekuisCache,
    readVis,
    writeVis,
    setVisFlag,
    getVisFlag,
    saveTekuisToLS,
    loadTekuisFromLS
  });

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
      trackFeatureOwnership?.(ticketLayerSource);
      ticketLayerCount = features.length;
      state.ticketLayerSource = ticketLayerSource;
      state.ticketLayerCount = ticketLayerCount;

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
      state.ticketLayer = ticketLayer;

      state.registerSnapSource?.(ticketLayerSource);

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
      trackFeatureOwnership?.(attachLayerSource);
      attachLayerCount = features.length;
      state.attachLayerSource = attachLayerSource;
      state.attachLayerCount = attachLayerCount;

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
      state.attachLayer = attachLayer;

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

      state.registerSnapSource?.(attachLayerSource);

      if (fit && attachLayerCount > 0){
        const ext = attachLayerSource.getExtent();
        map.getView().fit(ext, { padding:[20,20,20,20], duration:600, maxZoom:18 });
      }


      if (attachLayerCount > 0){
        // TEKUİS lokalda (LS) varsa, üstələmirik
        if (!state.tekuisHasCache?.()){
          await state.refreshTekuisFromAttachIfAny?.(false);
        }
        await state.refreshNecasFromAttachIfAny?.();
      } else {
        if (!state.tekuisHasCache?.()){
          tekuisSource?.clear?.(true);
          state.tekuisCount = 0;
        }
        necasSource?.clear?.(true);
        state.necasCount = 0;
      }




      return { ok:true, count: attachLayerCount };
    }catch(err){
      console.error(err);
      Swal.fire('Xəta', (err && err.message) || 'Attach layı yüklənmədi.', 'error');
      return { ok:false, count:0 };
    }
  }


  const tekuisNecasApi = window.TekuisNecas?.create({
    applyNoDataCardState,
    getPageTicket: () => PAGE_TICKET,
    getTekuisCount: () => state.tekuisCount,
    setTekuisCount: (val) => { state.tekuisCount = val; },
    getNecasCount: () => state.necasCount,
    setNecasCount: (val) => { state.necasCount = val; },
    getAttachLayer: () => attachLayer,
    getAttachLayerSource: () => attachLayerSource,
    getTekuisLayer: () => tekuisLayer,
    getTekuisSource: () => tekuisSource,
    getNecasLayer: () => necasLayer,
    getNecasSource: () => necasSource
  });

  if (tekuisNecasApi) {
    window.tekuisNecasApi = tekuisNecasApi;
    ({
      fetchTekuisByBboxForLayer: state.fetchTekuisByBboxForLayer,
      fetchTekuisByAttachTicket: state.fetchTekuisByAttachTicket,
      refreshTekuisFromAttachIfAny: state.refreshTekuisFromAttachIfAny,
      refreshNecasFromAttachIfAny: state.refreshNecasFromAttachIfAny,
      clearTekuisCache: state.clearTekuisCache,
      tekuisHasCache: state.tekuisHasCache
    } = tekuisNecasApi);
  }

  window.addEventListener('beforeunload', () => {
    state.clearTekuisCache?.();
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
    refreshTekuisFromAttachIfAny: state.refreshTekuisFromAttachIfAny,
    refreshNecasFromAttachIfAny: state.refreshNecasFromAttachIfAny,
    clearTekuisCache: state.clearTekuisCache,
    tryValidateAndSaveTekuis,
    getTicketLayer: () => ticketLayer,
    getTicketLayerSource: () => ticketLayerSource,
    getTicketLayerCount: () => ticketLayerCount,
    getAttachLayer: () => attachLayer,
    getAttachLayerSource: () => attachLayerSource,
    getAttachLayerCount: () => attachLayerCount,
    getTekuisLayer: () => tekuisLayer,
    getTekuisSource: () => tekuisSource,
    getTekuisCount: () => state.tekuisCount,
    setTekuisCount: (val) => { state.tekuisCount = val; },
    getNecasLayer: () => necasLayer,
    getNecasSource: () => necasSource,
    getNecasCount: () => state.necasCount,
    setNecasCount: (val) => { state.necasCount = val; }

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
        renderLayersPanel?.();
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
  state.authFetchTicketStatus?.();

  // --- İlk yükləmədə qoşma layını və TEKUİS/NECAS-ı serverdən gətir
  (async () => {
    if (!PAGE_TICKET) return;

    // Qoşma layını yüklə (fit=false: avtomatik zoom etməsin)
    await loadAttachLayer({ fit: false });

    // TEKUİS/NECAS-ı qoşma geometriyasına görə serverdən yenilə
    await state.refreshTekuisFromAttachIfAny?.(true);  // LS-ə baxmadan serverdən gətir
    await state.refreshNecasFromAttachIfAny?.();

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


      renderLayersPanel?.();
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

  Object.assign(state, {
    loadAttachLayer,
    loadTicketLayer,
    getVisFlag,
    setVisFlag
  });

  return {
    loadAttachLayer,
    loadTicketLayer,
    flashLayer
  };
};