/* =========================
   EDITING & INTERACTIONS
   ========================= */
window.MainEditing = window.MainEditing || {};

window.MainEditing.init = function initEditing(state = {}) {
  const {
    map,
    tekuisSource,
    tekuisLayer,
    necasLayer,
    infoHighlightSource,
    openPanel,
    moveIndicatorToButton,
    setInfoHighlight,
    trackFeatureOwnership,
    getFeatureOwner
  } = state;

  if (!map) return {};

  /* =========================
     REDAKTƏ
     ========================= */
  const editSource = new ol.source.Vector();
  trackFeatureOwnership?.(editSource);
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
  ['addfeature','removefeature','changefeature'].forEach(ev => {
    tekuisSource?.on?.(ev, () => {
      window.saveTekuisToLS?.();
      // Geometriya dəyişdi → həm OK, həm də əvvəlki ignore-ları sıfırla
      window._topoLastOk = null;
      window._lastTopoValidation = null;
      window._ignoredTopo = { overlaps: new Set(), gaps: new Set() };
    });
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
  function cloneFeatureAttributes(feature) {
    const props = { ...(feature?.getProperties?.() || {}) };
    delete props.geometry;
    return props;
  }

  function explodeSelectedMultiPolygons(){
    const unified = getUnifiedSelectedFeatures();
    if (unified.length === 0) {
      Swal.fire('Info', 'Seçilmiş obyekt yoxdur.', 'info');
      return;
    }

    let explodedCount = 0;
    let createdCount = 0;
    let skippedSingle = 0;
    let skippedNoSource = 0;

    const selectAnyFeatures = selectAny.getFeatures();
    const selectInteractionFeatures = selectInteraction.getFeatures();

    unified.forEach(feature => {
      const geom = feature?.getGeometry?.();
      if (!geom || geom.getType() !== 'MultiPolygon') return;

      const parts = geom.getCoordinates();
      if (!Array.isArray(parts) || parts.length < 2) {
        skippedSingle += 1;
        return;
      }

      const hit = findVectorLayerAndSourceOfFeature(feature);
      if (!hit || !hit.source) {
        skippedNoSource += 1;
        return;
      }

      const wasInSelectAny = selectAnyFeatures.getArray().includes(feature);
      const wasInSelectInteraction = selectInteractionFeatures.getArray().includes(feature);

      const baseProps = cloneFeatureAttributes(feature);

      try { hit.source.removeFeature(feature); } catch {}
      if (wasInSelectAny) { try { selectAnyFeatures.remove(feature); } catch {} }
      if (wasInSelectInteraction) { try { selectInteractionFeatures.remove(feature); } catch {} }

      const newFeatures = parts.map(partCoords => {
        const nf = new ol.Feature({ ...baseProps });
        nf.setGeometry(new ol.geom.Polygon(partCoords));
        hit.source.addFeature(nf);
        return nf;
      });

      if (wasInSelectAny) {
        newFeatures.forEach(f => selectAnyFeatures.push(f));
      }
      if (wasInSelectInteraction) {
        newFeatures.forEach(f => selectInteractionFeatures.push(f));
      }

      explodedCount += 1;
      createdCount += newFeatures.length;
    });

    if (explodedCount === 0) {
      Swal.fire(
        'Info',
        'Explode üçün ən azı 1 multipart (MultiPolygon) seçilməlidir.',
        'info'
      );
      return;
    }

    try { window.saveTekuisToLS?.(); } catch {}
    updateDeleteButtonState();
    updateAllSaveButtons();

    const noteParts = [];
    if (skippedSingle) noteParts.push(`${skippedSingle} ədəd tək hissəli poliqon atlandı`);
    if (skippedNoSource) noteParts.push(`${skippedNoSource} ədəd mənbəsiz obyekt atlandı`);
    const noteText = noteParts.length ? ` (${noteParts.join(', ')})` : '';

    if (window.showToast) {
      window.showToast(`Explode: ${explodedCount} multipart → ${createdCount} poliqon${noteText}`, 2600);
    } else {
      Swal.fire(
        'Uğurlu',
        `Explode tamamlandı: ${explodedCount} multipart → ${createdCount} poliqon${noteText}`,
        'success'
      );
    }
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


  async function uploadAttachmentToBackend(file, crs){
    if (!window.EDIT_ALLOWED) {
      return { ok:false, message:'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!' };
    }


    if (!file || !state.PAGE_TICKET) return { ok:false, message:'Fayl və ya ticket yoxdur' };
    try{
      const fd = new FormData();
      fd.append('file', file);
      fd.append('ticket', state.PAGE_TICKET);
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

    if (!state.PAGE_TICKET){
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
          body: JSON.stringify({ wkt: wkt, ticket: state.PAGE_TICKET })
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
      if (alsoAttach && state.lastUploadState?.file){
        RTLoading.set('Qoşmalar saxlanır…'); // mətnini dəyiş
        try {
          const up = await uploadAttachmentToBackend(state.lastUploadState.file, state.lastUploadState.crs);
          attachOk = !!(up && up.ok);
          if (attachOk){
            await state.loadAttachLayer?.({ fit:false });
            state.updateTicketDeleteState?.();
          }
        } catch (e) {
          console.error(e);
          attachOk = false;
        }
      }

      // 3) Nəticə mesajı
      if (attachOk){
        Swal.fire('Uğurlu', `Poliqon və qoşmalar yadda saxlandı.`, 'success');
      } else if (!state.lastUploadState?.file){
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
    btnExplode: null,
    btnClear:  null,
    btnSave:   null
  };

  function ensureEditAllowed(title = 'Diqqət') {
    if (!window.EDIT_ALLOWED) {
      Swal.fire(title, 'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'info');
      return false;
    }
    return true;
  }
  window.ensureEditAllowed = ensureEditAllowed;

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
    rtEditUI.btnExplode = mkBtn('rtExplode',  'Multipart poliqonu parçala',  'explode');
    rtEditUI.btnClear  = mkBtn('rtClearAll',  'Hamısını sil',                 'clearAll');
    rtEditUI.btnSave   = mkBtn('rtSave',      'Yadda saxla',                  'save');
    rtEditUI.btnErase  = mkBtn('rtErase',     'Tədqiqat daxilini kəs & sil',  'erase');

    // ===== EVENT LISTENER-LƏR =====

    // 1) Info düyməsi
    rtEditUI.btnInfo.addEventListener('click', toggleInfoMode);

    // 2) Erase düyməsi
    rtEditUI.btnErase.addEventListener('click', () => {
      if (!ensureEditAllowed()) return;
      const btn = document.getElementById('btnEraseTekuisInsideTicket');
      if (btn && typeof btn.click === 'function') {
        btn.click();
      } else {
        Swal.fire('Info', 'Erase funksiyası hazırda əlçatan deyil. "Laylar" panelini açın.', 'info');
      }
    });

    // 3) Draw düyməsi
    rtEditUI.btnDraw.addEventListener('click', () => {
      if (!ensureEditAllowed()) return;
      if (drawInteraction) { stopDraw(); } else { startDraw(); }
    });

    // 4) Snap düyməsi
    rtEditUI.btnSnap.addEventListener('click', () => {
      if (!ensureEditAllowed()) return;
      toggleSnap();
      rtEditUI.btnSnap.classList.toggle('active', !!snapState.enabled);
    });

    // 5) Delete düyməsi — Tədqiqat layı + TEKUİS layı üçün işləsin
    rtEditUI.btnDelete.addEventListener('click', () => {
      if (!ensureEditAllowed()) return;

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

        // 5.5) Explode düyməsi — Multipart poliqonu parçala
    rtEditUI.btnExplode.addEventListener('click', () => {
      if (!ensureEditAllowed()) return;
      explodeSelectedMultiPolygons();
    });


    // 6) Clear düyməsi
    rtEditUI.btnClear.addEventListener('click', () => {
      if (!ensureEditAllowed()) return;
      editSource.clear();
      selectInteraction.getFeatures().clear();
      updateDeleteButtonState();
      updateEditStatus && updateEditStatus('Bütün obyektlər silindi.');
      updateAllSaveButtons();
    });

    // 7) Save düyməsi
    rtEditUI.btnSave.addEventListener('click', () => {
      if (!ensureEditAllowed()) return;
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
      if (!ensureEditAllowed()) return;
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

  modifyAnyInteraction.on('modifyend', () => {
    try { window.saveTekuisToLS?.(); } catch {}
    updateAllSaveButtons?.();
  });

  Object.assign(state, {
    editSource,
    editLayer,
    selectAny,
    selectInteraction,
    pauseEditingInteractions,
    resumeEditingInteractions,
    registerSnapSource,
    enableSnap,
    disableSnap,
    stopDraw,
    startDraw,
    updateAllSaveButtons,
    updateDeleteButtonState
  });

  window.updateAllSaveButtons = updateAllSaveButtons;
  window.updateDeleteButtonState = updateDeleteButtonState;
  window.saveSelected = saveSelected;

  return {
    registerSnapSource,
    updateAllSaveButtons,
    updateDeleteButtonState
  };
  
};