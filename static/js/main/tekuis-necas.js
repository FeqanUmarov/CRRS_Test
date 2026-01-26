window.TekuisNecas = window.TekuisNecas || {};

window.TekuisNecas.create = function createTekuisNecas({
  applyNoDataCardState,
  getPageTicket,
  getTekuisCount,
  setTekuisCount,
  getNecasCount,
  setNecasCount,
  getAttachLayer,
  getAttachLayerSource,
  getTekuisLayer,
  getTekuisSource,
  getNecasLayer,
  getNecasSource
} = {}) {
  const TEXT_TEKUIS_DEFAULT = 'TEKUİS sisteminin parsel məlumatları.';
  const TEXT_NECAS_DEFAULT  = 'NECAS sistemində qeydiyyatdan keçmiş parsellər.';
  const TEXT_TEKUIS_EMPTY   = 'TEKUİS məlumat bazasında heç bir məlumat tapılmadı.';
  const TEXT_NECAS_EMPTY    = 'NECAS məlumat bazasında heç bir məlumat tapılmadı.';

  const localState = {
    tekuisCount: 0,
    necasCount: 0
  };

  const getPageTicketSafe = typeof getPageTicket === 'function'
    ? getPageTicket
    : () => window.PAGE_TICKET || null;

  const getTekuisCountSafe = typeof getTekuisCount === 'function'
    ? getTekuisCount
    : () => localState.tekuisCount;
  const setTekuisCountSafe = typeof setTekuisCount === 'function'
    ? setTekuisCount
    : (val) => { localState.tekuisCount = val; };

  const getNecasCountSafe = typeof getNecasCount === 'function'
    ? getNecasCount
    : () => localState.necasCount;
  const setNecasCountSafe = typeof setNecasCount === 'function'
    ? setNecasCount
    : (val) => { localState.necasCount = val; };

  const getAttachLayerSafe = typeof getAttachLayer === 'function'
    ? getAttachLayer
    : () => window.attachLayer;
  const getAttachLayerSourceSafe = typeof getAttachLayerSource === 'function'
    ? getAttachLayerSource
    : () => window.attachLayerSource;

  const getTekuisLayerSafe = typeof getTekuisLayer === 'function'
    ? getTekuisLayer
    : () => window.MapContext?.tekuisLayer;
  const getTekuisSourceSafe = typeof getTekuisSource === 'function'
    ? getTekuisSource
    : () => window.MapContext?.tekuisSource;

  const getNecasLayerSafe = typeof getNecasLayer === 'function'
    ? getNecasLayer
    : () => window.MapContext?.necasLayer;
  const getNecasSourceSafe = typeof getNecasSource === 'function'
    ? getNecasSource
    : () => window.MapContext?.necasSource;

  const safeApplyNoDataCardState = (...args) => applyNoDataCardState?.(...args);

  function isFiniteExtent(ext){
    return Array.isArray(ext) && ext.length === 4 && ext.every(Number.isFinite);
  }

  function showTekuis(fc){
    try{
      const format = new ol.format.GeoJSON();
      const feats = format.readFeatures(fc, {
        dataProjection: 'EPSG:4326',
        featureProjection: 'EPSG:3857'
      });
      const tekuisSource = getTekuisSourceSafe();
      const tekuisLayer = getTekuisLayerSafe();
      if (!tekuisSource) return;
      tekuisSource.clear(true);
      tekuisSource.addFeatures(feats);

      setTekuisCountSafe(feats.length);
      const tekuisMode = window.TekuisSwitch?.getMode?.() || 'live';
      if (tekuisMode === 'live') {
        window.tekuisCache?.saveOriginalTekuis?.(fc);
      }

      if (document.getElementById('cardTekuis')){
        const mode = (window.TekuisSwitch && typeof window.TekuisSwitch.getMode === 'function')
          ? window.TekuisSwitch.getMode()
          : 'live';

        const isDbMode = mode === 'current' || mode === 'old';
        const defaultText = isDbMode
          ? (window.TEXT_TEKUIS_DB_DEFAULT || 'tədqiqat nəticəsində dəyişiklik edilərək saxlanılan TEKUİS parselləri')
          : (window.TEXT_TEKUIS_DEFAULT    || 'TEKUİS sisteminin parsel məlumatları.');

        const suffix = isDbMode ? ' (Mənbə: Local baza)' : ' (Mənbə: TEKUİS – canlı)';

        safeApplyNoDataCardState(
          'cardTekuis',
          getTekuisCountSafe() === 0,
          TEXT_TEKUIS_EMPTY,
          defaultText + suffix
        );
      }

      const chk = document.getElementById('chkTekuisLayer');
      if (getTekuisCountSafe() === 0){
        tekuisLayer?.setVisible(false);
        if (chk) chk.checked = false;
      } else if (chk){
        tekuisLayer?.setVisible(chk.checked);
      }
    }catch(e){
      console.error('TEKUİS parse error:', e);
    }

    window.saveTekuisToLS?.();
  }
  window.showTekuis = showTekuis;

  function fetchTekuisByBboxForLayer(layer){
    if (!layer || !layer.getSource) return;
    const extent3857 = layer.getSource().getExtent?.();
    if (!isFiniteExtent(extent3857)) return;

    window.TekuisSwitch?.setMode?.('live');

    const [minx, miny, maxx, maxy] =
      ol.proj.transformExtent(extent3857, 'EPSG:3857', 'EPSG:4326');

    const url = `/api/tekuis/parcels/by-bbox/?minx=${minx}&miny=${miny}&maxx=${maxx}&maxy=${maxy}`; // ⬅ limit YOXDUR

    fetch(url, { headers: { 'Accept':'application/json' } })
      .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
      .then(showTekuis)
      .catch(err => console.error('TEKUİS BBOX error:', err));
  }

  async function fetchTekuisByAttachTicket(){
    const pageTicket = getPageTicketSafe();
    if (!pageTicket) return;
    window.TekuisSwitch?.setMode?.('live');
    try{
      const resp = await fetch(`/api/tekuis/parcels/by-attach-ticket/?ticket=${encodeURIComponent(pageTicket)}`, {
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
    if (window.tekuisCache?.hasTekuisCache) return window.tekuisCache.hasTekuisCache();
    const pageTicket = getPageTicketSafe();
    const key = pageTicket ? `tekuis_fc_${pageTicket}` : 'tekuis_fc_global';
    try { return !!localStorage.getItem(key); } catch { return false; }
  }

  function clearTekuisCache(){
    if (window.tekuisCache?.clearTekuisCache) {
      window.tekuisCache.clearTekuisCache();
      return;
    }
    const pageTicket = getPageTicketSafe();
    const key = pageTicket ? `tekuis_fc_${pageTicket}` : 'tekuis_fc_global';
    try { localStorage.removeItem(key); } catch {}
  }

  function fetchTekuisByGeomForLayer(layer){
    const { wkt, bufferMeters } = window.composeLayerWKTAndSuggestBuffer?.(layer) || { wkt: null, bufferMeters: 0 };
    if (!wkt){
      // Heç nə formalaşmadısa — son çarə BBOX
      return fetchTekuisByBboxForLayer(layer);
    }
    window.TekuisSwitch?.setMode?.('live');
    return fetch('/api/tekuis/parcels/by-geom/', {
      method: 'POST',
      headers: { 'Content-Type':'application/json', 'Accept':'application/json' },
      body: JSON.stringify({ wkt, srid: 4326, buffer_m: bufferMeters }) // ⬅ limit YOXDUR
    })
    .then(r => r.ok ? r.json() : Promise.reject(r.statusText))
    .then(fc => showTekuis(fc))
    .catch(err => console.error('TEKUİS GEOM error:', err));
  }

  function refreshTekuisFromAttachIfAny(force=false){
    if (!force && tekuisHasCache()) {
      // Kəsilmiş / redaktə olunmuş TEKUİS LS-dədir – üstələməyək
      return Promise.resolve();
    }
    const attachLayerSource = getAttachLayerSourceSafe();
    const n = attachLayerSource?.getFeatures?.()?.length || 0;
    if (n > 0){
      const attachLayer = getAttachLayerSafe();
      return attachLayer ? fetchTekuisByGeomForLayer(attachLayer) : Promise.resolve();
    } else {
      const tekuisSource = getTekuisSourceSafe();
      const tekuisLayer = getTekuisLayerSafe();
      tekuisSource?.clear(true);
      setTekuisCountSafe(0);
      const lbl = document.getElementById('lblTekuisCount');
      if (lbl) lbl.textContent = '(0)';
      if (document.getElementById('cardTekuis')){
        safeApplyNoDataCardState('cardTekuis', true, TEXT_TEKUIS_EMPTY, TEXT_TEKUIS_DEFAULT);
      }
      const chk = document.getElementById('chkTekuisLayer');
      if (chk) chk.checked = false;
      tekuisLayer?.setVisible(false);
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
      const necasSource = getNecasSourceSafe();
      const necasLayer = getNecasLayerSafe();
      if (!necasSource) return;
      necasSource.clear(true);
      necasSource.addFeatures(feats);

      setNecasCountSafe(feats.length);

      if (document.getElementById('cardNecas')){
        safeApplyNoDataCardState('cardNecas', getNecasCountSafe() === 0, TEXT_NECAS_EMPTY, TEXT_NECAS_DEFAULT);
      }

      const chk = document.getElementById('chkNecasLayer');
      if (getNecasCountSafe() === 0){
        necasLayer?.setVisible(false);
        if (chk) chk.checked = false;
      } else if (chk){
        necasLayer?.setVisible(chk.checked);
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
    const { wkt, bufferMeters } = window.composeLayerWKTAndSuggestBuffer?.(layer) || { wkt: null, bufferMeters: 0 };
    if (!wkt) return fetchNecasByBboxForLayer(layer);

    return fetch('/api/necas/parcels/by-geom/', {
      method: 'POST',
      headers: { 'Content-Type':'application/json','Accept':'application/json' },
      body: JSON.stringify({ wkt, srid: 4326, buffer_m: bufferMeters })
    })
    .then(async r => {
      if (!r.ok) {
        const txt = await r.text();
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
    const attachLayerSource = getAttachLayerSourceSafe();
    const n = attachLayerSource?.getFeatures?.()?.length || 0;
    if (n > 0){
      const attachLayer = getAttachLayerSafe();
      return attachLayer ? fetchNecasByGeomForLayer(attachLayer) : Promise.resolve();
    } else {
      const necasSource = getNecasSourceSafe();
      const necasLayer = getNecasLayerSafe();
      necasSource?.clear(true);
      setNecasCountSafe(0);
      if (document.getElementById('cardNecas')){
        safeApplyNoDataCardState('cardNecas', true, TEXT_NECAS_EMPTY, TEXT_NECAS_DEFAULT);
      }
      const chk = document.getElementById('chkNecasLayer');
      if (chk) chk.checked = false;
      necasLayer?.setVisible(false);
      return Promise.resolve();
    }
  }

  return {
    fetchTekuisByAttachTicket,
    fetchTekuisByBboxForLayer,
    refreshTekuisFromAttachIfAny,
    refreshNecasFromAttachIfAny,
    clearTekuisCache,
    tekuisHasCache
  };
};