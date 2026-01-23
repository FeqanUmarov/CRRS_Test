function setupTekuisCache({
  pageTicket,
  tekuisSource,
  selectAny,
  getFeatureOwner,
  onCountChange
} = {}){
  const LS_KEYS = {
    vis: 'map_layer_visibility_v1',
    tekuis: (pageTicket ? `tekuis_fc_${pageTicket}` : 'tekuis_fc_global')
  };

  const geojsonFmt = new ol.format.GeoJSON();

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

  function saveTekuisToLS(){
    try{
      const feats = tekuisSource?.getFeatures?.() || [];
      const fcObj = geojsonFmt.writeFeaturesObject(feats, {
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
      const feats = geojsonFmt.readFeatures(fcObj, {
        dataProjection: 'EPSG:4326',
        featureProjection: 'EPSG:3857'
      });
      try {
        const selA = selectAny?.getFeatures?.();
        const arr  = selA?.getArray?.().slice() || [];
        arr.forEach(f => {
          if (getFeatureOwner?.(f) === tekuisSource) {
            selA.remove(f);
          }
        });
      } catch {}

      tekuisSource?.clear?.(true);
      tekuisSource?.addFeatures?.(feats);
      const nextCount = feats.length;
      onCountChange?.(nextCount);
      return nextCount > 0;
    }catch{ return false; }
  }

  return {
    readVis,
    writeVis,
    setVisFlag,
    getVisFlag,
    saveTekuisToLS,
    loadTekuisFromLS
  };
}

window.setupTekuisCache = setupTekuisCache;