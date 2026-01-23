(() => {
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
  window.map = map;

  const basemapApi = window.setupBasemaps?.(map, googleImagery);
  window.basemapApi = basemapApi;

  const mapOverlays = window.mapOverlays || window.initMapOverlays?.(map);
  window.mapOverlays = mapOverlays;

  // === Feature ownership map (feature → source) ===
  const trackFeatureOwnership = window.FeatureOwnership?.trackFeatureOwnership;

  /* =========================
     TEKUİS (M_G_PARSEL) LAY
     ========================= */
  const tekuisSource = new ol.source.Vector();
  trackFeatureOwnership?.(tekuisSource);
  window.setupTekuisSave?.({ tekuisSource, ticket: window.PAGE_TICKET || '' });

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
  trackFeatureOwnership?.(necasSource);
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
  const infoHighlightSource = mapOverlays?.infoHighlightSource;
  const topoErrorSource = mapOverlays?.topoErrorSource;

  // Xətaları xəritədə qırmızı layda göstərən helper
  function renderTopoErrorsOnMap(validation){
    try{
      topoErrorSource?.clear(true);
      if (!validation) return;
      const gj = new ol.format.GeoJSON();
      const add = (arr=[]) => arr.forEach(it=>{
        if (!it?.geom) return;
        const g = gj.readGeometry(it.geom, {
          dataProjection: 'EPSG:4326',
          featureProjection: 'EPSG:3857'
        });
        topoErrorSource?.addFeature(new ol.Feature({ geometry: g }));
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
    mapOverlays?.setInfoHighlight?.(feature);
  }

  window.MapContext = {
    basemapApi,
    infoHighlightSource,
    map,
    mapOverlays,
    necasLayer,
    necasSource,
    renderTopoErrorsOnMap,
    setInfoHighlight,
    tekuisLayer,
    tekuisSource,
    topoErrorSource,
    topoFocusLayer,
    topoFocusSource,
    zoomAndHighlightTopoGeometry,
    pulseTopoHighlight
  };
})();