function initMapOverlays(map){
  if (!map) return null;

  const infoHighlightSource = new ol.source.Vector();
  const infoHighlightLayer = new ol.layer.Vector({
    source: infoHighlightSource,
    zIndex: 99,
    style: (feature) => {
      const t = feature.getGeometry().getType();
      const sky = '#60a5fa';
      if (/Point/i.test(t)) {
        return new ol.style.Style({
          image: new ol.style.Circle({
            radius: 6,
            fill: new ol.style.Fill({ color: 'rgba(96,165,250,0.25)' }),
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
      return [
        new ol.style.Style({ fill: new ol.style.Fill({ color: 'rgba(96,165,250,0.10)' }) }),
        new ol.style.Style({ stroke: new ol.style.Stroke({ color: 'rgba(96,165,250,0.35)', width: 6 }) }),
        new ol.style.Style({ stroke: new ol.style.Stroke({ color: sky, width: 3 }) })
      ];
    }
  });
  infoHighlightLayer.set('infoIgnore', true);
  infoHighlightLayer.set('selectIgnore', true);
  map.addLayer(infoHighlightLayer);

  const topoErrorSource = new ol.source.Vector();
  const topoErrorLayer = new ol.layer.Vector({
    source: topoErrorSource,
    zIndex: 200,
    style: (feature) => {
      const t = feature.getGeometry().getType();
      const red = '#ef4444';
      if (/Point/i.test(t)) {
        return new ol.style.Style({
          image: new ol.style.Circle({
            radius: 6,
            fill: new ol.style.Fill({ color: 'rgba(239,68,68,0.12)' }),
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
      return [
        new ol.style.Style({ fill: new ol.style.Fill({ color: 'rgba(239,68,68,0.08)' }) }),
        new ol.style.Style({ stroke: new ol.style.Stroke({ color: 'rgba(239,68,68,0.35)', width: 6 }) }),
        new ol.style.Style({ stroke: new ol.style.Stroke({ color: red, width: 3 }) })
      ];
    }
  });
  topoErrorLayer.set('infoIgnore', true);
  topoErrorLayer.set('selectIgnore', true);
  map.addLayer(topoErrorLayer);

  function setInfoHighlight(feature){
    infoHighlightSource.clear(true);
    if (!feature || !feature.getGeometry) return;
    const f = new ol.Feature({ geometry: feature.getGeometry().clone() });
    infoHighlightSource.addFeature(f);
  }

  return {
    infoHighlightSource,
    topoErrorSource,
    setInfoHighlight
  };
}

window.initMapOverlays = initMapOverlays;