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

  if (polys.length > 0){
    const mp = new ol.geom.MultiPolygon(polys);
    return { wkt: wktWriterLocal.writeGeometry(mp, { decimals: 8 }), bufferMeters: 5 };
  }
  if (lines.length > 0){
    const ml = new ol.geom.MultiLineString(lines);
    return { wkt: wktWriterLocal.writeGeometry(ml, { decimals: 8 }), bufferMeters: 8 };
  }
  if (points.length > 0){
    const mp = new ol.geom.MultiPoint(points);
    return { wkt: wktWriterLocal.writeGeometry(mp, { decimals: 8 }), bufferMeters: 12 };
  }

  return { wkt:null, bufferMeters: 0 };
}

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
    }
  });

  if (multiCoords.length === 0) return null;
  const mp = new ol.geom.MultiPolygon(multiCoords);
  const wktWriterLocal = new ol.format.WKT();
  return wktWriterLocal.writeGeometry(mp, { decimals: 8 });
}

window.composeLayerWKTAndSuggestBuffer = composeLayerWKTAndSuggestBuffer;
window.composeLayerMultiPolygonWKT = composeLayerMultiPolygonWKT;