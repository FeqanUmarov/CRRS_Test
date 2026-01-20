/* TEKUİS → Tədqiqat daxilini kəsib sil və Tədqiqat obyektini TEKUİS layına əlavə et (frontend-only)
   Quraşdırma:
   - Bu faylı index.html-də main.js-dən SONRA və DEFER ilə qoşun:
     <script src="{% static 'js/tekuis_erase.js' %}?v=3" defer></script>
*/

(function(){
  'use strict';

  /* ------------------------------
   * Util-lər
   * ------------------------------ */
  const byId = (id)=>document.getElementById(id);

  // turf dinamiki
  function ensureTurf(){
    return new Promise((resolve, reject)=>{
      if (window.turf) return resolve(window.turf);
      const s = document.createElement('script');
      s.src = 'https://cdn.jsdelivr.net/npm/@turf/turf@6/turf.min.js';
      s.async = true;
      s.onload = ()=>resolve(window.turf);
      s.onerror = ()=>reject(new Error('turf.js yüklənmədi'));
      document.head.appendChild(s);
    });
  }

  // xəritə hazır olana kimi gözlə
  function waitForMap(timeoutMs=10000, intervalMs=120){
    return new Promise((resolve, reject)=>{
      const t0 = Date.now();
      const timer = setInterval(()=>{
        if (window.ol && window.map && typeof map.getLayers === 'function'){
          clearInterval(timer); resolve(map);
        } else if (Date.now() - t0 > timeoutMs){
          clearInterval(timer); reject(new Error('Xəritə (map) hazır deyil.'));
        }
      }, intervalMs);
    });
  }

  /* ------------------------------
   * Layları xəritədən tapmaq
   * ------------------------------ */
  function _flattenLayers(groupOrCollection){
    try{
      const arr = groupOrCollection.getArray
        ? groupOrCollection.getArray()
        : groupOrCollection.getLayers
          ? groupOrCollection.getLayers().getArray()
          : [];
      return arr.flatMap(l => (l instanceof ol.layer.Group) ? _flattenLayers(l.getLayers()) : [l]);
    }catch(_){ return []; }
  }
  function _allVectorLayers(){
    try{
      const all = _flattenLayers(map.getLayers());
      return all.filter(l => l instanceof ol.layer.Vector);
    }catch(_){ return []; }
  }
  function _findVectorByTitleFragments(fragments){
    const frags = (fragments||[]).map(s=>String(s).toLowerCase());
    const layers = _allVectorLayers();
    return layers.find(l=>{
      const title = (l.get && (l.get('title')||l.get('name'))) || '';
      const low = String(title).toLowerCase();
      return frags.some(f => low.includes(f));
    }) || null;
  }

  // TEKUİS layını təxmin et
  function _guessTekuisLayer(){
    try{ if (window.tekuisLayer instanceof ol.layer.Vector) return window.tekuisLayer; }catch(_){}
    let lyr = _findVectorByTitleFragments(['tekuis','parsel','parcel','m_g_parsel']);
    if (lyr) return lyr;

    // polygonlu və ən çox feature-li
    const cands = _allVectorLayers().filter(l=>{
      const src = l.getSource && l.getSource();
      const feats = (src && src.getFeatures && src.getFeatures()) || [];
      if (!feats.length) return false;
      const g = feats[0].getGeometry && feats[0].getGeometry();
      return !!g && /Polygon/i.test(g.getType());
    });
    if (!cands.length) return null;
    cands.sort((a,b)=> (b.getSource().getFeatures().length) - (a.getSource().getFeatures().length));
    return cands[0] || null;
  }

  // Tədqiqat layını təxmin et
  function _guessTicketLayer(){
    try{ if (window.ticketLayer instanceof ol.layer.Vector) return window.ticketLayer; }catch(_){}
    let lyr = _findVectorByTitleFragments(['tədqiqat','tedqiqat','ticket']);
    if (lyr) return lyr;

    // fk_metadata olan lay
    lyr = _allVectorLayers().find(l=>{
      const src = l.getSource && l.getSource();
      const feats = (src && src.getFeatures && src.getFeatures()) || [];
      return feats.some(f => f && (f.get('fk_metadata') != null || f.get('FK_METADATA') != null));
    });
    if (lyr) return lyr;

    // zIndex = 5
    lyr = _allVectorLayers().find(l=> (l.getZIndex && l.getZIndex()) === 5);
    if (lyr) return lyr;

    // fallback: polygonlu, TEKUİS deyil
    const tekuis = _guessTekuisLayer();
    const cands = _allVectorLayers().filter(l=>{
      if (tekuis && l === tekuis) return false;
      const src = l.getSource && l.getSource();
      const feats = (src && src.getFeatures && src.getFeatures()) || [];
      if (!feats.length) return false;
      const g = feats[0].getGeometry && feats[0].getGeometry();
      return !!g && /Polygon/i.test(g.getType());
    });
    if (!cands.length) return null;
    cands.sort((a,b)=> (b.getSource().getFeatures().length) - (a.getSource().getFeatures().length));
    return cands[0] || null;
  }

  function _getTekuisSource(){
    try{ if (window.tekuisSource && typeof window.tekuisSource.getFeatures === 'function') return window.tekuisSource; }catch(_){}
    const lyr = _guessTekuisLayer();
    return (lyr && lyr.getSource) ? lyr.getSource() : null;
  }
  function _getTicketSource(){
    try{ if (window.ticketLayerSource && typeof window.ticketLayerSource.getFeatures === 'function') return window.ticketLayerSource; }catch(_){}
    const lyr = _guessTicketLayer();
    return (lyr && lyr.getSource) ? lyr.getSource() : null;
  }

  // Seçilmiş poliqonlar varsa onları, yoxdursa bütün Tədqiqat poliqonlarını götür
  function _getTicketTurfPolys(gjFmt){
    const polys = [];
    const originalFeatures = []; // Orijinal feature-ları saxlayacağıq
    
    console.log('_getTicketTurfPolys çağırıldı');
    
    try{
      if (typeof getSelectedPolygons === 'function'){
        console.log('getSelectedPolygons funksiyası mövcuddur');
        const sel = getSelectedPolygons() || [];
        console.log('Seçilmiş poliqonlar:', sel.length);
        sel.forEach((f, idx)=>{
          const g = gjFmt.writeFeatureObject(f, {
            dataProjection: 'EPSG:4326',
            featureProjection: 'EPSG:3857'
          }).geometry;
          if (g && (g.type==='Polygon' || g.type==='MultiPolygon')) {
            polys.push(g);
            originalFeatures.push(f);
            console.log(`Seçilmiş feature ${idx} əlavə edildi`);
          }
        });
        if (polys.length) {
          console.log(`Seçilmiş ${polys.length} poliqon, ${originalFeatures.length} feature`);
          return { polys, originalFeatures };
        }
      }
    }catch(err){
      console.warn('getSelectedPolygons xətası:', err);
    }

    const src = _getTicketSource();
    console.log('Ticket source:', !!src);
    if (!src) return { polys, originalFeatures };
    
    const allFeats = src.getFeatures() || [];
    console.log(`Ticket layında cəmi ${allFeats.length} feature`);
    
    allFeats.forEach((f, idx)=>{
      const g = gjFmt.writeFeatureObject(f, {
        dataProjection: 'EPSG:4326',
        featureProjection: 'EPSG:3857'
      }).geometry;
      if (g && (g.type==='Polygon' || g.type==='MultiPolygon')) {
        polys.push(g);
        originalFeatures.push(f);
        console.log(`Ticket feature ${idx} əlavə edildi`);
      }
    });
    
    console.log(`Final: ${polys.length} poliqon, ${originalFeatures.length} feature`);
    return { polys, originalFeatures };
  }

  // Turf → OL geometry (Feature gəlirsə, içindəki geometry-ni çıxar)
  function _olGeometryFromTurf(gjFmt, turfOut){
    if (!turfOut) return null;
    let geom = turfOut;
    if (geom.type === 'Feature') geom = geom.geometry; // ƏSAS DÜZƏLİŞ
    if (!geom) return null;
    return gjFmt.readGeometry(geom, {
      dataProjection: 'EPSG:4326',
      featureProjection: 'EPSG:3857'
    });
  }

  // Tədqiqat obyektlərini TEKUİS layına əlavə et
  function _addTicketFeaturesToTekuis(tekuisSrc, ticketFeatures, gjFmt) {
    console.log('_addTicketFeaturesToTekuis çağırıldı:', {
      tekuisSrc: !!tekuisSrc,
      ticketFeatures: ticketFeatures?.length || 0,
      gjFmt: !!gjFmt
    });
    
    if (!tekuisSrc || !ticketFeatures || !ticketFeatures.length) {
      console.warn('TEKUİS source və ya ticket features yoxdur');
      return 0;
    }
    
    let added = 0;
    ticketFeatures.forEach((originalFeature, index) => {
      try {
        console.log(`Feature ${index} emal edilir:`, originalFeature);
        
        // Orijinal feature-ın bütün xüsusiyyətlərini kopyala
        const newFeature = originalFeature.clone();
        console.log('Clone edildi:', newFeature);
        
        // TEKUİS layına aid olduğunu göstərmək üçün xüsusi işarə əlavə et
        newFeature.set('_from_tedqiqat', true);
        newFeature.set('_transfer_timestamp', Date.now());
        console.log('Xüsusiyyətlər əlavə edildi');
        
        // Feature-ı TEKUİS layına əlavə et
        tekuisSrc.addFeature(newFeature);
        console.log('Feature TEKUİS layına əlavə edildi');
        added++;
      } catch (err) {
        console.error(`Feature ${index} əlavə edilərkən xəta:`, err, originalFeature);
      }
    });
    
    console.log(`Cəmi ${added} feature əlavə edildi`);
    return added;
  }

  /* ------------------------------
   * Əsas əməliyyat
   * ------------------------------ */
  async function runEraseFlow(){
    try{
      const gjFmt = new ol.format.GeoJSON();
      const tekuisSrc = _getTekuisSource();
      const ticketResult = _getTicketTurfPolys(gjFmt);
      const ticketPolys = ticketResult.polys;
      const ticketFeatures = ticketResult.originalFeatures;

      if (!tekuisSrc){
        (window.Swal ? Swal.fire('Diqqət','TEKUİS layı tapılmadı.','info') : alert('TEKUİS layı tapılmadı.'));
        return;
      }
      if (!ticketPolys.length){
        (window.Swal ? Swal.fire('Diqqət','Tədqiqat poliqonu seçilməyib və ya mövcud deyil.','info') : alert('Tədqiqat poliqonu yoxdur.'));
        return;
      }
      if (tekuisSrc.getFeatures().length === 0){
        (window.Swal ? Swal.fire('Diqqət','TEKUİS layında obyekt yoxdur.','info') : alert('TEKUİS boşdur.'));
        return;
      }

      // təsdiq
      let proceed = true;
      if (window.Swal){
        const ask = await Swal.fire({
          title: 'Əminsiniz?',
          html: 'Tədqiqat sərhədləri daxilində qalan <b>TEKUİS hissələri kəsilib silinəcək</b> və <b>Tədqiqat obyektləri TEKUİS layına əlavə ediləcək</b> (yalnız bu sessiyada).',
          icon: 'warning',
          showCancelButton: true,
          confirmButtonText: 'Bəli, icra et',
          cancelButtonText: 'İmtina'
        });
        proceed = ask.isConfirmed;
      } else {
        proceed = confirm('Tədqiqat daxilində qalan TEKUİS hissələrini kəsib silmək və Tədqiqat obyektlərini TEKUİS layına əlavə etmək istəyirsiniz?');
      }
      if (!proceed) return;

      const turf = await ensureTurf();

      // maskanı (Tədqiqat) birləşdir
      let eraseGeom = ticketPolys[0];
      for (let i=1; i<ticketPolys.length; i++){
        try { eraseGeom = turf.union(eraseGeom, ticketPolys[i]); }
        catch(e){
          eraseGeom = turf.buffer(turf.union(turf.buffer(eraseGeom,0), turf.buffer(ticketPolys[i],0)),0);
        }
      }

      // TEKUİS → difference
      const feats = tekuisSrc.getFeatures().slice();
      let removed = 0, modified = 0, skipped = 0;

      for (const f of feats){
        // Əgər bu feature əvvəlcədən Tədqiqatdan gəlibsə, onu toxunulmazlığa götür
        if (f.get('_from_tedqiqat')) continue;

        const g = gjFmt.writeFeatureObject(f, {
          dataProjection: 'EPSG:4326',
          featureProjection: 'EPSG:3857'
        }).geometry;

        if (!g || (g.type!=='Polygon' && g.type!=='MultiPolygon')) { skipped++; continue; }

        // kəsişmə?
        let isect = false;
        try { isect = turf.booleanIntersects(g, eraseGeom); }
        catch(_){
          try { isect = turf.booleanIntersects(turf.buffer(g,0), turf.buffer(eraseGeom,0)); }
          catch(__){ isect = false; }
        }
        if (!isect) continue;

        // fərq
        let diff = null;
        try { diff = turf.difference(g, eraseGeom); }
        catch(_){
          try { diff = turf.difference(turf.buffer(g,0), turf.buffer(eraseGeom,0)); }
          catch(__){ diff = null; }
        }

        // diff null-dursa: tam içəridə qalıb → sil
        if (!diff){
          tekuisSrc.removeFeature(f);
          removed++;
          continue;
        }

        // Turf Feature/Geometry → OL Geometry
        const newOlGeom = _olGeometryFromTurf(gjFmt, diff);

        // bəzən fərq nəticəsi boş koordinatlı ola bilər
        const isEmpty =
          !newOlGeom ||
          (newOlGeom.getType && /Polygon|MultiPolygon/.test(newOlGeom.getType()) &&
           (!newOlGeom.getCoordinates || !newOlGeom.getCoordinates().length));

        if (isEmpty){
          tekuisSrc.removeFeature(f);
          removed++;
        } else {
          f.setGeometry(newOlGeom);
          modified++;
        }
      }

      // Tədqiqat obyektlərini TEKUİS layına əlavə et
      console.log('Tədqiqat obyektlərini TEKUİS layına əlavə etmək üçün hazırlanır...');
      console.log('ticketFeatures:', ticketFeatures?.length || 0);
      console.log('tekuisSrc:', !!tekuisSrc);
      
      const addedCount = _addTicketFeaturesToTekuis(tekuisSrc, ticketFeatures, gjFmt);
      console.log('Əlavə edilən obyekt sayı:', addedCount);

      // UI
      const remain = tekuisSrc.getFeatures().length;
      if (typeof window.tekuisCount !== 'undefined') window.tekuisCount = remain;

      if (typeof window.applyNoDataCardState === 'function'){
        const empty = remain === 0;
        const TEXT_TEKUIS_EMPTY = (window.TEXT_TEKUIS_EMPTY || 'TEKUİS məlumat bazasında heç bir məlumat tapılmadı.');
        const TEXT_TEKUIS_DEFAULT = (window.TEXT_TEKUIS_DEFAULT || '');
        window.applyNoDataCardState('cardTekuis', empty, TEXT_TEKUIS_EMPTY, TEXT_TEKUIS_DEFAULT);
      }

      const chk = byId('chkTekuisLayer');
      if (chk && remain === 0) chk.checked = false;
      if (window.tekuisLayer){
        window.tekuisLayer.setVisible(remain > 0 && (!chk || chk.checked));
        if (typeof window.flashLayer === 'function' && remain > 0){
          window.flashLayer(window.tekuisLayer);
        }
      }

      // --- Maskanı WKT kimi çıxart (backend üçün hazır) ---
      let maskWkt = null;
      try {
        const maskOlGeom = _olGeometryFromTurf(gjFmt, eraseGeom); // EPSG:3857
        if (maskOlGeom) {
          const g4326 = maskOlGeom.clone().transform('EPSG:3857','EPSG:4326');
          maskWkt = new ol.format.WKT().writeGeometry(g4326, { decimals: 8 });
        }
      } catch(_){ /* ignore */ }

      // Son nəticəni yadda saxla (persist üçün lazım olacaq)
      const _result = {
        maskWkt,
        stats: { modified, removed, skipped, remain, added: addedCount },
        ts: Date.now()
      };
      window._TEKUIS_ERASE_LAST = _result;

      const msg = `Kəsildi: ${modified}, Tam silindi: ${removed}${skipped?`, Ötüldü: ${skipped}`:''}. Əlavə edildi: ${addedCount}. Qalan: ${remain}.`;
      (window.Swal ? Swal.fire('Hazırdır', msg, 'success') : alert(msg));

    }catch(err){
      console.error(err);
      (window.Swal ? Swal.fire('Xəta', err?.message || 'Əməliyyat alınmadı.', 'error') : alert('Xəta: '+(err?.message || err)));
    }
  }

  /* ------------------------------
   * Publik API + Deleqasiya
   * ------------------------------ */
  // Gələcəkdə backend saxlanması üçün minimal API
  window.TEKUIS_ERASE = {
    run: (opts={}) => runEraseFlow(opts),
    last: () => window._TEKUIS_ERASE_LAST || null,
    // Stub: backend hazır olanda bu hissəni aktiv edəcəksiniz
    persist: async function({ ticket=window.PAGE_TICKET } = {}){
      const last = this.last();
      if (!last || !last.maskWkt) {
        window.Swal?.fire('Info','Yadda saxlanacaq mask tapılmadı. Əvvəl kəsmə əməliyyatını edin.','info');
        return { ok:false };
      }
      // const resp = await fetch('/api/tekuis/erase-inside/', {
      //   method: 'POST',
      //   headers: { 'Content-Type':'application/json','Accept':'application/json' },
      //   body: JSON.stringify({ ticket, mask_wkt: last.maskWkt })
      // });
      // if (!resp.ok) throw new Error(await resp.text() || `HTTP ${resp.status}`);
      // return { ok:true, data: await resp.json() };
      return { ok:true, data:null }; // hələlik frontend-only
    }
  };

  // Deleqasiya: kartdakı və (əgər varsa) sağ alətlərdəki silmə düymələri
  document.addEventListener('click', async (e) => {
    const btn = e.target.closest('#btnEraseTekuisInsideTicket, #rtErase');
    if (!btn) return;
    e.preventDefault();
    try {
      await waitForMap();
      await window.TEKUIS_ERASE.run({ persist:false }); // hələlik yalnız front
    } catch (err) {
      console.error(err);
      window.Swal?.fire('Xəta', err?.message || 'Əməliyyat alınmadı.','error');
    }
  }, true);

  /* ------------------------------
   * Sağ toolbar-a Erase düyməsi
   * ------------------------------ */
  function _injectRightToolsButton(){
    const host = byId('rightTools');
    if (!host || byId('rtErase')) return;
    const b = document.createElement('button');
    b.id = 'rtErase';
    b.className = 'rt-btn';
    b.title = 'TEKUİS → Tədqiqat daxilini kəs və sil, Tədqiqat obyektlərini əlavə et';
    b.innerHTML = `
      <svg class="rt-icon" viewBox="0 0 24 24" fill="none" aria-hidden="true">
        <path d="M4 7l8 8M12 7L4 15" stroke="#ef4444" stroke-width="2" stroke-linecap="round"/>
        <circle cx="16.5" cy="7.5" r="2.5" stroke="#ef4444" stroke-width="2"/>
        <circle cx="16.5" cy="16.5" r="2.5" stroke="#ef4444" stroke-width="2"/>
        <path d="M8 16l4-4" stroke="#22c55e" stroke-width="2" stroke-linecap="round"/>
      </svg>`;
    host.appendChild(b);
  }

  // Başlat
  document.addEventListener('DOMContentLoaded', async ()=>{
    try{
      await waitForMap();
      _injectRightToolsButton(); // sağ paneldə görünməsi opsionaldır
    }catch(err){
      console.warn('Map gözlənərkən problem:', err.message);
    }
  });

})();