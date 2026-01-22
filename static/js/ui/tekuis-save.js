function setupTekuisSave({ tekuisSource, ticket } = {}){
  const btnSaveTekuis = document.getElementById('btnSaveTekuis');
  if (!btnSaveTekuis) return;

  async function onSaveTekuisClick(){
    try {
      if (window.AttributesPanel && typeof window.AttributesPanel.applyUIToSelectedFeature === 'function') {
        window.AttributesPanel.applyUIToSelectedFeature();
      }
    } catch (e) {
      console.warn('Attributes sync xətası:', e);
    }

    if (!ticket) { window.showToast?.('Ticket tapılmadı.'); return; }

    const features = tekuisSource?.getFeatures?.();
    if (!features || features.length === 0){
      window.showToast?.('Yadda saxlamaq üçün TEKUİS obyektləri tapılmadı.');
      return;
    }

    const gjFmt = new ol.format.GeoJSON();
    const fc = gjFmt.writeFeaturesObject(features, {
      featureProjection: 'EPSG:3857',
      dataProjection: 'EPSG:4326'
    });

    if (fc.features.length === 1) {
      const t = fc.features[0]?.geometry?.type || '';
      if (t === 'Polygon' || t === 'MultiPolygon'){
        window.showToast?.('Dissolve edilmiş tək (Multi)Polygon göndərilə bilməz. Parselləri ayrı feature kimi saxla.');
        return;
      }
    }

    try{
      const r0 = await fetch(`/api/tekuis/exists?ticket=${encodeURIComponent(ticket)}`, {
        headers: { 'Accept':'application/json', 'X-Ticket': ticket }
      });
      if (r0.ok){
        const j0 = await r0.json();
        if (j0?.exists){
          window.showToast?.('TEKUİS parsellər local bazada yadda saxlanılıb');
          return;
        }
      }
    }catch{ /* şəbəkə xətası olsa da POST-da tutulacaq */ }

    let hideLoading = () => {};
    try{
      hideLoading = (window.RTLoading ? RTLoading.show('Məlumat yadda saxlanır…') : () => {});

      const resp = await fetch('/api/tekuis/save', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-CSRFToken': window.getCSRFToken?.() || '',
          'X-Ticket': ticket,
          'Accept': 'application/json'
        },
        body: JSON.stringify({ ticket, geojson: fc })
      });

      if (resp.status === 409) {
        let msg = 'TEKUİS parsellər local bazada yadda saxlanılıb';
        try { msg = (await resp.json())?.message || msg; } catch {}
        window.showToast?.(msg);
        return;
      }

      if (!resp.ok){
        const txt = await resp.text().catch(()=> '');
        window.showToast?.('Yadda saxlama xətası: ' + (txt || resp.status));
        return;
      }

      const out = await resp.json();
      if (out?.ok){
        window.showToast?.(`Saxlandı: ${out.saved_count || 0} parsel`);
      } else {
        window.showToast?.(out?.error || 'Bilinməyən xəta');
      }

    }catch(e){
      console.warn('Save TEKUIS error:', e);
      window.showToast?.('Şəbəkə xətası');
    }finally{
      try { hideLoading(); } catch {}
    }
  }

  btnSaveTekuis.addEventListener('click', onSaveTekuisClick);
}

window.setupTekuisSave = setupTekuisSave;