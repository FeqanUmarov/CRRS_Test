function setupLayersPanel({
  openPanel,
  map,
  pageTicket,
  getCSRFToken,
  flashLayer,
  getVisFlag,
  setVisFlag,
  loadTicketLayer,
  loadAttachLayer,
  refreshTekuisFromAttachIfAny,
  refreshNecasFromAttachIfAny,
  clearTekuisCache,
  tryValidateAndSaveTekuis,
  getTicketLayer,
  getTicketLayerSource,
  getTicketLayerCount,
  getAttachLayer,
  getAttachLayerSource,
  getAttachLayerCount,
  getTekuisLayer,
  getTekuisSource,
  getTekuisCount,
  setTekuisCount,
  getNecasLayer,
  getNecasSource,
  getNecasCount,
  setNecasCount
} = {}){
  function setCardDisabled(cardId, disabled){
    const card = document.getElementById(cardId);
    if (!card) return;
    card.style.opacity = disabled ? '0.55' : '1';
    card.style.filter  = disabled ? 'grayscale(10%)' : 'none';
    card.querySelectorAll('input, button').forEach(el => { el.disabled = !!disabled; });
    const chk = card.querySelector('input[type="checkbox"]');
    if (chk && disabled) chk.checked = false;
  }

  function updateTicketDeleteState(){
    const delBtn = document.getElementById('btnDeleteTicket');
    if (!delBtn) return;

    const showByStatus = (window.CURRENT_STATUS_ID === 2 || window.CURRENT_STATUS_ID === 99);
    // Yalnız STATUS_ID 2 və 99 olduqda göstər
    delBtn.style.display = showByStatus ? '' : 'none';
    if (!showByStatus) return;

    // Görünəndə də icazəyə görə aktiv/deaktiv et
    const allowed = (window.CURRENT_STATUS_ID === 2 || window.CURRENT_STATUS_ID === 99);
    delBtn.disabled = !allowed;
    delBtn.title = allowed ? '' : 'Bu əməliyyat yalnız STATUS 2 və ya 99 üçün mümkündür.';
  }

  function applyNoDataCardState(cardId, isEmpty, emptyText, defaultText){
    const card = document.getElementById(cardId);
    if (!card) return;
    const small = card.querySelector('.small');

    // setCardDisabled-dən gələn solğunluğu ləğv edirik
    card.style.opacity = '1';
    card.style.filter  = 'none';

    if (isEmpty){
      // yumşaq qırmızı fon + nazik sərhəd
      card.style.background = 'rgba(239,68,68,0.06)';           // red-500 ~ 6% opacity
      card.style.border     = '1px solid rgba(239,68,68,0.25)'; // nazik qırmızı sərhəd
      card.style.boxShadow  = 'inset 0 0 0 1px rgba(239,68,68,0.10)';

      if (small) small.textContent = emptyText || '';

      // idarələri söndür və checkbox-ı söndürülmüş vəziyyətə gətir
      card.querySelectorAll('input, button').forEach(el=>{
        if (el.type === 'checkbox') el.checked = false;
        el.disabled = true;
      });
    } else {
      // normal hala qaytar
      card.style.background = '';
      card.style.border     = '';
      card.style.boxShadow  = '';
      if (small && defaultText) small.textContent = defaultText;

      card.querySelectorAll('input, button').forEach(el=>{ el.disabled = false; });
    }
  }

  async function tekuisExistsForTicket(ticket){
    if (!ticket) return false;
    try {
      const resp = await fetch(`/api/tekuis/exists?ticket=${encodeURIComponent(ticket)}`, {
        headers: { 'Accept': 'application/json' }
      });
      if (!resp.ok) return false;
      const data = await resp.json();
      return !!data?.exists;
    } catch (e) {
      console.warn('TEKUİS exists check failed:', e);
      return false;
    }
  }

  async function loadSavedTekuisFromDbIfExists(){
    if (!pageTicket || window.tekuisCache?.hasTekuisCache?.()) return false;
    if (!window.TekuisSwitch?.showSource) return false;

    const exists = await tekuisExistsForTicket(pageTicket);
    if (!exists) return false;

    window.tekuisNecasApi?.markTekuisSaved?.(true);

    setVisFlag?.('tekuis', true);
    const chkTekuis = document.getElementById('chkTekuisLayer');
    if (chkTekuis) chkTekuis.checked = true;

    const tekuisLayer = getTekuisLayer?.();
    const tekuisSource = getTekuisSource?.();
    const count = tekuisSource?.getFeatures?.().length ?? 0;
    tekuisLayer?.setVisible(count > 0);
    return true;
  }


  function renderLayersPanel(){
    const canDelete = (window.CURRENT_STATUS_ID === 2 || window.CURRENT_STATUS_ID === 99);

    const html = `
    <div class="card layer-card" id="cardTicket">
      <div class="card-head">
        <div class="layer-left">
          <div class="checkbox-wrapper-18">
            <div class="round">
              <input type="checkbox" id="chkTicketLayer" checked />
              <label for="chkTicketLayer"></label>
            </div>
          </div>
          <label class="layer-title" for="chkTicketLayer">
            <span>Tədqiqat layı</span>
          </label>
        </div>
        <div class="card-actions">
          <button id="btnDeleteTicket" class="icon-btn ico-delete" title="Ləğv et" disabled></button>
          <button id="btnZoomTicket"   class="icon-btn ico-zoom zoombtn"   title="Laya yaxınlaşdır"></button>
        </div>
      </div>
      <div class="small" style="color:#6b7280;">Bu lay istifadəçi tərəfidən yadda saxlanılan <b>torpaq sahələrini</b> göstərir.</div>
    </div>

    <div style="height:10px;"></div>

    <div class="card layer-card" id="cardAttach">
      <div class="card-head">
        <div class="layer-left">
          <div class="checkbox-wrapper-18">
            <div class="round">
              <input type="checkbox" id="chkAttachLayer"/>
              <label for="chkAttachLayer"></label>
            </div>
          </div>
          <label class="layer-title" for="chkAttachLayer">
            <span>Qoşma lay</span>
          </label>
        </div>

        <div class="card-actions">
          <button id="btnZoomAttach" class="icon-btn ico-zoom zoombtn" title="Laya yaxınlaşdır"></button>
        </div>
      </div>
      <div class="small" style="color:#6b7280;">Bu lay istifadəçi tərəfidən əlavə edilən qoşa məlumatlarını göstərir</div>
    </div>

    <div style="height:10px;"></div>

    <div class="card layer-card" id="cardTekuis">
      <div class="card-head">
        <div class="layer-left">
          <div class="checkbox-wrapper-18">
            <div class="round">
              <input type="checkbox" id="chkTekuisLayer" />
              <label for="chkTekuisLayer"></label>
            </div>
          </div>
          <label class="layer-title" for="chkTekuisLayer">
            <span>TEKUİS Parsellər</span>
          </label>
        </div>

        <div class="card-actions">
          <button id="btnSaveTekuis"               class="icon-btn ico-save" title="TEKUİS parsellərini yadda saxla"></button>
          <button id="btnEraseTekuisInsideTicket"  class="icon-btn ico-erase" title="Tədqiqat daxilini kəs & sil"></button>
          <button id="btnZoomTekuis"               class="icon-btn ico-zoom zoombtn"  title="Laya yaxınlaşdır"></button>
        </div>
      </div>
      <div class="small" style="color:#6b7280;">Tədqiqat nəticəsində dəyişdirilmiş TEKUİS Parselləri.</div>
    </div>

    <div style="height:10px;"></div>

    <div class="card layer-card" id="cardNecas">
      <div class="card-head">
        <div class="layer-left">
          <div class="checkbox-wrapper-18">
            <div class="round">
              <input type="checkbox" id="chkNecasLayer" />
              <label for="chkNecasLayer"></label>
            </div>
          </div>
          <label class="layer-title" for="chkNecasLayer">
            <span>NECAS Parsellər</span>
          </label>
        </div>

        <div class="card-actions">
          <button id="btnZoomNecas" class="icon-btn ico-zoom zoombtn" title="Laya yaxınlaşdır"></button>
        </div>
      </div>
      <div class="small" style="color:#6b7280;">NECAS sistemində qeydiyyatdan keçmiş parsellər.</div>
    </div>
  `;

    openPanel?.('Laylar', html);

    const ticketLayerCount = getTicketLayerCount?.() ?? 0;
    const attachLayerCount = getAttachLayerCount?.() ?? 0;
    const tekuisSource = getTekuisSource?.();
    const necasSource = getNecasSource?.();
    const tekuisCount = getTekuisCount?.() ?? tekuisSource?.getFeatures?.().length ?? 0;
    const necasCount = getNecasCount?.() ?? necasSource?.getFeatures?.().length ?? 0;

    setTekuisCount?.(tekuisCount);
    setNecasCount?.(necasCount);


    setCardDisabled('cardTicket', ticketLayerCount === 0);
    setCardDisabled('cardAttach', attachLayerCount === 0);

    updateTicketDeleteState();

    // === Ticket (LS ilə) ===
    const chkTicket = document.getElementById('chkTicketLayer');
    const btnZoomTicket = document.getElementById('btnZoomTicket');

    // === TEKUİS: "Yadda saxla" → əvvəl topologiya, sonra save
    const btnSaveTekuis = document.getElementById('btnSaveTekuis');
    if (btnSaveTekuis) {
      btnSaveTekuis.addEventListener('click', async () => {
        // ✅ Attributes panel sinxi
        try {
          if (window.AttributesPanel && typeof window.AttributesPanel.applyUIToSelectedFeature === 'function') {
            window.AttributesPanel.applyUIToSelectedFeature();
          }
        } catch (e) {
          console.warn('Attributes sync:', e);
        }

        await tryValidateAndSaveTekuis?.();
      });
    }

    (async () => {
      const alreadyLoaded = !!getTicketLayerSource?.();
      if (!alreadyLoaded) await loadTicketLayer?.({ fit:false });
      // init from LS
      const visTicket = getVisFlag?.('ticket', true);
      chkTicket.checked = visTicket;
      const ticketLayer = getTicketLayer?.();
      if (ticketLayer) ticketLayer.setVisible(chkTicket.checked);
      updateTicketDeleteState();
    })();

    chkTicket.addEventListener('change', () => {
      const ticketLayer = getTicketLayer?.();
      if (ticketLayer) ticketLayer.setVisible(chkTicket.checked);
      setVisFlag?.('ticket', chkTicket.checked);
      if (chkTicket.checked) flashLayer?.(ticketLayer);
    });

    btnZoomTicket.addEventListener('click', () => {
      const ticketLayerSource = getTicketLayerSource?.();
      if (ticketLayerSource && ticketLayerSource.getFeatures().length > 0){
        const ext = ticketLayerSource.getExtent();
        map.getView().fit(ext, { padding:[20,20,20,20], duration:600, maxZoom:18 });
      } else {
        Swal.fire('Info','Zoom ediləcək obyekt yoxdur.','info');
      }
    });

    if (canDelete) {
      document.getElementById('btnDeleteTicket').addEventListener('click', async () => {
        const allowed = (window.CURRENT_STATUS_ID === 2 || window.CURRENT_STATUS_ID === 99);

        const currentTicketLayerCount = getTicketLayerCount?.() ?? 0;
        const hasTicketData = (currentTicketLayerCount > 0);

        if (!hasTicketData) {
          Swal.fire('Info','Tədqiqat layında ləğv ediləcək obyekt yoxdur.','info');
          return;
        }
        if (!allowed) {
          Swal.fire('Diqqət','Bu əməliyyat yalnız 99 statusu və ya redaktə (qaralama) rejimində mümkündür.','info');
          return;
        }

        const ask = await Swal.fire({
          title: 'Əminsiniz?',
          html: 'Bütün məlumatlar ləğv ediləcək.',
          icon: 'warning',
          showCancelButton: true,
          confirmButtonText: 'Ləğv et',
          cancelButtonText: 'İmtina et'
        });
        if (!ask.isConfirmed) return;

        try {
          const resp = await fetch(`/api/layers/soft-delete-by-ticket/?ticket=${encodeURIComponent(pageTicket || '')}`, {
            method: 'POST',
            headers: {
              'Accept': 'application/json',
              'X-CSRFToken': getCSRFToken?.()
            },
            credentials: 'include'
          });

          if (!resp.ok) throw new Error((await resp.text()) || `HTTP ${resp.status}`);
          const data = await resp.json();

          // Layları yenilə
          await loadTicketLayer?.({ fit:false });
          await loadAttachLayer?.({ fit:false });

          // UI/state təmizləmələri
          updateTicketDeleteState();
          clearTekuisCache?.();
          await refreshTekuisFromAttachIfAny?.(true); // attach boşdursa TEKUİS də təmizlənəcək
          const necasSource = getNecasSource?.();
          necasSource?.clear(true);
          setNecasCount?.(0);

          // Nəticəni göstər (serverdən gələn saylara uyğun)
          const info = [
            (data?.meta_id != null ? `meta_id: <b>${data.meta_id}</b>` : ''),
            (data?.affected_parcel != null ? `TEKUİS (parcel): <b>${data.affected_parcel}</b>` : ''),
            (data?.affected_gis != null ? `GIS data: <b>${data.affected_gis}</b>` : ''),
            (data?.affected_attach != null ? `Attach: <b>${data.affected_attach}</b>` : ''),
            (data?.objectid_nullified ? `TBL_REQUEST_REG.OBJECTID <b>NULL</b> edildi` : '')
          ].filter(Boolean).join('<br>');

          Swal.fire('OK','Ləğv etmə əməliyyatı tamamlandı','success');

          // Paneli təzələ (kartların aktivlik vəziyyəti yenilənsin)
          renderLayersPanel();
        } catch (e) {
          console.error(e);
          Swal.fire('Xəta', e.message || 'Soft delete əməliyyatı alınmadı.','error');
        }
      });
    }

    // === Attach (LS ilə) ===
    const chkAttach  = document.getElementById('chkAttachLayer');
    const btnZoomA   = document.getElementById('btnZoomAttach');

    (async () => {
      await loadAttachLayer?.({ fit:false });
      // init from LS
      const visAttach = getVisFlag?.('attach', true);
      chkAttach.checked = visAttach;
      const attachLayer = getAttachLayer?.();
      if (attachLayer) attachLayer.setVisible(chkAttach.checked);

      const currentAttachCount = getAttachLayerCount?.() ?? 0;
      const tekuisLoadedFromDb = await loadSavedTekuisFromDbIfExists();
      if (currentAttachCount > 0) {
        if (!tekuisLoadedFromDb) {
          await refreshTekuisFromAttachIfAny?.();
        }
        await refreshNecasFromAttachIfAny?.();
      } else if (!tekuisLoadedFromDb) {
        getTekuisSource?.()?.clear(true);
        getNecasSource?.()?.clear(true);
        setTekuisCount?.(0);
        setNecasCount?.(0);
        if (document.getElementById('cardTekuis')) setCardDisabled('cardTekuis', true);
        if (document.getElementById('cardNecas'))  setCardDisabled('cardNecas',  true);
      }
    })();

    chkAttach.addEventListener('change', async () => {
      const attachLayer = getAttachLayer?.();
      if (attachLayer) attachLayer.setVisible(chkAttach.checked);
      setVisFlag?.('attach', chkAttach.checked);

      // >>> IMPORTANT: Qoşma layı sadəcə görünən/gizli edirik.
      // TEKUİS lokalda kəsilibsə, onu üstələməyək – heç nə fetch etmirik.
      // Əgər həqiqətən yeniləmək istəsəniz, başqa yerdə refreshTekuisFromAttachIfAny(true) çağıracağıq.

      if (chkAttach.checked) flashLayer?.(attachLayer);
    });

    btnZoomA.addEventListener('click', () => {
      const attachLayerSource = getAttachLayerSource?.();
      if (attachLayerSource && attachLayerSource.getFeatures().length > 0){
        const ext = attachLayerSource.getExtent();
        map.getView().fit(ext, { padding:[20,20,20,20], duration:600, maxZoom:18 });
      } else {
        Swal.fire('Info','Zoom ediləcək attach obyekti yoxdur.','info');
      }
    });

    // === TEKUİS (LS ilə) ===
    const chkT  = document.getElementById('chkTekuisLayer');
    const btnZT = document.getElementById('btnZoomTekuis');
    const tekuisLayer = getTekuisLayer?.();


    // init from LS
    const visTekuis = getVisFlag?.('tekuis', false);
    chkT.checked = visTekuis;
    const tekuisFeatureCount = tekuisSource?.getFeatures?.().length ?? tekuisCount ?? 0;
    tekuisLayer?.setVisible(chkT.checked && tekuisFeatureCount > 0);


    chkT.addEventListener('change', () => {
      setVisFlag?.('tekuis', chkT.checked);
      const count = tekuisSource?.getFeatures?.().length ?? tekuisCount ?? 0;
      tekuisLayer?.setVisible(chkT.checked && count > 0);
      if (chkT.checked) flashLayer?.(tekuisLayer);
    });

    if (btnZT){
      btnZT.addEventListener('click', () => {
        if (tekuisSource && tekuisSource.getFeatures().length > 0){
          const ext = tekuisSource.getExtent();
          map.getView().fit(ext, { padding:[20,20,20,20], duration:600, maxZoom:18 });
        } else {
          Swal.fire('Info','Zoom ediləcək TEKUİS obyekti yoxdur.','info');
        }
      });
    }

    // === NECAS (LS ilə) ===
    const chkN  = document.getElementById('chkNecasLayer');
    const btnZN = document.getElementById('btnZoomNecas');
    const necasLayer = getNecasLayer?.();


    // init from LS
    const visNecas = getVisFlag?.('necas', false);
    chkN.checked = visNecas;
    const necasFeatureCount = necasSource?.getFeatures?.().length ?? necasCount ?? 0;
    necasLayer?.setVisible(chkN.checked && necasFeatureCount > 0);

    chkN.addEventListener('change', () => {
      setVisFlag?.('necas', chkN.checked);
      const count = necasSource?.getFeatures?.().length ?? necasCount ?? 0;
      necasLayer?.setVisible(chkN.checked && count > 0);
      if (chkN.checked) flashLayer?.(necasLayer);
    });

    if (btnZN){
      btnZN.addEventListener('click', () => {
        if (necasSource && necasSource.getFeatures().length > 0){
          const ext = necasSource.getExtent();
          map.getView().fit(ext, { padding:[20,20,20,20], duration:600, maxZoom:18 });
        } else {
          Swal.fire('Info','Zoom ediləcək NECAS obyekti yoxdur.','info');
        }
      });
    }

    // Qoşma lay checkbox wiring (təhlükəsizlik üçün panel renderdən sonra da)
    (function syncAttachCheckbox(){
      const chkAttach = document.getElementById('chkAttachLayer');
      if (!chkAttach) return;

      const vis = getVisFlag?.('attach', false);
      chkAttach.checked = vis;
      const attachLayer = getAttachLayer?.();
      if (attachLayer) attachLayer.setVisible(vis);

      if (!chkAttach._wired) {
        chkAttach.addEventListener('change', (e) => {
          const on = !!e.target.checked;
          setVisFlag?.('attach', on);
          const attachLayer = getAttachLayer?.();
          if (attachLayer) attachLayer.setVisible(on);
        });
        chkAttach._wired = true;
      }
    })();
  }

  return {
    setCardDisabled,
    updateTicketDeleteState,
    applyNoDataCardState,
    renderLayersPanel
  };
}

window.setupLayersPanel = setupLayersPanel;