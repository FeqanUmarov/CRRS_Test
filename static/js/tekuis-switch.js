// tekuis-switch.js
// TEKUİS mənbə rejimi: 'live' (cari TEKUİS) | 'db' (PostgreSQL tekuis_parcel, status=1)
(function(){
  let TEKUIS_MODE = 'live';

  function setTekuisMode(mode){
    TEKUIS_MODE = (mode === 'db') ? 'db' : 'live';
    updateTekuisSwitchUI();
  }

  function updateTekuisSwitchUI(){
    const btn = document.getElementById('btnSwitchTekuis');
    const small = document.querySelector('#cardTekuis .small');
    if (!btn || !small) return;

    if (TEKUIS_MODE === 'live'){
      btn.title = 'PostgreSQL (status=1) məlumatlarına keç';
      btn.classList.remove('is-db');
      btn.classList.add('is-live');
      small.textContent = (window.TEXT_TEKUIS_DEFAULT || 'TEKUİS sisteminin parsel məlumatları.') + ' (Mənbə: TEKUİS – canlı)';
    } else {
      btn.title = 'TEKUİS canlı məlumata qayıt';
      btn.classList.remove('is-live');
      btn.classList.add('is-db');
      small.textContent =
        (window.TEXT_TEKUIS_DB_DEFAULT || 'Tədqiqat nəticəsində dəyişiklik eilərək saxlanılan TEKUİS parselləri')
        + ' (Mənbə: DB)';

    }
  }

  function getNextTekuisMode(){
    return TEKUIS_MODE === 'db' ? 'live' : 'db';
  }

  function getRefreshTekuisFromAttach(){
    return window.refreshTekuisFromAttachIfAny
      || window.MainState?.refreshTekuisFromAttachIfAny
      || window.MainState?.layers?.refreshTekuisFromAttachIfAny;
  }


  // Ticket və ya meta_id tapmaq üçün yardımçı funksiya
  function getCurrentIdentifier(){
    // 1. window.CURRENT_META_ID yoxla (əgər sistemdə belə bir dəyişən varsa)
    if (window.CURRENT_META_ID) {
      return { type: 'meta_id', value: window.CURRENT_META_ID };
    }
    // 2. Səhifə meta_id-si varsa, onu istifadə et
    if (typeof window.META_ID !== 'undefined' && window.META_ID !== null && window.META_ID !== '') {
      return { type: 'meta_id', value: window.META_ID };
    }
    // 3. Attach list varsa, onun meta_id-sini götür
    if (window.attachListData && window.attachListData.length > 0) {
      const firstAttach = window.attachListData[0];
      if (firstAttach.meta_id) {
        return { type: 'meta_id', value: firstAttach.meta_id };
      }
    }
    
    // 4. GIS data obyektlərindən meta_id tapmağa çalış
    if (window.gisDataFeatures && window.gisDataFeatures.length > 0) {
      const firstFeature = window.gisDataFeatures[0];
      if (firstFeature.properties && firstFeature.properties.fk_metadata) {
        return { type: 'meta_id', value: firstFeature.properties.fk_metadata };
      }
    }
    
    // 5. Əvvəl window.PAGE_TICKET yoxla
    if (window.PAGE_TICKET) {
      return { type: 'ticket', value: window.PAGE_TICKET };
    }
    
    // 6. URL-dən ticket parametrini yoxla
    const urlParams = new URLSearchParams(window.location.search);
    const ticketFromUrl = urlParams.get('ticket');
    if (ticketFromUrl) {
      return { type: 'ticket', value: ticketFromUrl };
    }
    
    // 7. Form və ya DOM elementlərindən ticket tapmağa çalış
    const ticketInput = document.querySelector('input[name="ticket"]');
    if (ticketInput && ticketInput.value) {
      return { type: 'ticket', value: ticketInput.value };
    }
    
    return null;
  }

  async function fetchTekuisFromDb({ metaId=null } = {}){
    // Əgər parametr verilməyibsə, avtomatik tap
    if (!metaId) {
      const identifier = getCurrentIdentifier();
      if (!identifier) {
        console.warn('TEKUİS DB: ticket və ya meta_id tapılmadı');
        Swal.fire('Diqqət','Bu səhifədə ticket və ya meta_id tapılmadı. Zəhmət olmasa əvvəlcə məlumat yükləyin.','warning');
        return;
      }
      
      // identifier tipinə görə parametr hazırla
      if (identifier.type === 'meta_id') {
        metaId = identifier.value;
      }
    }
    
    const qs = new URLSearchParams();
    if (metaId) {
      qs.set('meta_id', metaId);
    } else {
      const identifier = getCurrentIdentifier();
      if (identifier && identifier.type === 'ticket') {
        qs.set('ticket', identifier.value);
      } else {
        console.error('TEKUİS DB: nə ticket, nə də meta_id tapıldı');
        Swal.fire('Xəta', 'Məlumat identifikatoru tapılmadı.', 'error');
        return;
      }
    }

    const url = `/api/tekuis/parcels/by-db/?${qs.toString()}`;
    console.log('TEKUİS DB sorğusu:', url);
    
    try{
      const resp = await fetch(url, { headers: { 'Accept':'application/json' } });
      if (!resp.ok) throw new Error(await resp.text() || `HTTP ${resp.status}`);
      const fc = await resp.json();
      
      // Nəticəni yoxla
      if (!fc.features || fc.features.length === 0) {
        console.log('TEKUİS DB: heç bir parsel tapılmadı');
        Swal.fire('Məlumat', 'Database-də saxlanılmış parsel tapılmadı.', 'info');
        return;
      }
      
      console.log(`TEKUİS DB: ${fc.features.length} parsel tapıldı`);
      
      // main.js-dən hazır util:
      window.showTekuis && window.showTekuis(fc);
    }catch(e){
      console.error('TEKUİS DB error:', e);
      Swal.fire('Xəta', e.message || 'DB-dən TEKUİS parsellərini almaq alınmadı.', 'error');
    }
  }

  // TEKUİS kartı DOM-a gələndə düyməni yerinə tik
  function ensureSwitchButton(){
    const card = document.getElementById('cardTekuis');
    if (!card) return;

    const actions = card.querySelector('.card-actions');
    if (!actions) return;

    if (!document.getElementById('btnSwitchTekuis')){
      const btn = document.createElement('button');
      btn.id = 'btnSwitchTekuis';
      btn.className = 'icon-btn ico-switch is-live';
      btn.title = 'Mənbəni dəyiş (TEKUİS ↔ DB)';
      actions.prepend(btn);

      btn.addEventListener('click', async ()=>{
        const nextMode = getNextTekuisMode();
        if (nextMode === 'db'){
          // Əvvəlcə identifier olub-olmadığını yoxla
          const identifier = getCurrentIdentifier();
          if (!identifier) {
            Swal.fire('Diqqət', 'Əvvəlcə məlumat yükləyin və ya ticket daxil edin.', 'warning');
            return;
          }
          setTekuisMode('db');
          await fetchTekuisFromDb();
        } else {
          setTekuisMode('live');
          // canlı TEKUİS-ə qayıdış: qoşma geometriyasına görə çək (force=true)
                    const refreshTekuis = getRefreshTekuisFromAttach();
          if (refreshTekuis) {
            await refreshTekuis(true);
          } else {
            console.warn('TEKUİS canlı yeniləmə funksiyası tapılmadı');
          }
        }

        const chk = document.getElementById('chkTekuisLayer');
        const tekuisVisible = chk ? chk.checked : true;
        if (window.tekuisLayer && window.tekuisSource){
          window.tekuisLayer.setVisible(tekuisVisible && window.tekuisSource.getFeatures().length > 0);
          if (tekuisVisible && window.flashLayer) window.flashLayer(window.tekuisLayer);
        }
      });

      updateTekuisSwitchUI();
    }
  }

  // Panel render olduqca izləyək
  const mo = new MutationObserver(() => ensureSwitchButton());
  window.addEventListener('DOMContentLoaded', () => {
    ensureSwitchButton();
    const panelBody = document.querySelector('.panel-body') || document.body;
    mo.observe(panelBody, { childList:true, subtree:true });
  });
  
  // Global funksiyaları export et (debug üçün)
  window.TekuisSwitch = {
    setMode: setTekuisMode,
    getMode: () => TEKUIS_MODE,
    fetchFromDb: fetchTekuisFromDb,
    getCurrentIdentifier: getCurrentIdentifier
  };
})();