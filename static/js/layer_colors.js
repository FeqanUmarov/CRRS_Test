// static/js/layer_colors.js
(function () {
  /* ==============================
     UI STİLİ (rəng çipi + gizli picker)
     ============================== */
  const css = `
    .layer-color-chip{
      display:inline-flex; width:18px; height:18px;
      margin-left:8px; border:1px solid rgba(0,0,0,.08);
      box-shadow: inset 0 0 0 2px #fff; cursor:pointer; vertical-align:middle;
      transition: transform .08s ease;
    }
    .layer-color-chip:hover{ transform:scale(1.05); }
    .layer-color-wrap{ display:inline-flex; align-items:center; position:relative; }
    /* Gizli input - daha yaxşı üsul ilə gizlədək */
    .layer-color-hidden{
      position:absolute; 
      opacity:0; 
      width:1px; 
      height:1px; 
      pointer-events:none;
      top:0;
      left:0;
    }
  `;
  const style = document.createElement('style');
  style.textContent = css;
  document.head.appendChild(style);

  /* ==============================
     SAXLAMA
     ============================== */
  const LS_KEY = 'layer_colors_v1';
  const DEFAULTS = {
    ticket: '#dbeb00', // Tədqiqat layı (main.js default yaşıl)
    attach: '#26a657', // Qoşma layı (narıncı)
    tekuis: '#7f2ec2', // TEKUİS stroke (main.js-də bu rəngdə idi)
    necas:  '#3b82f6'  // NECAS stroke (main.js-də bu rəngdə idi)
  };
  function readColors(){
    try { return Object.assign({}, DEFAULTS, JSON.parse(localStorage.getItem(LS_KEY) || '{}')); }
    catch { return Object.assign({}, DEFAULTS); }
  }
  function writeColors(obj){
    try { localStorage.setItem(LS_KEY, JSON.stringify(obj || {})); } catch {}
  }
  const COLORS = readColors();

  /* ==============================
     KÖMƏKÇİ FUNKSİYALAR
     ============================== */
  function hex2rgba(hex, alpha){
    const m = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex || '#000000');
    const r = m ? parseInt(m[1], 16) : 0;
    const g = m ? parseInt(m[2], 16) : 0;
    const b = m ? parseInt(m[3], 16) : 0;
    return `rgba(${r},${g},${b},${alpha})`;
  }

  // Lay rənginə görə OpenLayers style qaytarır (Point/Line/Polygon üçün)
  function makeStyleForColor(hex){
    const fillA = 0.25, lineW = 2;
    return function (feature){
      const t = feature.getGeometry().getType();
      if (t === 'Point' || t === 'MultiPoint'){
        return new ol.style.Style({
          image: new ol.style.Circle({
            radius: 5,
            fill:  new ol.style.Fill({ color: hex2rgba(hex, 0.9) }),
            stroke:new ol.style.Stroke({ color: '#ffffff', width: 1.5 })
          })
        });
      }
      if (t === 'LineString' || t === 'MultiLineString'){
        return new ol.style.Style({
          stroke: new ol.style.Stroke({ color: hex, width: lineW })
        });
      }
      // Polygon / MultiPolygon
      return new ol.style.Style({
        fill:   new ol.style.Fill({ color: hex2rgba(hex, fillA) }),
        stroke: new ol.style.Stroke({ color: hex, width: lineW })
      });
    };
  }

  // main.js-də yaradılan laylara çıxış
  const LAYER_GETTERS = {
    ticket: () => window.ticketLayer,
    attach: () => window.attachLayer,
    tekuis: () => window.tekuisLayer,
    necas:  () => window.necasLayer
  };
  
  // Layları qlobal səviyyəyə çıxart (main.js-də istifadə üçün)
  window.ticketLayer = window.ticketLayer || null;
  window.attachLayer = window.attachLayer || null;

  // Rəngi laya tətbiq et + varsa çipi sinxron saxla
  const CHIP_MAP = new Map(); // layerKey -> { chip, picker }
  function applyColorToLayer(key, hex){
    let layer = null;
    
    // Xüsusi hallar: ticket və attach layları main.js-də lokal dəyişəndir
    if (key === 'ticket') {
      // main.js-də ticketLayer dəyişənini tap
      if (typeof map !== 'undefined' && map.getLayers) {
        map.getLayers().forEach(l => {
          if (l.get && l.get('title') === 'Tədqiqat layı') {
            layer = l;
            window.ticketLayer = l; // qlobal et
          }
        });
      }
    } else if (key === 'attach') {
      // main.js-də attachLayer dəyişənini tap
      if (typeof map !== 'undefined' && map.getLayers) {
        map.getLayers().forEach(l => {
          if (l.get && l.get('title') === 'Qoşma lay') {
            layer = l;
            window.attachLayer = l; // qlobal et
          }
        });
      }
    } else {
      // TEKUİS və NECAS üçün adi yol
      const get = LAYER_GETTERS[key];
      layer = get && get();
    }
    
    if (layer) {
      layer.setStyle(makeStyleForColor(hex));
      if (layer.changed) layer.changed();
    }
    const pair = CHIP_MAP.get(key);
    if (pair){
      pair.chip.style.background = hex;
      pair.picker.value = hex;
    }
  }

  function applyAllSaved(){
    Object.keys(LAYER_GETTERS).forEach(k => applyColorToLayer(k, COLORS[k]));
  }

  /* ==============================
     LAY ADININ YANINDA RƏNG ÇİPİ
     ============================== */
function installColorControl(cardId, layerKey, titleText){
  const card = document.getElementById(cardId);
  if (!card) return;
  
  // layer-title div-ini tap
  const layerTitle = card.querySelector('.layer-title');
  if (!layerTitle) return;

  // Eyni control təkrar qurulmasın
  if (layerTitle.querySelector('.layer-color-wrap')) return;

  // Lay adının span elementini tap
  const nameSpan = layerTitle.querySelector('span');
  if (!nameSpan) return;

  const wrap = document.createElement('span');
  wrap.className = 'layer-color-wrap';
  
  // HƏR KART ÜÇÜN FƏRDİ MARGIN
  const margins = {
    'cardTicket': '23px',     // "Tədqiqat layı" üçün
    'cardAttach': '38px',      // "Qoşma lay" üçün
    'cardTekuis': '-2px',      // "TEKUİS Parsellər" üçün
    'cardNecas': '-2px'        // "NECAS Parsellər" üçün
  };
  
  wrap.style.marginLeft = margins[cardId] || '8px'; // Default 8px

  const chip = document.createElement('button');
  chip.type = 'button';
  chip.className = 'layer-color-chip';
  chip.title = titleText || 'Rəngi seç';
  chip.style.background = COLORS[layerKey];

  // Chip klikini idarə et
  chip.addEventListener('click', e => {
    e.stopPropagation();
    e.preventDefault();
    
    // Color picker-i aktivləşdir
    picker.style.opacity = '0.01';
    picker.style.width = '1px';
    picker.style.height = '1px';
    picker.style.pointerEvents = 'auto';
    
    if (typeof picker.showPicker === 'function') {
      try {
        picker.showPicker();
      } catch (err) {
        picker.click();
      }
    } else {
      picker.focus();
      picker.click();
    }
    
    setTimeout(() => {
      picker.style.pointerEvents = 'none';
    }, 100);
  });

  const picker = document.createElement('input');
  picker.type = 'color';
  picker.className = 'layer-color-hidden';
  picker.value = COLORS[layerKey];
  picker.tabIndex = -1;

  const onColor = () => {
    const hex = picker.value;
    COLORS[layerKey] = hex;
    writeColors(COLORS);
    applyColorToLayer(layerKey, hex);
    picker.style.pointerEvents = 'none';
  };
  picker.addEventListener('input', onColor);
  picker.addEventListener('change', onColor);

  wrap.appendChild(chip);
  wrap.appendChild(picker);
  
  // Rəng çipini addan SONRA yerləşdir
  nameSpan.insertAdjacentElement('afterend', wrap);

  CHIP_MAP.set(layerKey, { chip, picker });
}

  // "Laylar" paneli render olunandan dərhal sonra çipləri quraşdır
  function afterLayersPanelRendered(){
    installColorControl('cardTicket', 'ticket', 'Tədqiqat layının rəngi');
    installColorControl('cardAttach', 'attach', 'Qoşma layının rəngi');
    installColorControl('cardTekuis', 'tekuis', 'TEKUİS rəngi');
    installColorControl('cardNecas',  'necas',  'NECAS rəngi');
    // Xəritədə də saxlanmış rəngləri tətbiq et (çiplərlə eyni olsun)
    applyAllSaved();
  }

  /* ==============================
     FUNKSİYA HOOK-LARI (təhlükəsiz)
     ============================== */
  function hookRenderLayersPanel(){
    const orig = window.renderLayersPanel;
    if (typeof orig === 'function' && !orig.__colorHooked){
      const wrapped = function(...args){
        const out = orig.apply(this, args);
        // Panel DOM-a düşdükdən sonra çipləri yerləşdir
        requestAnimationFrame(afterLayersPanelRendered);
        return out;
      };
      wrapped.__colorHooked = true;
      window.renderLayersPanel = wrapped;
    }
  }

  function hookLoadersOnce(){
    // Ticket
    if (typeof window.loadTicketLayer === 'function' && !window.loadTicketLayer.__colorHooked){
      const orig = window.loadTicketLayer;
      const wrapped = async function(...args){
        const r = await orig.apply(this, args);
        applyColorToLayer('ticket', COLORS.ticket);
        return r;
      };
      wrapped.__colorHooked = true;
      window.loadTicketLayer = wrapped;
    }
    // Attach
    if (typeof window.loadAttachLayer === 'function' && !window.loadAttachLayer.__colorHooked){
      const orig = window.loadAttachLayer;
      const wrapped = async function(...args){
        const r = await orig.apply(this, args);
        applyColorToLayer('attach', COLORS.attach);
        return r;
      };
      wrapped.__colorHooked = true;
      window.loadAttachLayer = wrapped;
    }
  }

  // Hook layların yaradılmasını və qlobal et
  function hookLayerCreation(){
    // loadTicketLayer hook
    const origLoadTicket = window.loadTicketLayer;
    if (typeof origLoadTicket === 'function' && !origLoadTicket.__colorHooked2) {
      window.loadTicketLayer = async function(...args) {
        const result = await origLoadTicket.apply(this, args);
        // ticketLayer-i qlobal et və rəng tətbiq et
        if (typeof ticketLayer !== 'undefined' && ticketLayer) {
          window.ticketLayer = ticketLayer;
          applyColorToLayer('ticket', COLORS.ticket);
        }
        return result;
      };
      window.loadTicketLayer.__colorHooked2 = true;
    }
    
    // loadAttachLayer hook
    const origLoadAttach = window.loadAttachLayer;
    if (typeof origLoadAttach === 'function' && !origLoadAttach.__colorHooked2) {
      window.loadAttachLayer = async function(...args) {
        const result = await origLoadAttach.apply(this, args);
        // attachLayer-i qlobal et və rəng tətbiq et
        if (typeof attachLayer !== 'undefined' && attachLayer) {
          window.attachLayer = attachLayer;
          applyColorToLayer('attach', COLORS.attach);
        }
        return result;
      };
      window.loadAttachLayer.__colorHooked2 = true;
    }
  }

  // Əsas: səhifə hazır olanda TEKUİS/NECAS kimi dərhal mövcud laylara da rəngi tətbiq et
  document.addEventListener('DOMContentLoaded', () => {
    // Mövcud olan laylarda rəngləri tətbiq et
    applyAllSaved();
    // Hook-ları qura
    hookRenderLayersPanel();
    hookLoadersOnce();
    hookLayerCreation(); // YENİ
    // Ehtiyat: hook gec gəlsə, bir neçə dəfə yoxla
    setTimeout(hookRenderLayersPanel, 0);
    setTimeout(hookRenderLayersPanel, 300);
    setTimeout(hookLoadersOnce, 0);
    setTimeout(hookLoadersOnce, 300);
    setTimeout(hookLayerCreation, 0); // YENİ
    setTimeout(hookLayerCreation, 300); // YENİ
  });
})();