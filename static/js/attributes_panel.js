// attributes_panel.js
(function () {
  // asılılıqlar: map, tekuisLayer, openPanel(title, html), closePanel? (istifadə olunmur), window.APP.ticket
  if (!window.map) return;

  const STATE = {
    options: null,            // serverdən gələn select dəyərləri (cache)
    selectedFeature: null,    // seçilmiş TEKUİS feature
    readOnly: false           // save olunubsa true
  };
  const PAGE_TICKET = (window.PAGE_TICKET || (window.APP && window.APP.ticket) || '');

  // Panel sabitləri
  const PANEL_TITLE = 'Atribut məlumatları';
  const PANEL_BODY_SELECTOR = '#side-panel .panel-body';

    // Sol paneldə "Attributes" düyməsini aktiv edən helper
  function setAttributesTabActive() {
    try {
      const btn = document.querySelector('.tool-btn[data-panel="attributes"]');
      if (!btn) return;

      // Bütün sol panel düymələrindən "active" klassını sil
      document.querySelectorAll('.tool-btn').forEach(b => b.classList.remove('active'));

      // Attributes düyməsini aktiv et
      btn.classList.add('active');

      // Sağdakı indikator xəttini də bu düymənin yanına çək
      if (typeof moveIndicatorToButton === 'function') {
        moveIndicatorToButton(btn);
      }
    } catch (e) {
      console.warn('Attributes tab aktivləşdirmə xətası:', e);
    }
  }


  // ---- Stil (select/input, radius=5px) ----
  ensureStyles();
  function ensureStyles() {
    if (document.getElementById('attr-panel-styles')) return;
    const css = `
      #side-panel .attr-panel .k{ color:#111827; font-weight:500; align-self:center; }
      #side-panel .attr-panel select,
      #side-panel .attr-panel input[type="text"],
      #side-panel .attr-panel input[type="number"]{
        width:100%; box-sizing:border-box; padding:6px 10px;
        border:1px solid #d1d5db; border-radius:5px; background:#fff;
        font:14px/1.35 system-ui,Segoe UI,Roboto; outline:none;
        transition:border-color .15s ease, box-shadow .15s ease; appearance:none;
      }
      #side-panel .attr-panel select{
        padding-right:28px;
        background-image:url("data:image/svg+xml,%3Csvg width='12' height='8' viewBox='0 0 12 8' xmlns='http://www.w3.org/2000/svg'%3E%3Cpath d='M1 1.5l5 5 5-5' stroke='%23888' stroke-width='1.5' fill='none' stroke-linecap='round'/%3E%3C/svg%3E");
        background-repeat:no-repeat; background-position:right 8px center;
      }
      #side-panel .attr-panel select:focus,
      #side-panel .attr-panel input:focus{ border-color:#3b82f6; box-shadow:0 0 0 3px rgba(59,130,246,.15); }
      #side-panel .attr-panel select:disabled,
      #side-panel .attr-panel input:disabled{ background:#f9fafb; color:#6b7280; cursor:not-allowed; }
      #side-panel .attr-panel .sep{ grid-column:1/-1; height:1px; background:#e5e7eb; margin:6px 0; }
      #side-panel .attr-panel #attr-readonly-note{
        margin-top:10px; display:none; color:#b45309;
        background:#fffbeb; border:1px solid #fef3c7; padding:8px 10px; border-radius:8px;
      }
      /* Empty state kartı */
      #side-panel .attr-empty{
        display:flex; flex-direction:column; gap:10px; padding:12px;
        border:1px dashed #cbd5e1; border-radius:10px; background:#f8fafc;
        font:14px/1.45 system-ui,Segoe UI,Roboto; color:#0f172a;
      }
      #side-panel .attr-empty .title{
        font-weight:600; color:#0f172a;
      }
      #side-panel .attr-empty .hint{
        font-size:13px; color:#475569;
      }
    `;
    const style = document.createElement('style');
    style.id = 'attr-panel-styles';
    style.textContent = css;
    document.head.appendChild(style);
  }

  // TEKUİS properties adı → options açarı
  const FIELD_MAP = {
    LAND_CATEGORY2ENUM: "uqodiya",
    LAND_CATEGORY_ENUM: "kateqoriya",
    OWNER_TYPE_ENUM:    "mulkiyyet",
    SUVARILMA_NOVU_ENUM:"suvarma",
    EMLAK_NOVU_ENUM:    "emlak",
    OLD_LAND_CATEGORY2ENUM: "uqodiya",
    LAND_CATEGORY3ENUM: "alt_kateqoriya",
    LAND_CATEGORY4ENUM: "alt_uqodiya"
  };

  // ---- Panelin boş görünüşü (seçim YOXDUR) ----
  function renderEmptyState() {
    const html = `
      <div class="attr-empty">
        <div class="title">TEKUİS Parsellər layına aid olan hər hansı bir obyektə klik edin</div>
        <div class="hint">Xəritədə TEKUİS Parseli seçdikdə atribut sahələri burada görünəcək.</div>
      </div>
    `;
    openPanel(PANEL_TITLE, html);
    setAttributesTabActive();
  }

  // ---- Panelin forma görünüşü (seçim VAR) ----
  function renderForm() {
    const html = `
      <div class="attr-panel" style="display:grid;grid-template-columns:180px 1fr;gap:10px 12px;font:14px/1.35 system-ui,Segoe UI,Roboto">
        ${renderRow('LAND_CATEGORY_ENUM','Kateqoriya')}
        ${renderRow('LAND_CATEGORY2ENUM','Uqodiya')}
        ${renderRow('LAND_CATEGORY3ENUM','Alt Kateqoriya')}
        ${renderRow('LAND_CATEGORY4ENUM','Alt Uqodiya')}
        ${renderRow('OWNER_TYPE_ENUM','Mülkiyyət')}
        ${renderRow('SUVARILMA_NOVU_ENUM','Suvarma növü')}
        ${renderRow('EMLAK_NOVU_ENUM','Əmlak növü')}

        <div class="sep"></div>

        ${renderText('NAME','Adı')}
        ${renderText('TERRITORY_NAME','Ərazi')}
        ${renderText('RAYON_ADI','Rayon')}
        ${renderText('IED_ADI','İƏD')}
        ${renderText('BELEDIYE_ADI','Bələdiyyə')}
        ${renderText('AREA_HA','Sahə (ha)', 'number')}
      </div>
      <div id="attr-readonly-note">Bu parsel artıq yadda saxlanıb – atributlar yalnız oxunandır.</div>
    `;
    openPanel(PANEL_TITLE, html);

    setAttributesTabActive();

    // renderdən sonra dəyərləri doldur
    ensureOptions()
      .then(() => {
        fillSelects();
        return resolveReadOnly();
      })
      .then(() => {
        reflectReadOnly();
        attachChangeListeners();
      })
      .catch(() => {});
  }

  // ---- Ümumi render məntiqi ----
  function renderPanel() {
    if (!STATE.selectedFeature) {
      renderEmptyState();
      return;
    }
    renderForm();
  }

  function renderRow(propKey, label) {
    return `
      <label class="k" for="sel-${propKey}">${label}</label>
      <select id="sel-${propKey}" data-prop="${propKey}">
        <option value="">— seçin —</option>
      </select>
    `;
  }
  function renderText(propKey, label, type = 'text') {
    return `
      <label class="k" for="inp-${propKey}">${label}</label>
      <input id="inp-${propKey}" data-prop="${propKey}" type="${type}" />
    `;
  }

  async function ensureOptions() {
    if (STATE.options) return STATE.options;
    const url = '/api/attributes/options/' + (PAGE_TICKET ? ('?ticket=' + encodeURIComponent(PAGE_TICKET)) : '');
    const r = await fetch(url, { headers: { 'Accept': 'application/json' } });
    if (!r.ok) throw new Error(await r.text());
    const j = await r.json();
    STATE.options = (j && j.data) || {};
    return STATE.options;
  }

  function fillSelects() {
    if (!STATE.options || !STATE.selectedFeature) return;

    // options doldur
    Object.entries(FIELD_MAP).forEach(([propKey, optKey]) => {
      const sel = document.getElementById('sel-' + propKey);
      if (!sel) return;
      const list = STATE.options[optKey] || [];
      sel.innerHTML = `<option value="">— seçin —</option>` +
        list.map(it => `<option value="${escapeHtml(it.code)}" data-name="${escapeHtml(it.name)}">${escapeHtml(it.name)}</option>`).join('');
    });

    // seçili dəyər
    const f = STATE.selectedFeature;
    const p = f ? (f.getProperties() || {}) : {};
    const propVal = (k) => (p && typeof p[k] !== 'undefined' && p[k] !== null) ? String(p[k]) : '';

    Object.keys(FIELD_MAP).forEach((k) => {
      const valName = propVal(k);
      const sel = document.getElementById('sel-' + k);
      if (!sel) return;
      if (!valName) { sel.value = ''; return; }
      const opt = Array.from(sel.options).find(o => (o.getAttribute('data-name') || '') === valName);
      sel.value = opt ? opt.value : '';
    });

    // Text inputlar
    ['NAME', 'TERRITORY_NAME', 'RAYON_ADI', 'IED_ADI', 'BELEDIYE_ADI', 'AREA_HA']
      .forEach(k => {
        const el = document.getElementById('inp-' + k);
        if (!el) return;
        el.value = propVal(k);
      });
  }

  // Read-only vəziyyəti (save edilibsə blok)
  async function resolveReadOnly(){
    const TICKET = PAGE_TICKET;
    if (!TICKET) { STATE.readOnly = false; return; }
    try{
      const qs = '?ticket=' + encodeURIComponent(TICKET);
      const r = await fetch('/api/tekuis/exists' + qs, {
        headers: { 'Accept': 'application/json', 'X-Ticket': TICKET }
      });
      if (!r.ok) { STATE.readOnly = false; return; }
      const j = await r.json();
      STATE.readOnly = !!(j && j.exists);
    }catch(e){
      STATE.readOnly = false;
    }
  }

  function reflectReadOnly() {
    const ro = !!STATE.readOnly;
    const note = document.getElementById('attr-readonly-note');
    if (note) note.style.display = ro ? 'block' : 'none';

    document.querySelectorAll('#side-panel .attr-panel select, #side-panel .attr-panel input')
      .forEach(el => { el.disabled = ro; });
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, m => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
  }

  // ✅ YENİ: Real-time UI → Feature sync
  function attachChangeListeners() {
    if (STATE.readOnly) return;
    
    const selectors = '#side-panel .attr-panel select, #side-panel .attr-panel input';
    document.querySelectorAll(selectors).forEach(el => {
      if (el._attrListener) return;
      
      const handler = () => {
        try {
          if (window.AttributesPanel && typeof window.AttributesPanel.applyUIToSelectedFeature === 'function') {
            window.AttributesPanel.applyUIToSelectedFeature();
            if (typeof saveTekuisToLS === 'function') {
              saveTekuisToLS();
            }
          }
        } catch (e) {
          console.warn('Real-time sync xətası:', e);
        }
      };
      
      el.addEventListener('change', handler);
      el.addEventListener('input', handler);
      el._attrListener = true;
    });
  }

  // ---- Xəritə klikləri: TEKUİS seçilərsə formaya yönləndir ----
  map.on('singleclick', async (evt) => {
    try {
      let found = null;
      map.forEachFeatureAtPixel(evt.pixel, (feat, layer) => {
        if (layer && layer === window.tekuisLayer) { found = feat; return true; }
        return false;
      }, { hitTolerance: 5 });

      if (!found) {
        STATE.selectedFeature = null;
        renderPanel();
        return;
      }

      STATE.selectedFeature = found;
      renderPanel();
    } catch (e) {
      console.warn('attributes click error', e);
    }
  });

  // ---- Sidebar düyməsi ----
  const btn = document.querySelector('.tool-btn[data-panel="attributes"]');
  if (btn) {
    btn.addEventListener('click', () => {
      renderPanel();
    });
  }

  // ==== [HELPER FUNKSIYALAR - DAXILI] ====

  function _getSelName(id) {
    const el = document.getElementById(id);
    if (!el) return '';
    const opt = el.options[el.selectedIndex];
    return (opt && (opt.getAttribute('data-name') || opt.textContent || '')).trim();
  }

  function _getInpVal(id) {
    const el = document.getElementById(id);
    return el ? el.value.trim() : '';
  }

  function _buildPropsFromUI() {
    const p0 = (STATE.selectedFeature && STATE.selectedFeature.getProperties()) || {};

    const pick = (uiName, propKey) => _getSelName(uiName) || (p0[propKey] || '');

    return {
      LAND_CATEGORY_ENUM:   pick('sel-LAND_CATEGORY_ENUM',   'LAND_CATEGORY_ENUM'),
      LAND_CATEGORY2ENUM:   pick('sel-LAND_CATEGORY2ENUM',   'LAND_CATEGORY2ENUM'),
      LAND_CATEGORY3ENUM:   pick('sel-LAND_CATEGORY3ENUM',   'LAND_CATEGORY3ENUM'),
      LAND_CATEGORY4ENUM:   pick('sel-LAND_CATEGORY4ENUM',   'LAND_CATEGORY4ENUM'),
      OWNER_TYPE_ENUM:      pick('sel-OWNER_TYPE_ENUM',      'OWNER_TYPE_ENUM'),
      SUVARILMA_NOVU_ENUM:  pick('sel-SUVARILMA_NOVU_ENUM',  'SUVARILMA_NOVU_ENUM'),
      EMLAK_NOVU_ENUM:      pick('sel-EMLAK_NOVU_ENUM',      'EMLAK_NOVU_ENUM'),
      OLD_LAND_CATEGORY2ENUM: p0['OLD_LAND_CATEGORY2ENUM'] || '',

      NAME:           _getInpVal('inp-NAME')          || (p0['NAME'] || ''),
      TERRITORY_NAME: _getInpVal('inp-TERRITORY_NAME')|| (p0['TERRITORY_NAME'] || ''),
      RAYON_ADI:      _getInpVal('inp-RAYON_ADI')     || (p0['RAYON_ADI'] || ''),
      IED_ADI:        _getInpVal('inp-IED_ADI')       || (p0['IED_ADI'] || ''),
      BELEDIYE_ADI:   _getInpVal('inp-BELEDIYE_ADI')  || (p0['BELEDIYE_ADI'] || ''),
      AREA_HA:        _getInpVal('inp-AREA_HA')       || (p0['AREA_HA'] || '')
    };
  }

  function _featureToGeoJSON4326(feat) {
    const fmt = new ol.format.GeoJSON();
    const viewProj = window.map.getView().getProjection() || 'EPSG:3857';
    return fmt.writeFeatureObject(feat, { featureProjection: viewProj, dataProjection: 'EPSG:4326' });
  }

  // ✅ YENİ: Panel dəyişəndə avtomatik sync
  let _lastRenderedPanel = null;
  const _originalOpenPanel = window.openPanel;
  if (typeof _originalOpenPanel === 'function') {
    window.openPanel = function(title, html) {
      if (_lastRenderedPanel === 'attributes' && title !== PANEL_TITLE) {
        try {
          if (window.AttributesPanel && typeof window.AttributesPanel.applyUIToSelectedFeature === 'function') {
            window.AttributesPanel.applyUIToSelectedFeature();
            if (typeof saveTekuisToLS === 'function') {
              saveTekuisToLS();
            }
          }
        } catch (e) {
          console.warn('Panel dəyişərkən attributes sync xətası:', e);
        }
      }
    

      _lastRenderedPanel = (title === PANEL_TITLE) ? 'attributes' : null;
      return _originalOpenPanel(title, html);
    };
  }

  // ==== [EXPORT] Main.js-dən çağırılmaq üçün public API ====
  window.AttributesPanel = window.AttributesPanel || {};

  window.AttributesPanel.getSelectedFeature = function() {
    return STATE.selectedFeature;
  };

  window.AttributesPanel.isReadOnly = function() {
    return STATE.readOnly;
  };

  window.AttributesPanel.applyUIToSelectedFeature = function() {
    if (!STATE.selectedFeature) return false;
    if (STATE.readOnly) return false;

    const props = _buildPropsFromUI();
    const oldProps = STATE.selectedFeature.getProperties() || {};
    const newProps = { ...oldProps, ...props };
    delete newProps.geometry;
    
    STATE.selectedFeature.setProperties(newProps);
    
    console.log('✅ UI məlumatları feature-ə yazıldı:', props);
    return true;
  };


})();