function setupDataPanel({ openPanel, panelBodyEl, uploadHandlers } = {}){
  function renderDataPanel(){
    if (!window.EDIT_ALLOWED) {
      Swal.fire('Diqqət', 'Bu əməliyyatları yalnız redaktə və ya qaralama rejimində edə bilərsiz!', 'info');
      return;
    }
    const html = `
      <div class="tabs">
        <div class="tab active" data-tab="shp">Shapefile (.zip)</div>
        <div class="tab" data-tab="pts">Koordinatlar (.csv/.txt)</div>
      </div>
      <div id="tabContent"></div>
    `;
    openPanel?.('Məlumat daxil et', html);
    const tabContent = document.getElementById('tabContent');
    const tabs = panelBodyEl?.querySelectorAll?.('.tab') || [];

    const loadTab = (which)=>{
      tabs.forEach(t => t.classList.toggle('active', t.dataset.tab === which));
      if (which === 'shp') {
        tabContent.innerHTML = `
          <div class="card">
            <div class="upload-box" id="uploadBoxShp">
              <div class="upload-title">Shapefile arxivi (.zip)</div>
              <div class="hint">Arxivdə .shp, .shx, .dbf (və varsa .prj) olmalıdır</div>
              <input type="file" id="shpArchiveInput" accept=".zip,.rar" hidden />
              <button id="chooseArchiveBtn" class="btn">Arxiv seç və yüklə</button>
              <div class="filename" id="archiveName"></div>
            </div>
          </div>
        `;
        const input   = document.getElementById('shpArchiveInput');
        const choose  = document.getElementById('chooseArchiveBtn');
        const box     = document.getElementById('uploadBoxShp');
        const nameLbl = document.getElementById('archiveName');
        const pick = (file)=>{
          if (!file) return;
          const low = file.name.toLowerCase();
          if (!(low.endsWith('.zip') || low.endsWith('.rar'))) {
            Swal.fire('Xəta', 'Zəhmət olmasa .zip və ya .rar shapefile arxivi seçin.', 'error');
            return;
          }
          if (nameLbl) nameLbl.textContent = file.name;
          uploadHandlers?.uploadArchiveToBackend?.(file);
        };
        choose?.addEventListener('click', () => input?.click());
        input?.addEventListener('change', e => pick(e.target.files?.[0]));
        box?.addEventListener('dragover', e => { e.preventDefault(); box.classList.add('drag'); });
        box?.addEventListener('dragleave', () => box.classList.remove('drag'));
        box?.addEventListener('drop', e => { e.preventDefault(); box.classList.remove('drag'); pick(e.dataTransfer.files?.[0]); });
      } else {
        tabContent.innerHTML = `
          <div class="card">
            <div class="upload-title">Koordinatlar (.csv / .txt)</div>
            <div class="small">CSV üçün ayırıcı avtomatik tanınır (<code>,</code> <code>;</code> <code>\\t</code> və s.). Başlıq yoxdursa ilk iki sütun X,Y kimi qəbul ediləcək.</div>
            <div class="form-row">
              <div class="radio-group" id="crsRadios">
                <label class="radio"><input type="radio" name="crs" value="wgs84" checked> WGS84 (lon/lat)</label>
                <label class="radio"><input type="radio" name="crs" value="utm38"> UTM 38N</label>
                <label class="radio"><input type="radio" name="crs" value="utm39"> UTM 39N</label>
              </div>
            </div>
            <div class="upload-box" id="uploadBoxCsv" style="margin-top:10px;">
              <input type="file" id="pointsFileInput" accept=".csv,.txt" hidden />
              <button id="choosePointsBtn" class="btn">Fayl seç və yüklə</button>
              <div class="filename" id="pointsFileName"></div>
            </div>
          </div>
        `;
        const input   = document.getElementById('pointsFileInput');
        const choose  = document.getElementById('choosePointsBtn');
        const box     = document.getElementById('uploadBoxCsv');
        const nameLbl = document.getElementById('pointsFileName');
        const pick = (file)=>{
          if (!file) return;
          const low = file.name.toLowerCase();
          if (!(low.endsWith('.csv') || low.endsWith('.txt'))) {
            Swal.fire('Xəta', 'Zəhmət olmasa .csv və ya .txt faylı seçin.', 'error');
            return;
          }
          if (nameLbl) nameLbl.textContent = file.name;
          const crs = (document.querySelector('input[name="crs"]:checked')?.value) || 'wgs84';
          uploadHandlers?.uploadPointsToBackend?.(file, crs);
        };
        choose?.addEventListener('click', () => input?.click());
        input?.addEventListener('change', e => pick(e.target.files?.[0]));
        box?.addEventListener('dragover', e => { e.preventDefault(); box.classList.add('drag'); });
        box?.addEventListener('dragleave', () => box.classList.remove('drag'));
        box?.addEventListener('drop', e => { e.preventDefault(); box.classList.remove('drag'); pick(e.dataTransfer.files?.[0]); });
      }
    };
    tabs.forEach(t => t.addEventListener('click', ()=> loadTab(t.dataset.tab)));
    loadTab('shp');

    const btnSave = document.getElementById('btnSaveDataPanel');
    btnSave?.addEventListener('click', () => window.saveSelected?.({ alsoAttach:true }));
    window.updateAllSaveButtons?.();
  }

  return { renderDataPanel };
}

window.setupDataPanel = setupDataPanel;