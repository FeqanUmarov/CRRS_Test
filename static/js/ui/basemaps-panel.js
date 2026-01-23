function setupBasemapsPanel({ openPanel, panelBodyEl, basemapApi } = {}){
  function renderBasemapsPanel(){
    const thumbs = [
      { key:'google',          title:'Imagery',          img:'https://mt1.google.com/vt/lyrs=s&x=18&y=12&z=5' },
      { key:'imagery_hybrid',  title:'Imagery Hybrid',   img:'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/5/12/18' },
      { key:'streets',         title:'Streets',          img:'https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/5/12/18' },
      { key:'osm',             title:'OSM',              img:'https://tile.openstreetmap.org/5/17/11.png' },
      { key:'streets_night',   title:'Streets (Night)',  img:'https://a.basemaps.cartocdn.com/dark_all/5/17/11.png' },
      { key:'topographic',     title:'Topographic',      img:'https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/5/12/18' },
      { key:'navigation',      title:'Navigation',       img:'https://services.arcgisonline.com/ArcGIS/rest/services/Specialty/World_Navigation_Charts/MapServer/tile/5/12/18' }
    ];
    const html = `
      <div class="card">
        <div class="upload-title" style="margin-bottom:10px;">Basemaps</div>
        <div class="basemap-grid" style="display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:12px;padding:8px;">
          ${thumbs.map(t => `
            <div class="basemap-item" data-key="${t.key}"
                 style="border:2px solid transparent;border-radius:5px;overflow:hidden;cursor:pointer;background:#f3f4f6;">
              <img src="${t.img}" alt="${t.title}" style="width:100%;height:110px;object-fit:cover;display:block;" />
              <div class="bm-title" style="padding:8px 10px;font-size:13px;color:#111827;">${t.title}</div>
            </div>
          `).join('')}
        </div>
      </div>
    `;
    openPanel?.('Basemaps', html);
    panelBodyEl?.querySelectorAll?.('.basemap-item').forEach(el=>{
      el.addEventListener('click', ()=> basemapApi?.setBasemap?.(el.dataset.key));
    });
    basemapApi?.highlightSelectedBasemap?.();
  }

  return { renderBasemapsPanel };
}

window.setupBasemapsPanel = setupBasemapsPanel;