// static/js/rtLoading.js
;(() => {
  const STYLE_ID = 'rt-loading-style';
  const HOST_ID  = 'rt-loading-host';

  function ensureStyle() {
    if (document.getElementById(STYLE_ID)) return;
    const css = `
      .rtld-overlay{position:fixed;inset:0;background:rgba(17,24,39,.35);backdrop-filter:blur(2px);
        display:flex;align-items:center;justify-content:center;z-index:100000;opacity:0;transition:opacity .15s}
      .rtld-box{min-width:260px;max-width:420px;background:#111827;color:#f9fafb;padding:16px 18px;border-radius:12px;
        box-shadow:0 20px 45px rgba(0,0,0,.35);display:flex;gap:12px;align-items:center}
      .rtld-spinner{width:26px;height:26px;border-radius:50%;border:3px solid rgba(255,255,255,.25);
        border-top-color:#60a5fa;animation:rtld-spin .75s linear infinite}
      .rtld-title{font:500 14px/1.35 system-ui,-apple-system,Segoe UI,Roboto}
      @keyframes rtld-spin{to{transform:rotate(360deg)}}
    `;
    const st = document.createElement('style');
    st.id = STYLE_ID; st.textContent = css;
    document.head.appendChild(st);
  }

  function ensureHost() {
    let host = document.getElementById(HOST_ID);
    if (host) return host;
    host = document.createElement('div');
    host.id = HOST_ID;
    host.className = 'rtld-overlay';
    host.style.pointerEvents = 'none'; // göstərərkən aktiv edəcəyik
    host.innerHTML = `
      <div class="rtld-box">
        <div class="rtld-spinner" aria-hidden="true"></div>
        <div class="rtld-title" id="rtld-text">Emal olunur…</div>
      </div>
    `;
    host.hidden = true;
    document.body.appendChild(host);
    return host;
  }

  let depth = 0;

  function show(text='Emal olunur…') {
    ensureStyle();
    const host = ensureHost();
    const label = host.querySelector('#rtld-text');
    if (label) label.textContent = text;

    depth++;
    if (depth === 1) {
      host.hidden = false;
      host.style.pointerEvents = 'auto';
      requestAnimationFrame(()=> host.style.opacity = '1');
    }

    // rahat finally üçün: hide funksiyasını geri qaytarırıq
    let called = false;
    return function hide() {
      if (called) return;
      called = true;
      depth = Math.max(0, depth - 1);
      if (depth === 0) {
        const h = ensureHost();
        h.style.opacity = '0';
        h.style.pointerEvents = 'none';
        setTimeout(()=> { h.hidden = true; }, 160);
      }
    };
  }

  function set(text='Emal olunur…') {
    const host = ensureHost();
    const label = host.querySelector('#rtld-text');
    if (label) label.textContent = text;
  }

  // Global API
  window.RTLoading = { show, set };
})();
