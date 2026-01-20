(function () {
  "use strict";

  // =========================
  //  Stil (yalnız bir dəfə əlavə olunur)
  // =========================
  const STYLE_ID = "hm-history-style";
  const ensureStyle = () => {
    if (document.getElementById(STYLE_ID)) return;
    const css = `
      .hm-panel {
        position: fixed;
        top: 12vh;
        left: 50%;
        transform: translateX(-50%);
        width: min(560px, 92vw);
        max-height: 76vh;
        display: flex;
        flex-direction: column;
        background: #fff;
        border: 1px solid rgba(0,0,0,.08);
        border-radius: 5px;
        box-shadow: 0 10px 30px rgba(0,0,0,.15);
        z-index: 9999;
        overflow: hidden;
        user-select: none;
      }
      .hm-hidden { display: none !important; }
      .hm-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
        padding: 10px 14px;
        background: linear-gradient(180deg, #f8fafc, #f3f4f6);
        border-bottom: 1px solid #e5e7eb;
        cursor: move;
      }
      .hm-title {
        font-size: 15px;
        font-weight: 600;
        color: #111827;
        letter-spacing: .2px;
        margin: 0;
        display: flex;
        align-items: center;
        gap: 8px;
      }
      .hm-close {
        appearance: none;
        border: none;
        background: transparent;
        width: 28px;
        height: 28px;
        border-radius: 6px;
        display: grid;
        place-items: center;
        cursor: pointer;
      }
      .hm-close:hover { background: rgba(0,0,0,.06); }
      .hm-body {
        padding: 14px;
        overflow: auto;
      }
      .hm-footer {
        padding: 10px 14px;
        border-top: 1px solid #e5e7eb;
        background: #fafafa;
        display: flex;
        align-items: center;
        justify-content: space-between;
        font-size: 12px;
        color: #6b7280;
      }

      .hm-row {
        display: grid;
        grid-template-columns: 1fr auto;
        gap: 10px;
        align-items: center;
        padding: 10px 12px;
        border: 1px solid #eef2f7;
        border-radius: 10px;
        margin-bottom: 10px;
        background: #fcfcfd;
      }
      .hm-row:last-child { margin-bottom: 0; }

      /* Həm <img>, həm də <svg> üçün eyni ölçü */


      .hm-head {
        display: inline-flex;
        align-items: center;
        gap: 8px;            /* ikonla ad arasında məsafə */
        line-height: 1.2;
      }
      .hm-icon {
        width: 20px;
        height: 20px;
        display: inline-block;
        object-fit: contain;
        vertical-align: middle; /* mətənsətri ilə hizalanma */
      }
      .hm-name {
        font-weight: 600; 
        color: #111827; 
        margin: 0;
      }
      .hm-msg  { font-size: 12px; color: #4b5563; margin-top: 4px; }
      .hm-badge {
        font-size: 12px;
        padding: 4px 8px;
        border-radius: 999px;
        border: 1px solid transparent;
        white-space: nowrap;
      }
      .hm-badge.ok   { color:#065f46; background:#ecfdf5; border-color:#a7f3d0; }
      .hm-badge.warn { color:#92400e; background:#fffbeb; border-color:#fcd34d; }
      .hm-badge.err  { color:#991b1b; background:#fef2f2; border-color:#fecaca; }

      .hm-loading { display:flex; align-items:center; gap:10px; font-size:14px; color:#374151; padding:12px; }
      .hm-spinner { width:16px; height:16px; border-radius:50%; border:2px solid #d1d5db; border-top-color:#111827; animation: hm-spin .8s linear infinite; }
      @keyframes hm-spin { to { transform: rotate(360deg); } }

      .hm-error {
        padding: 12px; border: 1px solid #fecaca; background: #fef2f2;
        color: #7f1d1d; border-radius: 8px; font-size: 13px;
      }

      @media (max-width: 480px) {
        .hm-panel { top: 6vh; }
        .hm-title { font-size: 14px; }
      }
    `;
    const style = document.createElement("style");
    style.id = STYLE_ID;
    style.textContent = css;
    document.head.appendChild(style);
  };

  // =========================
  //  Utility
  // =========================

function formatYMDToDMY(txt) {
    if (!txt) return txt;
    return String(txt).replace(/\b(\d{4})-(\d{2})-(\d{2})\b/g, '$3-$2-$1');
}

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, (m) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
    }[m]));
  }

  function buildMsgHTML(raw) {
    const t = formatYMDToDMY(raw || "");

    // 1) Əgər “İstifadəçi: …” və/və ya “Yaradıldı: …” keçirsə, onları ayrıca sətir kimi qururuq
    const userMatch    = t.match(/İstifadəçi\s*:\s*([^|•\n]+)\s*/i);
    const createdMatch = t.match(/Yaradıldı\s*:\s*([^\n|•]+)/i);
    if (userMatch || createdMatch) {
      const parts = [];
      if (userMatch)    parts.push(`<p class="hm-line"><strong>İstifadəçi:</strong> ${escapeHtml(userMatch[1].trim())}</p>`);
      if (createdMatch) parts.push(`<p class="hm-line"><strong>Yaradıldı:</strong> ${escapeHtml(createdMatch[1].trim())}</p>`);
      return parts.join("");
    }

    // 2) Əks halda “|” və ya “•” ayırıcısını sətirsonuna çevir, sonra HTML-ə uyğunlaşdır
    const normalized = String(t).replace(/\s*(?:\||•)\s*/g, "\n");
    const escaped    = escapeHtml(normalized);
    return escaped.replace(/\n/g, "<br>");
}




  const shortTicket = (t) => {
    if (!t) return "";
    const s = String(t).trim();
    return s.length > 12 ? s.slice(0, 4) + "…" + s.slice(-6) : s;
  };

  // Fallback (ikon təyin edilməyibsə istifadə olunur)
  const fallbackSVGs = {
    attach: `<svg class="hm-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path stroke-width="2" d="M21 15V7a4 4 0 10-8 0v10a3 3 0 01-6 0V9"/></svg>`,
    gis: `<svg class="hm-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path stroke-width="2" d="M3 7l9-4 9 4-9 4-9-4z"/><path stroke-width="2" d="M3 7v10l9 4 9-4V7"/></svg>`,
    tekuis: `<svg class="hm-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path stroke-width="2" d="M4 12l6 6L20 6"/></svg>`,
    default: `<svg class="hm-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor"><circle cx="12" cy="12" r="9" stroke-width="2"/></svg>`
  };

  // İstifadəçinin təyin etdiyi PNG/SVG yolundan ikon yaradırıq
  function createIconEl(kind) {
    const map = (window.HISTORY_STATUS_ICONS || {});
    const url = map[kind] || map.default;
    if (url) {
      const img = new Image();
      img.src = url;
      img.alt = kind || "icon";
      img.className = "hm-icon";
      img.decoding = "async";
      img.loading = "eager";
      return img;
    }
    // Fallback SVG
    const wrap = document.createElement("span");
    wrap.innerHTML = fallbackSVGs[kind] || fallbackSVGs.default;
    const svg = wrap.firstElementChild;
    if (svg) svg.classList.add("hm-icon");
    return svg || document.createElement("span");
  }

  // =========================
  //  Panel qurucu
  // =========================
  const DOM = { root:null, header:null, close:null, body:null, footer:null, title:null };

  function buildPanel() {
    ensureStyle();
    if (DOM.root) return;

    const root = document.createElement("div");
    root.className = "hm-panel hm-hidden";
    root.setAttribute("role", "dialog");
    root.setAttribute("aria-modal", "true");

    const header = document.createElement("div");
    header.className = "hm-header";

    const title = document.createElement("h3");
    title.className = "hm-title";
    title.innerHTML = `Tarixcə`;

    const closeBtn = document.createElement("button");
    closeBtn.className = "hm-close";
    closeBtn.type = "button";
    closeBtn.setAttribute("aria-label", "Bağla");
    closeBtn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path stroke-width="2" d="M18 6L6 18M6 6l12 12"/></svg>`;

    const body = document.createElement("div");
    body.className = "hm-body";
    body.innerHTML = `<div class="hm-loading"><span class="hm-spinner"></span>Yüklənir...</div>`;

    const footer = document.createElement("div");
    footer.className = "hm-footer";
    footer.innerHTML = `

    `;

    header.appendChild(title);
    header.appendChild(closeBtn);
    root.appendChild(header);
    root.appendChild(body);
    root.appendChild(footer);
    document.body.appendChild(root);

    makeDraggable(root, header);
    closeBtn.addEventListener("click", () => hide());
    document.addEventListener("keydown", (e) => {
      if (!root.classList.contains("hm-hidden") && e.key === "Escape") hide();
    });

    DOM.root = root; DOM.header = header; DOM.title = title;
    DOM.body = body; DOM.footer = footer; DOM.close = closeBtn;
  }

  // =========================
  //  Drag & position
  // =========================
  function makeDraggable(panel, handle) {
    let dragging = false;
    let startX = 0, startY = 0;
    let startLeft = 0, startTop = 0;

    const onDown = (clientX, clientY) => {
      dragging = true;
      const rect = panel.getBoundingClientRect();
      startLeft = rect.left; startTop = rect.top;
      startX = clientX; startY = clientY;
      panel.style.transition = "none";
      document.body.style.userSelect = "none";
    };

    const onMove = (clientX, clientY) => {
      if (!dragging) return;
      const dx = clientX - startX, dy = clientY - startY;
      let newLeft = startLeft + dx, newTop = startTop + dy;

      const pad = 5;
      const maxLeft = window.innerWidth - panel.offsetWidth - pad;
      const maxTop  = window.innerHeight - panel.offsetHeight - pad;
      newLeft = Math.min(Math.max(pad, newLeft), Math.max(pad, maxLeft));
      newTop  = Math.min(Math.max(pad, newTop),  Math.max(pad, maxTop));

      panel.style.left = `${newLeft}px`;
      panel.style.top  = `${newTop}px`;
      panel.style.transform = "translateX(0)";
    };

    const onUp = () => {
      dragging = false;
      panel.style.transition = "";
      document.body.style.userSelect = "";
    };

    handle.addEventListener("mousedown", (e) => {
      if (e.button !== 0) return;
      onDown(e.clientX, e.clientY);
      e.preventDefault();
    });
    window.addEventListener("mousemove", (e) => onMove(e.clientX, e.clientY));
    window.addEventListener("mouseup", onUp);

    handle.addEventListener("touchstart", (e) => {
      const t = e.touches[0]; if (!t) return;
      onDown(t.clientX, t.clientY);
    }, { passive: true });
    window.addEventListener("touchmove", (e) => {
      const t = e.touches[0]; if (!t) return;
      onMove(t.clientX, t.clientY);
    }, { passive: true });
    window.addEventListener("touchend", onUp);
    window.addEventListener("touchcancel", onUp);
  }

  function centerPanel() {
    if (!DOM.root) return;
    DOM.root.style.top = "12vh";
    DOM.root.style.left = "50%";
    DOM.root.style.transform = "translateX(-50%)";
  }

  // =========================
  //  Render helpers
  // =========================
  function setLoading(msg = "Yüklənir...") {
    if (!DOM.body) return;
    DOM.body.innerHTML = `<div class="hm-loading"><span class="hm-spinner"></span>${msg}</div>`;
  }

  function setError(text) {
    if (!DOM.body) return;
    DOM.body.innerHTML = `<div class="hm-error">${text}</div>`;
  }

  function renderStatusRow(kind, name, ok, hasRow, msg) {
    const badgeClass = ok ? "ok" : (hasRow ? "warn" : "err");
    const badgeText  = ok ? "İcra edilib" : (hasRow ? "İcra edilməyib" : "İcra edilməyib");

    const row = document.createElement("div");
    row.className = "hm-row";

    // --- INFO: ikon + ad yanaşı (başlıq sətiri) ---
    const infoBox = document.createElement("div");

    const head = document.createElement("div");
    head.className = "hm-head";

    const iconEl = createIconEl(kind);     // artıq ayrıca sütun YOXDUR
    const nm = document.createElement("div");
    nm.className = "hm-name";
    nm.textContent = name;

    head.appendChild(iconEl);
    head.appendChild(nm);

    const ms = document.createElement("div");
    ms.className = "hm-msg";
    ms.innerHTML = buildMsgHTML(msg || "");

    infoBox.appendChild(head);
    infoBox.appendChild(ms);

    // --- BADGE ---
    const badgeBox = document.createElement("div");
    const badge = document.createElement("span");
    badge.className = `hm-badge ${badgeClass}`;
    badge.textContent = badgeText;
    badgeBox.appendChild(badge);

    // Qeyd: iconBox artıq yoxdur
    row.appendChild(infoBox);
    row.appendChild(badgeBox);
    return row;
  }


  function renderData(data) {
    const { items = {}, messages = {}, meta_id } = data || {};
    const wrap = document.createElement("div");

    const rows = [
      ["attach", "Qoşma lay"],
      ["gis", "Tədqiqat layı"],
      ["tekuis", "TEKUİS parselləri"],
    ];
    rows.forEach(([k, label]) => {
      const info = items[k] || { ok: false, has_row: false };
      const msg = messages[k] || "";
      wrap.appendChild(renderStatusRow(k, label, !!info.ok, !!info.has_row, msg));
    });

    DOM.body.innerHTML = "";
    if (typeof meta_id === "number") {
      const meta = document.createElement("div");
      meta.style.cssText = "font-size:12px;color:#6b7280;margin:0 2px 10px 2px;";
      DOM.body.appendChild(meta);
    }
    DOM.body.appendChild(wrap);
  }

  // =========================
  //  API çağırışı
  // =========================
  async function load(ticket) {
    setLoading();
    try {
      const res = await fetch(`/api/history/status/?ticket=${encodeURIComponent(ticket)}`, {
        method: "GET",
        headers: { "Accept": "application/json" }
      });
      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        if (res.status === 401) {
          setError("Giriş icazəsi yoxdur və ya ticket aktiv deyil.");
          return;
        }
        setError(`Xəta: ${data && data.error ? data.error : "naməlum"}`);
        return;
      }
      renderData(data);
    } catch (e) {
      setError("Şəbəkə xətası: serverə qoşulmaq mümkün olmadı.");
    }
  }

  // =========================
  //  Aç / Bağla (public)
  // =========================
  function show(ticket) {
    if (!ticket) {
      buildPanel();
      centerPanel();
      DOM.title.textContent = "Tarixcə";
      DOM.root.classList.remove("hm-hidden");
      setError("Ticket dəyəri tapılmadı.");
      return;
    }
    if (!DOM.root) buildPanel();
    centerPanel();
    DOM.title.innerHTML = `Tarixçə <span style="font-weight:500;color:#6b7280;"></span>`;
    DOM.root.classList.remove("hm-hidden");
    load(ticket);
  }

  function hide() {
    if (DOM.root) DOM.root.classList.add("hm-hidden");
  }

  // =========================
  //  Auto-bind düymələr + düyməni yarat
  // =========================
  function readTicketFromDom(btn) {
    const dt = btn && btn.getAttribute("data-ticket");
    if (dt) return dt.trim();

    const t1 = document.getElementById("ticket");
    if (t1 && t1.value) return String(t1.value).trim();
    const t2 = document.querySelector("input[name='ticket']");
    if (t2 && t2.value) return String(t2.value).trim();

    if (window.APP && window.APP.ticket) {
      return String(window.APP.ticket).trim();
    }
    return "";
  }

  function ensureHistoryButton() {
    const box = document.getElementById("rightTools");
    if (!box) return;
    if (box.querySelector("[data-open-history]")) return;

    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "rt-btn";
    btn.title = "Tarixçə";
    btn.setAttribute("aria-label", "Tarixçə");
    btn.setAttribute("data-open-history", "1");

    const t = (window.APP && window.APP.ticket) ? String(window.APP.ticket).trim() : "";
    if (t) btn.setAttribute("data-ticket", t);

    const img = document.createElement("img");
    img.className = "rt-icon";
    img.alt = "history";

    const iconSrc = (window.HISTORY_ICON || "").toString();
    if (iconSrc) {
      img.src = iconSrc;
    } else {
      img.src = "data:image/svg+xml;utf8," + encodeURIComponent(
        '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><circle cx="12" cy="12" r="9" stroke-width="2"/><path d="M12 7v5l3 2" stroke-width="2"/></svg>'
      );
    }

    btn.appendChild(img);
    box.appendChild(btn);
  }

  function bindButtons() {
    document.addEventListener("click", (e) => {
      const btn = e.target.closest("[data-open-history]");
      if (!btn) return;
      const ticket = readTicketFromDom(btn);
      e.preventDefault();
      show(ticket);
    });
  }

  // Init
  buildPanel();
  bindButtons();
  ensureHistoryButton();

  // Public API
  window.HistoryPanel = { open: show, close: hide };
})();
