const LOGIN_APP_BASE = 'http://10.11.1.73:8085/login';

// --- Auth Guard (ilk açılış + periodik yoxlama)
function showAuthGateAndRedirect(msg){
  try {
    const div = document.createElement('div');
    div.style.cssText = 'position:fixed;inset:0;background:#0b102033;backdrop-filter:blur(3px);display:flex;align-items:center;justify-content:center;z-index:99999;';
    div.innerHTML = `<div style="background:#fff;padding:20px 24px;border-radius:10px;box-shadow:0 10px 30px rgba(0,0,0,.12);font-family:sans-serif;max-width:520px;text-align:center">
      <h3 style="margin:0 0 8px">Sessiya bitmişdir</h3>
      <p style="margin:0 0 12px;color:#374151">${msg || 'Token etibarsız və ya vaxtı keçib.'}</p>
      <p style="margin:0;color:#6b7280">Login səhifəsinə yönləndirilirsiniz…</p>
    </div>`;
    document.body.appendChild(div);
  } catch {}
  setTimeout(()=>{ window.location.replace(LOGIN_APP_BASE); }, 1200);
}

// 401 (Unauthorized) düşən kimi avtomatik login səhifəsinə yönləndir
(function hardAuthRedirect(){
  const _fetch = window.fetch;
  window.fetch = async (...args) => {
    const r = await _fetch(...args);
    if (r.status === 401) {
      showAuthGateAndRedirect('Sessiya bitdi və ya token etibarsızdır.');
      // axınları dayandırmaq üçün error atırıq:
      throw new Error('Unauthorized');
    }
    return r;
  };
})();

async function authGuardOnce(){
  // Ticket ümumiyyətlə yoxdursa → birbaşa geri
  if (!window.PAGE_TICKET) {
    showAuthGateAndRedirect('Ticket tapılmadı.');
    return false;
  }
  try{
    const r = await fetch(`/api/ticket-status/?ticket=${encodeURIComponent(window.PAGE_TICKET)}`, {
      headers: { 'Accept':'application/json' }
    });
    if (!r.ok) {
      const txt = await r.text().catch(()=> '');
      showAuthGateAndRedirect(txt || 'Giriş tələb olunur.');
      return false;
    }
    const data = await r.json();
    // mövcud dəyişənlərin yenilənməsi (səndə artıq var)
    window.CURRENT_STATUS_ID = data?.status_id ?? null;
    window.EDIT_ALLOWED = !!data?.allow_edit;
    window.applyEditPermissions?.();
    window.updateTicketDeleteState?.();
    return true;
  }catch(e){
    console.warn('authGuard error:', e);
    showAuthGateAndRedirect('Şəbəkə xətası.');
    return false;
  }
}

// Səhifə açılan kimi yoxla:
authGuardOnce();

// Hər 30 saniyədən bir sessiyanı yoxla:
setInterval(() => { authGuardOnce(); }, 30000);

// === STATUS icazəsi (yalnız STATUS_ID 2 və 99 üçün) ===
window.EDIT_ALLOWED = false;
window.CURRENT_STATUS_ID = null;

async function fetchTicketStatus() {
  if (!window.PAGE_TICKET) return false;
  try {
    const resp = await fetch(`/api/ticket-status/?ticket=${encodeURIComponent(window.PAGE_TICKET)}`, {
      headers: { 'Accept':'application/json' }
    });
    if (!resp.ok) throw new Error(await resp.text());
    const data = await resp.json();
    window.CURRENT_STATUS_ID = data?.status_id ?? null;
    window.EDIT_ALLOWED = !!data?.allow_edit;
    applyEditPermissions();
    window.updateTicketDeleteState?.();
    return window.EDIT_ALLOWED;
  } catch (e) {
    console.warn('ticket-status error:', e);
    window.EDIT_ALLOWED = false;
    applyEditPermissions();
    return false;
  }
}

function applyEditPermissions(){
  const dataBtn = document.querySelector('.tool-btn[data-panel="contents"]');
  const editBtn = document.querySelector('.tool-btn[data-panel="catalog"]');
  [dataBtn, editBtn].forEach(btn=>{
    if (!btn) return;
    if (window.EDIT_ALLOWED){
      btn.classList.remove('locked');
      btn.removeAttribute('aria-disabled');
      btn.title = btn.dataset.title || btn.title || '';
    } else {
      btn.classList.add('locked');
      btn.setAttribute('aria-disabled','true');
      btn.title = 'Bu müraciətin statusuna görə (STATUS_ID ≠ 2/99) bu bölmə bağlıdır.';
    }
  });
}

window.authGuardOnce = authGuardOnce;
window.fetchTicketStatus = fetchTicketStatus;
window.applyEditPermissions = applyEditPermissions;