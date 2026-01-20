/* app_toast.js – Sağ-aşağı toast bildirişləri + SweetAlert2 patch
   Quraşdırma: SweetAlert2-dən sonra, main.js-dən əvvəl qoşun.
*/
(function(){
  'use strict';

  // ===== Helpers =====
  const TYPES = new Set(['success','error','warning','info']);

  function ensureStack(){
    let stack = document.getElementById('toastStack');
    if (!stack){
      stack = document.createElement('div');
      stack.id = 'toastStack';
      stack.className = 'toast-stack';
      document.body.appendChild(stack);
    }
    return stack;
  }

  function removeToast(el){
    if (!el) return;
    el.classList.add('hide');
    setTimeout(()=> el.remove(), 220);
  }

  function showToast({ title='', text='', html=null, type='info', duration=5000 } = {}){
    const stack = ensureStack();
    if (!TYPES.has(type)) type = 'info';

    // max 5 toast: artıqdısa ən köhnəsini sil
    const existing = stack.querySelectorAll('.toast');
    if (existing.length >= 5) removeToast(existing[0]);

    const t = document.createElement('div');
    t.className = `toast toast--${type}`;
    t.setAttribute('role','status');
    t.setAttribute('aria-live','polite');

    const titleEl = document.createElement('div');
    titleEl.className = 'toast-title';
    titleEl.textContent = title || (type === 'success' ? 'Uğurlu' :
                                    type === 'error'   ? 'Xəta'   :
                                    type === 'warning' ? 'Diqqət' : 'Məlumat');

    const textEl = document.createElement('div');
    textEl.className = 'toast-text';
    if (html){ textEl.innerHTML = html; } else { textEl.textContent = text || ''; }

    const closeBtn = document.createElement('button');
    closeBtn.className = 'toast-close';
    closeBtn.type = 'button';
    closeBtn.setAttribute('aria-label','Bağla');
    closeBtn.textContent = '×';

    const prog = document.createElement('div');
    prog.className = 'toast-progress';
    const bar = document.createElement('span');
    bar.style.animationDuration = Math.max(1000, duration|0) + 'ms';
    prog.appendChild(bar);

    t.appendChild(titleEl);
    t.appendChild(textEl);
    t.appendChild(closeBtn);
    t.appendChild(prog);

    // clicklə də bağlamaq mümkün olsun (X-ə ehtiyac qalmasa)
    t.addEventListener('click', (e)=>{
      // progress və ya close klik – bağla
      if (e.target === closeBtn || e.target === t || e.target.closest('.toast-close')) {
        removeToast(t);
      }
    });

    stack.appendChild(t);
    // entrance anim
    requestAnimationFrame(()=> t.classList.add('show'));

    const hideTimer = setTimeout(()=> removeToast(t), Math.max(1000, duration|0));
    closeBtn.addEventListener('click', (e)=> {
      e.stopPropagation();
      clearTimeout(hideTimer);
      removeToast(t);
    });

    // Promise qaytar – Swal.fire ilə uyğunluq üçün
    return Promise.resolve({ isDismissed: true });
  }

  // Publik API
  window.Toast = {
    show: showToast,
    success: (text, title='Uğurlu', ms=5000)=> showToast({title, text, type:'success', duration:ms}),
    error:   (text, title='Xəta',   ms=5000)=> showToast({title, text, type:'error',   duration:ms}),
    warning: (text, title='Diqqət', ms=5000)=> showToast({title, text, type:'warning', duration:ms}),
    info:    (text, title='Məlumat',ms=5000)=> showToast({title, text, type:'info',    duration:ms})
  };

  // ===== SweetAlert2 patch =====
  // Qayda: 
  //  - Əgər obyekt parametrlərində showCancelButton / input və s. varsa → orijinal modal saxlanılır.
  //  - 3-arg (title, text, icon) və ya sadə obyekt → TOAST-a çevrilir.
  function patchSwalToToast(){
    if (!window.Swal || typeof Swal.fire !== 'function') return;
    const original = Swal.fire.bind(Swal);
    // ehtiyat üçün saxlayırıq
    Swal._originalFire = original;

    Swal.fire = function(a,b,c){
      try{
        // 3-arg imzası: (title, text, icon)
        if (typeof a === 'string' || typeof a === 'number'){
          const title = a != null ? String(a) : '';
          const text  = b != null ? String(b) : '';
          const icon  = c != null ? String(c) : 'info';
          return showToast({ title, text, type: icon, duration: 5000 });
        }
        // Obyekt imzası
        if (a && typeof a === 'object'){
          const opts = a;

          // Modal saxlamaq istədiyimiz hallar
          if (opts.showCancelButton || opts.showDenyButton || opts.input || opts.html && opts.showConfirmButton !== false){
            return original(opts);
          }

          // Qalan bütün hallarda TOAST
          const title = opts.title || '';
          const text  = (opts.text != null ? String(opts.text) : '');
          const icon  = opts.icon || 'info';
          const html  = opts.html || null;
          const timer = (typeof opts.timer === 'number') ? opts.timer : 5000;
          return showToast({ title, text, html, type: icon, duration: timer });
        }

        // Tanınmayan forma – təhlükəsiz tərəf: original
        return original(a,b,c);
      }catch(e){
        // Hər ehtimala qarşı səhv olarsa original-a düşək
        return original(a,b,c);
      }
    };
  }

  // DOM hazır olan kimi patch et
  if (document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', patchSwalToToast);
  } else {
    patchSwalToToast();
  }
})();
