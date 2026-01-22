function initPanelUI(){
  const panelEl = document.getElementById('side-panel');
  if (!panelEl) return null;

  const panelTitleEl = panelEl.querySelector('.panel-title');
  const panelBodyEl = panelEl.querySelector('.panel-body');
  const panelCloseBtn = document.getElementById('panel-close');
  const indicatorEl = document.getElementById('panel-indicator');
  const workspaceEl = document.querySelector('.workspace');

  function openPanel(title, html){
    panelTitleEl.textContent = title || 'Panel';
    panelBodyEl.innerHTML = html || '';
    panelEl.hidden = false;
    void panelEl.offsetWidth;
    panelEl.classList.add('open');
    panelEl.setAttribute('aria-hidden', 'false');
  }

  function closePanel(){
    window.stopDraw?.(true);
    panelEl.classList.remove('open');
    panelEl.setAttribute('aria-hidden', 'true');
    const onEnd = (e) => {
      if (e.propertyName === 'transform') {
        panelEl.hidden = true;
        panelEl.removeEventListener('transitionend', onEnd);
      }
    };
    panelEl.addEventListener('transitionend', onEnd);
    indicatorEl.hidden = true;
    document.querySelectorAll('.tool-btn').forEach(b => b.classList.remove('active'));
  }

  function moveIndicatorToButton(btn){
    const btnRect = btn.getBoundingClientRect();
    const wsRect = workspaceEl.getBoundingClientRect();
    const top = btnRect.top - wsRect.top;
    indicatorEl.style.top = `${top}px`;
    indicatorEl.style.height = `${btnRect.height}px`;
    indicatorEl.hidden = false;
  }

  panelCloseBtn?.addEventListener('click', closePanel);

  return {
    panelEl,
    panelBodyEl,
    indicatorEl,
    workspaceEl,
    openPanel,
    closePanel,
    moveIndicatorToButton
  };
}

window.PanelUI = initPanelUI();