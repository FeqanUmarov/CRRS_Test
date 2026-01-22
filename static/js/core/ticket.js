/* =========================
   TICKET
   ========================= */
function resolveTicket() {
  const fromApp = (window.APP && typeof APP.ticket === 'string') ? APP.ticket.trim() : '';
  const fromQS = (new URLSearchParams(window.location.search)).get('ticket') || '';
  const t = (fromApp || fromQS);
  return (t && t.length > 0) ? t.trim() : null;
}

window.PAGE_TICKET = resolveTicket();