# crrs/views.py
from django.shortcuts import render, redirect

from corrections.views import _redeem_ticket_with_token


LOGIN_URL = "http://10.11.1.73:8085/login"


def index(request):
    """
    Root səhifə:
      http://10.11.1.40:8085/?ticket=XXXX
    """
    ticket = (request.GET.get("ticket") or "").strip()


    if not ticket:
        return redirect(LOGIN_URL)


    fk, tok = _redeem_ticket_with_token(ticket)


    if not (fk and tok):
        return redirect(LOGIN_URL)

    return render(request, "index.html", {"ticket": ticket})
