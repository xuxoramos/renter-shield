"""HTML routes for the developer-facing web UI (htmx + Jinja2).

Self-service API access: developers register for a token, then use it as the
``X-API-Key`` header against the JSON API.  Developer keys are granted
investigator-grade access across the property, landlord, and combined
endpoints.  The developer landing page is intentionally not linked from the
public site — it is shared as a direct link.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Cookie, Form, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from renter_shield import audit

_TEMPLATE_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATE_DIR))

router = APIRouter(tags=["web-developer"])

TOKEN_COOKIE = "rs_token"
COOKIE_MAX_AGE = audit.SESSION_EXPIRY_DAYS * 86400


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------
def _get_user(rs_token: str | None) -> dict | None:
    if not rs_token:
        return None
    return audit.validate_token(rs_token)


def _set_token_cookie(response: Response, token: str) -> Response:
    response.set_cookie(
        key=TOKEN_COOKIE,
        value=token,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=False,  # nginx terminates TLS in prod
    )
    return response


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------
@router.get("/developer/register", response_class=HTMLResponse)
async def dev_register_page(request: Request, rs_token: Annotated[str | None, Cookie()] = None):
    user = _get_user(rs_token)
    if user and user.get("scope") == "developer":
        return RedirectResponse("/developer/", status_code=302)
    return templates.TemplateResponse(request, "dev_register.html", {"error": None})


@router.post("/developer/register", response_class=HTMLResponse)
async def dev_register_submit(
    request: Request,
    name: Annotated[str, Form()],
    email: Annotated[str, Form()],
    role: Annotated[str, Form()] = "",
    agree: Annotated[str | None, Form()] = None,
):
    if not agree:
        return templates.TemplateResponse(
            request, "dev_register.html",
            {"error": "You must agree to the disclaimer to continue."},
        )
    if not name.strip() or not email.strip():
        return templates.TemplateResponse(
            request, "dev_register.html",
            {"error": "Name and email are required."},
        )
    ip = request.client.host if request.client else ""
    user = audit.register_user(name=name, email=email, role=role, scope="developer", ip=ip)
    response = templates.TemplateResponse(
        request, "dev_registered.html", {"user": user, "token": user["token"]},
    )
    return _set_token_cookie(response, user["token"])


@router.post("/developer/token-login", response_class=HTMLResponse)
async def dev_token_login(request: Request, token: Annotated[str, Form()]):
    user = audit.validate_token(token.strip())
    if not user or user.get("scope") != "developer":
        return templates.TemplateResponse(
            request, "dev_register.html",
            {"error": "Invalid, expired, or non-developer token."},
        )
    response = RedirectResponse("/developer/", status_code=302)
    return _set_token_cookie(response, user["token"])


@router.get("/developer/sign-out")
async def dev_sign_out():
    response = RedirectResponse("/developer/register", status_code=302)
    response.delete_cookie(TOKEN_COOKIE)
    return response


# ---------------------------------------------------------------------------
# API documentation page (the direct link shared with select users)
# ---------------------------------------------------------------------------
@router.get("/developer/", response_class=HTMLResponse)
async def dev_home(request: Request, rs_token: Annotated[str | None, Cookie()] = None):
    user = _get_user(rs_token)
    if not user or user.get("scope") != "developer":
        return RedirectResponse("/developer/register", status_code=302)
    audit.log_page_view(user["id"], "developer", "docs", {})
    return templates.TemplateResponse(request, "developer.html", {
        "user": user,
        "token": user["token"],
    })
