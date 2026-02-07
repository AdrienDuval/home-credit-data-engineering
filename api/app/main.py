"""
REST API for datamarts: JWT auth and pagination.
- POST /token — login, returns JWT
- GET /clients/risk — paginated rows, optional filters (requires JWT)
- GET /clients/risk/{id} — one client detail (requires JWT)
- GET /portfolio/summary — portfolio summary for dashboard charts (requires JWT)
"""
from typing import Optional

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer
from pydantic import BaseModel

from app.auth import create_access_token, get_current_user, hash_password, verify_password
from app.config import API_PASSWORD_HASH, API_USER
from app.database import (
    fetch_bureau_by_client,
    fetch_client_by_id,
    fetch_client_risk_page,
    fetch_previous_apps_by_client,
)
from app.db_portfolio import fetch_portfolio_summary


def _app():
    from starlette.middleware.base import BaseHTTPMiddleware
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware

    ALLOWED_ORIGINS = {"http://localhost:3000", "http://127.0.0.1:3000"}

    app = FastAPI(title="Home Credit Datamart API", version="1.0.0")

    # Ensure CORS headers on every response (including 401/5xx) so the browser doesn't block
    class EnsureCORSHeaders(BaseHTTPMiddleware):
        async def dispatch(self, request, call_next):
            response = await call_next(request)
            origin = request.headers.get("origin")
            if origin in ALLOWED_ORIGINS:
                response.headers.setdefault("Access-Control-Allow-Origin", origin)
                response.headers.setdefault("Access-Control-Allow-Credentials", "true")
                response.headers.setdefault("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                response.headers.setdefault("Access-Control-Allow-Headers", "Authorization, Content-Type")
            return response

    app.add_middleware(EnsureCORSHeaders)

    # Allow Next.js frontend (different port) to call the API
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(ALLOWED_ORIGINS),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["*"],
    )

    class TokenRequest(BaseModel):
        username: str
        password: str

    class TokenResponse(BaseModel):
        access_token: str
        token_type: str = "bearer"

    _DEMO_HASH = hash_password("demo")

    @app.options("/token")
    def token_options():
        """CORS preflight for POST /token."""
        return {}

    @app.post("/token", response_model=TokenResponse)
    def login(form: TokenRequest):
        expected_hash = API_PASSWORD_HASH or _DEMO_HASH
        if form.username != API_USER or not verify_password(form.password, expected_hash):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
            )
        token = create_access_token(subject=form.username)
        return TokenResponse(access_token=token)

    class ClientsRiskResponse(BaseModel):
        data: list
        page: int
        page_size: int
        total_count: int

    @app.get("/clients/risk", response_model=ClientsRiskResponse)
    def get_clients_risk(
        page: int = 1,
        page_size: int = 50,
        risk_segment: Optional[str] = None,
        client_id: Optional[int] = None,
        min_income: Optional[float] = None,
        max_income: Optional[float] = None,
        min_credit_exposure: Optional[float] = None,
        max_credit_exposure: Optional[float] = None,
        _user: str = Depends(get_current_user),
    ):
        if page < 1:
            page = 1
        if page_size < 1 or page_size > 1000:
            page_size = 50
        rows, total = fetch_client_risk_page(
            page=page,
            page_size=page_size,
            risk_segment=risk_segment or None,
            client_id=client_id,
            min_income=min_income,
            max_income=max_income,
            min_credit_exposure=min_credit_exposure,
            max_credit_exposure=max_credit_exposure,
        )
        return ClientsRiskResponse(data=rows, page=page, page_size=page_size, total_count=total)

    @app.get("/clients/risk/{sk_id_curr}/bureau")
    def get_client_bureau(sk_id_curr: int, _user: str = Depends(get_current_user)):
        """Return all bureau credits for one client (from datamart_client_bureau)."""
        return fetch_bureau_by_client(sk_id_curr)

    @app.get("/clients/risk/{sk_id_curr}/previous-applications")
    def get_client_previous_apps(sk_id_curr: int, _user: str = Depends(get_current_user)):
        """Return all previous applications for one client (from datamart_client_previous_apps)."""
        return fetch_previous_apps_by_client(sk_id_curr)

    @app.get("/clients/risk/{sk_id_curr}")
    def get_client_by_id(sk_id_curr: int, _user: str = Depends(get_current_user)):
        """Return detailed risk profile for one client. 404 if not found."""
        client = fetch_client_by_id(sk_id_curr)
        if client is None:
            raise HTTPException(status_code=404, detail="Client not found")
        return client

    @app.get("/portfolio/summary")
    def get_portfolio_summary(_user: str = Depends(get_current_user)):
        """Portfolio summary for dashboard charts (risk distribution, default rate, exposure)."""
        return fetch_portfolio_summary()

    return app


app = _app()
