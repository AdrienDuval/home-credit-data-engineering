"""API configuration from environment. Loads project .env so backend uses same PostgreSQL as Docker."""
import os
from pathlib import Path

_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
if _env_path.exists():
    from dotenv import load_dotenv
    load_dotenv(_env_path)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(
        user=os.environ.get("POSTGRES_USER", "home_credit_user"),
        password=os.environ.get("POSTGRES_PASSWORD", "home_credit_pwd"),
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "home_credit"),
    ),
)
JWT_SECRET = os.environ.get("JWT_SECRET", "change-me-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_MINUTES = 60
API_USER = os.environ.get("API_USER", "api_user")
API_PASSWORD_HASH = os.environ.get("API_PASSWORD_HASH")
