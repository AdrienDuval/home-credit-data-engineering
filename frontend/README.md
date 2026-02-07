# Home Credit Datamart – Next.js frontend

- **Login:** default credentials `api_user` / `demo` (same as API).
- **Dashboard:** 3 charts (risk distribution, default rate, credit exposure) + paginated client risk table.

## Setup

1. Copy env and set API URL (default `http://localhost:8000`):
   ```bash
   cp .env.local.example .env.local
   ```
2. Install and run:
   ```bash
   npm install
   npm run dev
   ```
3. Open [http://localhost:3000](http://localhost:3000). Ensure the backend API is running (`python run_api.py` from `api/` or Docker).
