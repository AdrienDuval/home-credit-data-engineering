"""Run the API server. Use: python run_api.py (no need for uvicorn on PATH)."""
import uvicorn

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000)
