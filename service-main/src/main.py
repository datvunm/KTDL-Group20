from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.api.routes.health import router as health_router
from src.api.routes.marts import router as marts_router
from src.middleware.timing import add_process_time_header

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="KTDL-Group20")
app.middleware("http")(add_process_time_header)

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

app.include_router(health_router)
app.include_router(marts_router)


@app.get("/api")
def read_root() -> dict[str, str]:
    return {"message": "KTDL-Group20 API is running"}


@app.get("/")
def read_dashboard() -> FileResponse:
    dashboard_path = STATIC_DIR / "dashboard.html"
    return FileResponse(dashboard_path)
