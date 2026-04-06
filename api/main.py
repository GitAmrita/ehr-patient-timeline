from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.db import close_conn
from api.routers import patients, timeline


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    close_conn()


app = FastAPI(
    title="EHR Patient Timeline API",
    description="REST API over synthetic EHR data — dbt + DuckDB backend",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(patients.router)
app.include_router(timeline.router)
