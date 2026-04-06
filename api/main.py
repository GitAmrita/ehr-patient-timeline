from contextlib import asynccontextmanager
from fastapi import FastAPI
from api.db import close_conn
from api.routers import patients


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

app.include_router(patients.router)
