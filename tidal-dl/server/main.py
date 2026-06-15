"""
TIDAL 分布式下载系统 - Server
FastAPI 入口
"""
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import os

app = FastAPI(
    title="TIDAL 分布式下载系统",
    description="管理 TIDAL 音乐下载任务的 Server 端",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
from routers.config_router import router as config_router
from routers.accounts import router as accounts_router
from routers.jobs import router as jobs_router
from routers.workers import router as workers_router
from routers.tasks import router as tasks_router
from routers.dashboard import router as dashboard_router
from services.token_manager import router as auth_router
from routers.exports import router as exports_router

app.include_router(config_router)
app.include_router(accounts_router)
app.include_router(jobs_router)
app.include_router(workers_router)
app.include_router(tasks_router)
app.include_router(dashboard_router)
app.include_router(auth_router)
app.include_router(exports_router)


@app.on_event("startup")
def start_background_tasks():
    """启动后台任务：Token 自动刷新 + 导出分组构建"""
    import threading
    from services.token_manager import auto_refresh_loop
    t = threading.Thread(target=auto_refresh_loop, daemon=True)
    t.start()

    # 启动导出分组后台构建线程
    from routers.exports import start_group_builder
    start_group_builder()


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "tidal-dl-server"}


# 挂载前端静态文件（如果存在）
web_dist = os.path.join(os.path.dirname(__file__), "..", "web", "dist")
if os.path.exists(web_dist):
    app.mount("/", StaticFiles(directory=web_dist, html=True), name="web")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
