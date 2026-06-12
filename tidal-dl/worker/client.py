"""
Worker 与 Server 通信的 HTTP 客户端
"""
import requests
import logging

logger = logging.getLogger("worker.client")


class ServerClient:
    def __init__(self, server_url: str):
        self.server_url = server_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"
        # 增大连接池以匹配高并发
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def register(self, name: str, hostname: str, ip: str, max_concurrency: int) -> dict:
        """注册 Worker"""
        resp = self.session.post(f"{self.server_url}/api/workers/register", json={
            "name": name,
            "hostname": hostname,
            "ip": ip,
            "max_concurrency": max_concurrency,
        })
        resp.raise_for_status()
        return resp.json()

    def heartbeat(self, worker_id: str, status: str, active_tasks: int,
                  total_downloaded: int, total_failed: int, total_bytes: int) -> dict:
        """心跳上报"""
        resp = self.session.post(f"{self.server_url}/api/workers/{worker_id}/heartbeat", json={
            "status": status,
            "active_tasks": active_tasks,
            "total_downloaded": total_downloaded,
            "total_failed": total_failed,
            "total_bytes": total_bytes,
        })
        resp.raise_for_status()
        return resp.json()

    def get_config(self, worker_id: str) -> dict:
        """获取配置"""
        resp = self.session.get(f"{self.server_url}/api/workers/{worker_id}/config")
        resp.raise_for_status()
        return resp.json()

    def fetch_tasks(self, worker_id: str, batch_size: int = 10, account_id: int = None) -> dict:
        """拉取待下载任务"""
        data = {"worker_id": worker_id, "batch_size": batch_size}
        if account_id:
            data["account_id"] = account_id
        resp = self.session.post(f"{self.server_url}/api/tasks/fetch", json=data)
        resp.raise_for_status()
        return resp.json()

    def fetch_available_accounts(self) -> list:
        """获取所有可用账号"""
        resp = self.session.get(f"{self.server_url}/api/accounts/available")
        resp.raise_for_status()
        return resp.json()

    def report_task_status(self, task_id: int, status: str, **kwargs) -> dict:
        """上报单任务状态"""
        data = {"status": status}
        data.update(kwargs)
        resp = self.session.post(f"{self.server_url}/api/tasks/{task_id}/status", json=data)
        resp.raise_for_status()
        return resp.json()

    def report_task_status_batch(self, updates: list) -> dict:
        """批量上报任务状态"""
        resp = self.session.post(f"{self.server_url}/api/tasks/batch-status", json={"updates": updates})
        resp.raise_for_status()
        return resp.json()

    def report_account_issue(self, account_id: int, status: str, error_message: str = "") -> dict:
        """上报账号异常"""
        resp = self.session.post(f"{self.server_url}/api/accounts/{account_id}/report", json={
            "status": status,
            "error_message": error_message,
        })
        resp.raise_for_status()
        return resp.json()
