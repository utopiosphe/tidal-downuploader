"""
TIDAL 分布式下载系统 - Worker 主程序
"""
import argparse
import logging
import os
import platform
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from client import ServerClient
from downloader import (
    build_session, download_track,
    TokenExpiredError, AccountBannedError, TrackNotFoundError,
    RateLimitError, DownloadError
)
from uploader import MultiUploader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("worker")

# 禁用 requests/urllib3 的 SSL 警告
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AccountPool:
    """本地账号池：智能选号 + 冷却规避"""

    def __init__(self):
        self.accounts = []           # 从 Server 同步的账号列表
        self.cooldowns = {}          # {account_id: cooldown_until_timestamp}
        self.fail_counts = {}        # {account_id: 连续429次数}
        self.active_tasks = {}       # {account_id: 当前活跃任务数}
        self._lock = threading.Lock()

    def sync(self, accounts_from_server: list):
        """同步 Server 账号列表"""
        with self._lock:
            server_ids = {a["id"] for a in accounts_from_server}
            old_ids = {a["id"] for a in self.accounts}

            # 新增的账号
            for a in accounts_from_server:
                if a["id"] not in old_ids:
                    logger.info(f"🔑 新账号加入池: ID={a['id']}")

            # 被移除的账号
            for aid in old_ids - server_ids:
                logger.info(f"🚫 账号移出池: ID={aid}")

            self.accounts = accounts_from_server

    def pick(self) -> dict | None:
        """选择最优账号（最闲 + 不在冷却中 + 不超过最大并发）"""
        MAX_PER_ACCOUNT = 30
        with self._lock:
            now = time.time()
            available = [
                a for a in self.accounts
                if self.cooldowns.get(a["id"], 0) < now
                and self.active_tasks.get(a["id"], 0) < MAX_PER_ACCOUNT
            ]
            if not available:
                return None
            # 按活跃任务数排序 + 随机打散（同等活跃度的账号随机选，确保均匀使用）
            import random
            available.sort(key=lambda a: (self.active_tasks.get(a["id"], 0), random.random()))
            best = available[0]
            # 即选即锁定
            self.active_tasks[best["id"]] = self.active_tasks.get(best["id"], 0) + 1
            return best

    def report_success(self, account_id: int):
        """下载成功，重置失败计数"""
        with self._lock:
            self.fail_counts[account_id] = 0

    def report_rate_limited(self, account_id: int):
        """429 限流，设置冷却"""
        with self._lock:
            count = self.fail_counts.get(account_id, 0) + 1
            self.fail_counts[account_id] = count
            cooldown_seconds = min(2 ** count * 15, 300)
            self.cooldowns[account_id] = time.time() + cooldown_seconds
            logger.warning(f"❄️ 账号 {account_id} 冷却 {cooldown_seconds}s（连续429: {count}次）")

    def report_unavailable(self, account_id: int, reason: str):
        """账号不可用（token过期/封禁），从池中移除"""
        with self._lock:
            self.accounts = [a for a in self.accounts if a["id"] != account_id]
            logger.warning(f"❌ 账号 {account_id} 移出池: {reason}")

    def acquire(self, account_id: int):
        """任务开始，活跃数+1"""
        with self._lock:
            self.active_tasks[account_id] = self.active_tasks.get(account_id, 0) + 1

    def release(self, account_id: int):
        """任务结束，活跃数-1"""
        with self._lock:
            self.active_tasks[account_id] = max(self.active_tasks.get(account_id, 1) - 1, 0)

    def status_summary(self) -> str:
        """状态摘要"""
        with self._lock:
            total = len(self.accounts)
            now = time.time()
            cooling = sum(1 for cd in self.cooldowns.values() if cd > now)
            return f"账号池: {total}个账号, {cooling}个冷却中"


class Worker:
    def __init__(self, server_url: str, name: str = ""):
        self.server = ServerClient(server_url)
        self.name = name or platform.node()
        self.max_concurrency = 10

        self.worker_id = None
        self.config = {}
        self.proxy_config = {}
        self.s3_configs = []
        self.download_config = {}
        self.uploader = None  # MultiUploader
        self.download_session = None
        self.account_pool = AccountPool()

        # 统计
        self.total_downloaded = 0
        self.total_failed = 0
        self.total_bytes = 0
        self.active_tasks = 0
        self.running = True
        import queue
        self._status_queue = queue.Queue()

    def start(self):
        """启动 Worker"""
        logger.info(f"Worker 启动中... Server: {self.server.server_url}")

        # 1. 注册
        self._register()

        # 2. 加载配置
        self._load_config()

        # 3. 启动心跳线程
        heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        heartbeat_thread.start()

        # 3.5 启动攒批上报线程
        reporter_thread = threading.Thread(target=self._status_reporter_loop, daemon=True)
        reporter_thread.start()

        # 4. 主循环：拉取任务 + 并发下载
        self._main_loop()

    def _register(self):
        """注册到 Server"""
        hostname = platform.node()
        try:
            ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            ip = "unknown"

        result = self.server.register(self.name, hostname, ip, self.max_concurrency)
        self.worker_id = result["worker_id"]
        self.config = result.get("config", {})
        logger.info(f"✅ 注册成功: {self.worker_id}")

    def _load_config(self):
        """从 Server 加载配置 + 账号列表"""
        config = self.server.get_config(self.worker_id)
        self.proxy_config = config.get("proxy", {})
        s3_raw = config.get("s3", [])
        self.s3_configs = s3_raw if isinstance(s3_raw, list) else [s3_raw]
        self.download_config = config.get("download", {})
        self.max_concurrency = config.get("concurrency", self.max_concurrency)

        # 初始化代理 session
        self.download_session = build_session(self.proxy_config)

        # 初始化多存储 S3
        self.uploader = MultiUploader(self.s3_configs)

        # 初始化账号池
        try:
            accounts = self.server.fetch_available_accounts()
            self.account_pool.sync(accounts)
            logger.info(f"🔑 账号池初始化: {len(accounts)} 个可用账号")
        except Exception as e:
            logger.warning(f"账号获取失败: {e}")

        proxy_label = self.proxy_config.get("host", "直连")
        logger.info(f"📋 配置: 并发={self.max_concurrency}, 代理={proxy_label}, "
                     f"存储={len(self.uploader.uploaders)}个")

    def _heartbeat_loop(self):
        """心跳线程 - 每 10 秒上报 + 同步配置"""
        sync_counter = 0
        while self.running:
            try:
                self.server.heartbeat(
                    self.worker_id,
                    status="online",
                    active_tasks=self.active_tasks,
                    total_downloaded=self.total_downloaded,
                    total_failed=self.total_failed,
                    total_bytes=self.total_bytes,
                )
            except Exception as e:
                logger.warning(f"心跳失败: {e}")

            # 每 3 次心跳 (30s) 同步一次配置
            sync_counter += 1
            if sync_counter % 3 == 0:
                try:
                    self._sync_config()
                except Exception as e:
                    logger.warning(f"配置同步失败: {e}")

            time.sleep(10)

    def _sync_config(self):
        """从 Server 同步配置，热更新"""
        config = self.server.get_config(self.worker_id)
        new_concurrency = config.get("concurrency", self.max_concurrency)

        # 并发数变更（实时生效）
        if new_concurrency != self.max_concurrency:
            self.update_concurrency(new_concurrency)

        # 代理变更（全量对比）
        new_proxy = config.get("proxy", {})
        if new_proxy != self.proxy_config:
            logger.info(f"🔄 代理变更: → {new_proxy.get('host', '直连')}:{new_proxy.get('socks5_port', '')}")
            self.proxy_config = new_proxy
            self.download_session = build_session(self.proxy_config)

        # S3 变更（多存储热更新）
        new_s3 = config.get("s3", [])
        new_s3_list = new_s3 if isinstance(new_s3, list) else [new_s3]
        if new_s3_list != self.s3_configs:
            logger.info(f"🔄 S3 配置变更，更新存储列表")
            self.s3_configs = new_s3_list
            self.uploader.update(self.s3_configs)

        # 下载配置
        self.download_config = config.get("download", self.download_config)

        # 同步账号列表
        try:
            accounts = self.server.fetch_available_accounts()
            self.account_pool.sync(accounts)
        except Exception as e:
            logger.warning(f"账号同步失败: {e}")


    def _status_reporter_loop(self):
        """后台异步攒批上报状态"""
        import queue
        batch = []
        while self.running:
            try:
                update = self._status_queue.get(timeout=2)
                batch.append(update)
            except queue.Empty:
                pass
            
            if len(batch) >= 50 or (batch and self._status_queue.empty()):
                try:
                    self.server.report_task_status_batch(batch)
                    # logger.debug(f"📤 批量上报 {len(batch)} 个任务状态成功")
                    batch = []
                except Exception as e:
                    logger.warning(f"批量上报失败 (将重试): {e}")
                    time.sleep(2)

    def _main_loop(self):
        """主循环 - 流水线模式 + 本地账号池"""
        import threading
        from queue import Queue, Empty

        logger.info(f"🚀 开始工作循环 (并发: {self.max_concurrency})")

        self._semaphore = threading.Semaphore(self.max_concurrency)
        self._executor = ThreadPoolExecutor(max_workers=1000)
        self._task_queue = Queue()

        while self.running:
            try:
                # 1. 队列空了才拉新任务
                if self._task_queue.empty():
                    batch_size = max(self.max_concurrency, 10)
                    result = self.server.fetch_tasks(
                        self.worker_id, batch_size
                    )
                    tasks = result.get("tasks", [])

                    if not tasks:
                        time.sleep(3)
                        continue

                    logger.info(f"📥 拉取 {len(tasks)} 个任务")
                    for t in tasks:
                        self._task_queue.put(t)

                # 2. 从队列取任务
                try:
                    task = self._task_queue.get(timeout=1)
                except Empty:
                    continue

                # 3. 拿到任务后再挑账号
                account = self.account_pool.pick()
                if not account:
                    # 没有可用账号，把任务塞回队列
                    self._task_queue.put(task)
                    logger.info(f"无可用账号，等待冷却解除... ({self.account_pool.status_summary()})")
                    time.sleep(5)
                    continue

                # 4. 真正锁定并发限制
                self._semaphore.acquire()
                if not self.running:
                    self.account_pool.release(account["id"])
                    break

                # 5. 提交执行
                self._executor.submit(self._run_and_release, task, account)

            except KeyboardInterrupt:
                logger.info("收到中断信号，退出...")
                self.running = False
                break
            except Exception as e:
                logger.error(f"主循环异常: {e}")
                time.sleep(3)

    def _run_and_release(self, task, account):
        """执行任务并释放信号量"""
        try:
            self._process_task(task, account)
        finally:
            self._semaphore.release()

    def update_concurrency(self, new_concurrency):
        """动态调整并发数（热更新）- 不重建 Semaphore，避免主循环阻塞"""
        old = self.max_concurrency
        if new_concurrency == old:
            return
        self.max_concurrency = new_concurrency
        diff = new_concurrency - old

        if diff > 0:
            # 增加并发：多释放 diff 个信号量，让主循环能 acquire 到更多
            for _ in range(diff):
                self._semaphore.release()
        else:
            # 减少并发：尝试 acquire 掉多余的信号量（非阻塞，拿不到就算了，自然会收缩）
            for _ in range(-diff):
                self._semaphore.acquire(blocking=False)

        logger.info(f"🔧 并发数调整: {old} → {new_concurrency} (差值: {diff:+d})")

    def _process_task(self, task: dict, account: dict):
        """处理单个下载任务"""
        task_id = task["id"]
        track_id = task["track_id"]
        title = task.get("title", "?")
        artist = task.get("artist", "?")
        quality = self.download_config.get("quality", task.get("audio_quality", "LOSSLESS"))
        country = account.get("country_code", "NG")
        access_token = account.get("access_token", "")
        account_id = account.get("id")

        self.active_tasks += 1
        logger.info(f"🎵 [{task_id}] {artist} - {title} (track={track_id}, acc={account_id})")

        try:
            # 上报开始下载
            self._status_queue.put({"task_id": task_id, "status": "downloading", "account_id": account_id})

            # 下载
            start_time = time.time()
            file_path, codec, actual_quality = download_track(
                self.download_session, track_id, access_token, quality, country
            )
            file_size = os.path.getsize(file_path)
            elapsed = time.time() - start_time
            speed = file_size / (1024 * 1024) / elapsed if elapsed > 0 else 0

            # S3 上传
            ext = os.path.splitext(file_path)[1].lstrip(".")

            # 1. 跳过 M4A 格式
            if ext.lower() in ("m4a", "mp4"):
                logger.warning(f"  ⚠️  [{task_id}] 过滤: 跳过 M4A 格式")
                os.unlink(file_path)
                self._status_queue.put({"task_id": task_id, "status": "completed", "error_message": "Skipped M4A format", "account_id": account_id})
                self.total_downloaded += 1
                self.account_pool.report_success(account_id)
                return

            # 2. 仅保留大于 5MB 的文件
            if file_size <= 5 * 1024 * 1024:
                logger.warning(f"  ⚠️  [{task_id}] 过滤: 文件小于等于 5MB ({file_size} 字节)")
                os.unlink(file_path)
                self._status_queue.put({"task_id": task_id, "status": "completed", "error_message": f"Skipped small file: {file_size} bytes", "account_id": account_id})
                self.total_downloaded += 1
                self.account_pool.report_success(account_id)
                return

            s3_key = ""
            storage_id = ""
            if self.uploader and self.uploader.enabled:
                self._status_queue.put({"task_id": task_id, "status": "uploading", "account_id": account_id})
                storage_id, s3_key = self.uploader.upload(file_path, task, ext)
                os.unlink(file_path)
            else:
                s3_key = file_path

            # 上报完成（含 storage_id）
            self._status_queue.put({
                "task_id": task_id, "status": "completed",
                "file_size": file_size, "actual_quality": actual_quality,
                "codec": codec, "s3_key": s3_key, "account_id": account_id,
                "storage_id": storage_id,
            })

            self.total_downloaded += 1
            self.total_bytes += file_size
            self.account_pool.report_success(account_id)
            logger.info(f"  ✅ [{task_id}] {file_size/(1024*1024):.1f}MB | "
                        f"{elapsed:.1f}s | {speed:.1f}MB/s | {actual_quality}")

        except TokenExpiredError:
            logger.warning(f"  ⚠️  [{task_id}] Token 过期 (acc={account_id})")
            self.server.report_task_status(task_id, "failed",
                                           error_code="TOKEN_EXPIRED",
                                           error_message="Access token expired")
            self.server.report_account_issue(account_id, "token_expired", "Token expired")
            self.account_pool.report_unavailable(account_id, "token_expired")

        except AccountBannedError:
            logger.warning(f"  ⚠️  [{task_id}] 曲目禁止访问 (403, acc={account_id})")
            self.server.report_task_status(task_id, "failed",
                                           error_code="FORBIDDEN",
                                           error_message="Track forbidden (403)")
            self.total_failed += 1

        except TrackNotFoundError:
            logger.warning(f"  ⚠️  [{task_id}] 曲目不存在")
            self.server.report_task_status(task_id, "failed",
                                           error_code="TRACK_NOT_FOUND",
                                           error_message="Track not found (404)")

        except RateLimitError:
            logger.warning(f"  ⚠️  [{task_id}] 429 限流 (acc={account_id})")
            self.server.report_task_status(task_id, "failed",
                                           error_code="RATE_LIMITED",
                                           error_message="Rate limited (429)")
            self.server.report_account_issue(account_id, "rate_limited", "Rate limited (429)")
            self.account_pool.report_rate_limited(account_id)

        except Exception as e:
            logger.error(f"  ❌ [{task_id}] 异常: {e}")
            self.server.report_task_status(task_id, "failed",
                                           error_code="DOWNLOAD_ERROR",
                                           error_message=str(e)[:500])
            self.total_failed += 1

        finally:
            self.active_tasks -= 1
            self.account_pool.release(account_id)


def main():
    parser = argparse.ArgumentParser(description="TIDAL 分布式下载 Worker")
    parser.add_argument("--server", required=True, help="Server URL (如 http://117.55.199.29:8000)")
    parser.add_argument("--name", default="", help="Worker 名称 (默认使用主机名)")

    args = parser.parse_args()

    worker = Worker(
        server_url=args.server,
        name=args.name,
    )
    worker.start()


if __name__ == "__main__":
    main()
