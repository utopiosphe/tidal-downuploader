# TIDAL 分布式下载系统 — Go 重构架构设计

> 目标:彻底解决 Python 版的反复停机、高内存、多进程、连接爆炸问题。
> 决策:server + worker 全部 Go 重写;允许停机迁移;借机重新设计 DB/API。
>
> **已定稿的关键决策**:
> - 冷热分离:**完成即归档(事务内)** — 上报 completed 时同事务写 tasks_archive + 从 tasks 删除
> - Token 刷新:**server 内独立 goroutine** + panic 隔离 + MySQL 咨询锁(多 server 时只一个刷)
> - Worker 下载:**分段写磁盘再传** — 边下边写 NVMe 临时文件,ffmpeg 转封装,传 S3 后删,内存 <200MB

## 一、Python 版的根因(重写要正面解决的)

| # | 问题 | Python 版原因 | Go 版如何根治 |
|---|------|--------------|--------------|
| 1 | 高并发假死(CPU 却不高) | FastAPI 同步路由挤在 40 线程池,并发>40 就排队雪崩 | Go 每请求一个 goroutine(2KB),天然支撑上万并发,无线程池瓶颈 |
| 2 | 单核天花板(GIL) | Python 单进程只用 1 核 | Go runtime 自动多核,单进程吃满所有 CPU |
| 3 | Token 死亡螺旋 | 刷新线程寄生 server 进程,server 卡→token 全过期→失败风暴 | Token 刷新做成**独立后台 goroutine + 独立可执行**,并加**分布式锁**,与请求处理隔离;失败不影响 |
| 4 | worker 内存 6.3GB | 整首歌读进内存(3 份副本)+ ThreadPoolExecutor(1000) 线程泄漏 | **流式下载**:分段边下边写磁盘/直传 S3,内存 <200MB;goroutine 池替代线程 |
| 5 | 单机多 worker 进程 | 每进程独立、各开连接池 | **单进程 + goroutine 池**,一台机一个进程,连接复用 |
| 6 | API 连接爆炸(130 条/机) | 每 worker 独立 HTTP 连接池 | 单进程复用 keep-alive 连接,每机 1-4 条 |
| 7 | 热点表 fetch 慢(2400万行) | tasks 表只增不减 | **冷热分离**:completed 归档,主表恒小 |
| 8 | batch-status 低效(单批36次DB) | 逐 task 多次往返 | 单条 SQL 批量 UPDATE(CASE WHEN / 临时表) |
| 9 | 临时文件泄漏 690G | 上传后未删 | defer 保证删除 + 启动时清理孤儿文件 |

## 二、技术栈

- **Server**: Go + Gin(路由) + sqlx(原生 SQL) + MySQL
- **Worker**: Go 单进程 + goroutine 池,net/http + SOCKS5 代理,AWS SDK v2(S3)
- **Token 刷新**: 独立 goroutine(server 内)或独立 cron 二进制,带 MySQL 咨询锁 `GET_LOCK`
- **配置**: 复用 config 表(key-value JSON),启动加载 + 定时热更新

## 三、DB 冷热分离设计(核心)

**问题**: `tasks` 表 2400 万行,completed 占 2100 万,fetch 抢占查询要扫大表。

**方案**:
```
tasks         — 只存活跃任务(pending/assigned/downloading/uploading/failed)
                完成即移出 → 表恒定在几十万行内,fetch 恒定毫秒级
tasks_archive — completed/dead 归档表(只写不改,供导出/统计)
```

**归档流程**:
- worker 上报 completed → 事务内:写 `tasks_archive` + 从 `tasks` 删除 + `jobs.completed++`
- 或异步:completed 先标记,后台 goroutine 批量搬迁(降低上报延迟)
- 导出/趋势查询走 `tasks_archive`(可再按 job 分区)

**兼容**: 迁移时把现有 2100 万 completed 行搬到 archive,tasks 表瘦身。

## 四、任务分配(消除锁竞争)

Python 版:`UPDATE tasks SET status='assigned' ... LIMIT N`(原子抢占,已较优)。
Go 版保持原子抢占,但:
- 主表小 → 抢占查询快
- jobs 计数**攒批更新**:内存累加,每 1-2 秒 flush 一次 `UPDATE jobs SET completed=completed+N`,消除几百 worker 抢同一行的锁竞争(Python 版 row_lock_waits 129 万的根源)

## 五、Worker 并发模型

```
单进程
 ├── 主 goroutine:拉任务(fetch)填入 channel
 ├── N 个 download goroutine(可配置并发,如 50-200):
 │     从 channel 取任务 → 流式下载分段 → ffmpeg 转封装 → 直传 S3 → 上报
 ├── 1 个 reporter goroutine:攒批上报状态(成功+失败都攒批,掐掉失败风暴)
 ├── 1 个 heartbeat goroutine:心跳 + 配置热更新
 └── 本地账号池(内存,mutex 保护,复用现有选号/冷却逻辑)
```

**内存关键**:流式下载 —— DASH 分段边下边写临时文件,不在内存拼整首歌。

## 六、API 契约(重新设计,更简洁)

保留 worker 依赖的核心端点,合并冗余:
```
POST /api/workers/register        worker 注册,返回 worker_id + 全量配置
POST /api/workers/{id}/heartbeat  心跳(含配置版本号,变了才回配置)
POST /api/tasks/fetch             拉取任务(原子抢占)
POST /api/tasks/report            统一批量上报(合并 status + batch-status + account report)
GET  /api/accounts/available      可用账号列表
--- 管理端 ---
GET  /api/dashboard               总览
GET  /api/jobs, POST /api/jobs/import[-csv]
GET  /api/tasks/export/*          导出(走 archive 表)
POST /api/config                  配置更新
```

**关键改进**:worker 的失败上报也走 `/api/tasks/report` 攒批,不再每次失败发 2 个同步请求(掐掉风暴根源)。

## 七、目录结构

```
tidal-go/
  cmd/server/main.go        server 入口
  cmd/worker/main.go        worker 入口
  cmd/tokend/main.go        (可选)独立 token 刷新守护
  internal/
    config/                 配置加载 + 热更新
    db/                     sqlx 连接、迁移
    models/                 数据结构
    server/
      handlers/             各路由 handler
      router.go
    worker/                 下载/上传/账号池/上报
    tidal/                  TIDAL API 客户端(playbackinfo/DASH/token)
    storage/                S3 多存储上传
  migrations/               DB 迁移(建 archive 表、瘦身)
  docs/ARCHITECTURE.md      本文档
```

## 八、迁移计划(允许停机)

1. Go 版开发完成,本地/测试环境跑通
2. 选低峰期:停 Python server + 全部 worker
3. 跑 DB 迁移:建 tasks_archive,搬迁 completed 数据,tasks 瘦身
4. 起 Go server + Go worker
5. 观察;保留 Python 版代码可回滚

## 九、里程碑

- [ ] M1: 项目骨架 + config + db + models + 迁移脚本
- [ ] M2: server 核心(register/heartbeat/fetch/report + dashboard)
- [ ] M3: worker 核心(流式下载 + S3 + 账号池 + 攒批上报)
- [ ] M4: token 刷新 + 导出 + 管理端补齐
- [ ] M5: 联调 + 压测 + 迁移演练
