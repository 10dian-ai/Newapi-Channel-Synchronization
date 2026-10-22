# NewAPI Channel Synchronization

把一个**主 NewAPI** 的渠道和分组设置，实时同步到一个或多个**子 NewAPI**。

- 主站数据库**只读**：连接以只读方式打开，所有读取都在只读事务里，代码中没有任何写主库的路径
- 子站**完全跟随**主站：渠道的增、改、删和分组设置都会同步过去
- **快**：每秒一次轻量探测（一条校验和查询），发现变化后通常在 1 秒内同步完成并校验
- **改动立即生效**：写库后调用子站自己的接口刷新缓存，不用等子站的定时刷新
- **不怕版本差**：按两边实际的表结构计算字段归属，子站经常升级、主站落后几个版本都能继续同步
- 部署简单：一个 Docker 容器 / 一个二进制，`docker compose pull && docker compose up -d` 即可更新

## 工作原理

```
主库（只读）
  │  每秒一次校验和探测（忽略已用额度等运行时字段，不会被正常流量触发）
  ▼
发现变化 → 等 300ms 合并连续修改 → 在只读事务里读取渠道 + 分组设置
  │
  ├──▶ 子站 A worker ─┐
  └──▶ 子站 B worker ─┤  每个子站独立、互不影响：
                      │   1. 读取子站现状，按字段归属计算最小差异
                      │   2. 一个事务写入：渠道增删改 + 重建受影响渠道的能力表（abilities）
                      │   3. 调用子站 POST /api/channel/fix 刷新渠道缓存；分组设置走 PUT /api/option/
                      │   4. 重新读取子站，确认与主站一致（写后校验）
                      └   失败 → 指数退避重试，连续失败发告警
每 5 分钟全量对账一次，兜底任何遗漏或子站被手动改动的情况
```

## 同步哪些内容

| 内容 | 规则 |
|---|---|
| 渠道配置（key、base_url、models、group、priority、weight、tag、model_mapping、各种 override……） | 跟随主站，主键 id 保持一致 |
| 渠道状态 status | 见下方"状态规则" |
| 多 Key 信息 channel_info | 配置跟随主站；每个 Key 的启用状态按状态规则合并；轮询下标保留子站自己的 |
| 运行时数据（used_quota、balance、response_time、test_time、balance_updated_time、other_info） | **子站自己维护，永不覆盖** |
| 能力表 abilities（路由用） | 按 NewAPI 的规则自动重建，配置了接口时再由子站自己重建一次 |
| 分组设置 | 默认同步 `GroupRatio`、`GroupGroupRatio`、`UserUsableGroups`、`AutoGroups`、`DefaultUseAutoGroup` 与 `group_ratio_setting.*`，可在配置中增减 |
| 其他系统设置（站点名称、支付、邮件等） | 不同步 |

### 状态规则（`channels.status_mode`）

默认 `smart`：

- 主站**手动禁用** → 子站禁用
- 子站**自己自动禁用**的渠道 → 保持禁用，交给子站自己的渠道检测恢复
- 主站的**自动禁用**是主站自己的健康判断，不传给子站（子站会自己判断）

同步服务自己从不把渠道设为"自动禁用"，所以子站上每一个自动禁用都来自子站自己，不会出现被同步卡死在禁用状态的情况。

想让状态与主站完全一致，改成 `mirror`。

### 主站、子站版本不同时

每次对账都会读取两边的实际表结构：

- 两边都有的列：跟随主站
- 子站独有的列（子站版本更新）：归子站自己，不覆盖；新增渠道时如果该列必填，填零值
- 主站独有的列（子站版本更旧）：无法同步，在状态接口里列出
- 子站表结构变化（通常是刚升级）会发告警；`sync.on_schema_change: pause` 可以改成暂停该子站等人确认

能力表的生成规则如果在新版本有变化，子站自己的修复接口会按新规则再重建一次，所以不会因为版本差导致路由错误。

## 快速开始（Docker Compose）

### 1. 准备数据库账号

**主站（只读）** —— MySQL：

```sql
CREATE USER 'newapi_sync_ro'@'%' IDENTIFIED BY '换成强密码';
GRANT SELECT ON `new-api`.`channels`  TO 'newapi_sync_ro'@'%';
GRANT SELECT ON `new-api`.`abilities` TO 'newapi_sync_ro'@'%';
GRANT SELECT ON `new-api`.`options`   TO 'newapi_sync_ro'@'%';
```

PostgreSQL：

```sql
CREATE ROLE newapi_sync_ro LOGIN PASSWORD '换成强密码';
GRANT CONNECT ON DATABASE "new-api" TO newapi_sync_ro;
GRANT USAGE ON SCHEMA public TO newapi_sync_ro;
GRANT SELECT ON channels, abilities, options TO newapi_sync_ro;
```

PostgreSQL 连接还会在会话级设置 `default_transaction_read_only=on`，即使账号有写权限也写不进去。

**子站（读写这三张表）** —— MySQL：

```sql
CREATE USER 'newapi_sync'@'%' IDENTIFIED BY '换成强密码';
GRANT SELECT, INSERT, UPDATE, DELETE ON `new-api`.`channels`  TO 'newapi_sync'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE ON `new-api`.`abilities` TO 'newapi_sync'@'%';
GRANT SELECT, INSERT, UPDATE         ON `new-api`.`options`   TO 'newapi_sync'@'%';
```

PostgreSQL：

```sql
CREATE ROLE newapi_sync LOGIN PASSWORD '换成强密码';
GRANT CONNECT ON DATABASE "new-api" TO newapi_sync;
GRANT USAGE ON SCHEMA public TO newapi_sync;
GRANT SELECT, INSERT, UPDATE, DELETE ON channels, abilities, options TO newapi_sync;
GRANT USAGE, SELECT, UPDATE ON SEQUENCE channels_id_seq TO newapi_sync;
```

数据库名 `new-api` 换成你实际使用的名字。

### 2. 在子站创建访问令牌（推荐）

用子站的**超级管理员**账号登录，创建访问令牌，勾选 `channel:operate` 与 `option:write` 两个权限。
旧版本 NewAPI 没有权限勾选，使用"系统访问令牌"，并在配置里填 `user_id`（令牌所属用户的 ID）。

不配置令牌也能同步，但子站要等自己的 `SYNC_FREQUENCY`（默认 60 秒）周期刷新后才生效，建议这种情况下把子站的 `SYNC_FREQUENCY` 调到 10 左右。

### 3. 填写配置并启动

```bash
mkdir newapi-sync && cd newapi-sync
# 下载 docker-compose.yml、config.example.yaml、.env.example 到这个目录
cp config.example.yaml config.yaml
cp .env.example .env
vim .env                      # 填数据库地址、密码、子站地址和令牌

docker compose run --rm newapi-sync check   # 检查连接，并演练一次（只显示差异，不写入）
docker compose up -d                        # 正式启动
docker compose logs -f                      # 看日志
```

`check` 会列出每个子站将要发生的变更、两边版本不一致的列，没问题再启动。第一次接入时建议先备份子站数据库。

如果 NewAPI 的数据库在同一台机器的 Docker 网络里，把 `docker-compose.yml` 里的 `networks` 注释打开，DSN 里直接写数据库容器名。

## 更新

```bash
docker compose pull && docker compose up -d
```

1Panel：在容器列表里对 `newapi-sync` 选择"升级/重建"即可。也可以打开 `docker-compose.yml` 里的 watchtower 注释实现自动更新。

每次推送到 `main` 分支都会自动构建 `ghcr.io/10dian-ai/newapi-channel-synchronization:latest`（amd64 / arm64）；打 `v*` 标签会额外发布版本号镜像和各平台二进制。第一次发布后，如果拉取镜像提示无权限，到 GitHub 仓库的 Packages 页面把镜像设为 Public，或在服务器上 `docker login ghcr.io`。

不用 Docker 的话，见 `deploy/newapi-sync.service`（systemd）。

## 状态与告警

| 地址 | 内容 |
|---|---|
| `GET /healthz` | 主站能正常读取时返回 200，Docker 健康检查使用 |
| `GET /status` | 每个子站的状态、最近一次变更明细、同步延迟、版本差异列（JSON） |
| `GET /metrics` | Prometheus 指标，例如 `newapi_sync_child_lag_seconds`、`newapi_sync_child_consecutive_failures` |

`notify.webhook_url` 配置后，同步连续失败、恢复、子站表结构变化时会 POST 一段 JSON（含 `text` 字段，方便直接对接机器人）。

日志里只记录渠道 id、名称和变化的**列名**，不会出现渠道 Key 或任何字段值。

## 注意事项

- **子站后台不要手动改渠道和分组**：改了也会在下一次对账（默认 5 分钟）时被改回主站的样子。
- 子站多实例部署时，接口调用只会刷新其中一个实例，其他实例按各自的 `SYNC_FREQUENCY` 刷新。
- 主站删除某个分组设置项时，子站上的这一项会保留（日志里会提示），不会自动删除。
- 主站一条渠道都没有时，不会删除子站的渠道（防止主站连错库导致子站被清空）。
- 第一版支持 MySQL 与 PostgreSQL，暂不支持 SQLite。

## 开发

```bash
make test     # 单元测试
make itest    # 集成测试，需要 NEWAPI_SYNC_TEST_MYSQL / NEWAPI_SYNC_TEST_POSTGRES 环境变量
make build    # 编译到 bin/newapi-sync
```

代码结构：

```
cmd/newapi-sync      命令行入口：run / check / version
internal/config      配置加载、默认值、校验（拼错字段、主子库相同等都会直接报错）
internal/dbx         数据库方言：引用、占位符、表结构探测、校验和查询
internal/newapidb    NewAPI 三张表的读取与能力表生成规则
internal/source      主站（只读）：探测与快照
internal/target      子站：事务写入
internal/reconcile   核心规则（纯函数）：字段归属、状态规则、差异计算
internal/newapi      子站管理接口客户端
internal/engine      调度：探测、防抖、分发、重试、写后校验、状态
internal/httpserver  /healthz /status /metrics
internal/itest       连接真实 MySQL / PostgreSQL 的集成测试
```
