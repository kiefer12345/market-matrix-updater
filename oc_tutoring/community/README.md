# 社区口碑采集

把「发现线索 → 读原文 → 去重 → 质量门槛 → 候选库」做成可重复运行的流程。
它和正式证据库（`data/evidence.json`）是隔离的：这里的任何命令都不会改证据库，所以不会改变评分。

## 运行

```bash
python oc_tutoring/community_run.py status
```

```bash
python oc_tutoring/community_run.py run --dry-run
```

```bash
python oc_tutoring/community_run.py run
```

```bash
python oc_tutoring/community_run.py run --source oursteps
```

```bash
python -m unittest discover -s oc_tutoring/tests -v
```

只用 Python 标准库。没有任何 API Key 时，免费来源照常运行，需要凭据的来源在报告里显示为跳过。

## 来源

在 `community/sources.json` 里逐个开关（`enabled`）。

| 来源 | 作用 | 需要 | 说明 |
|---|---|---|---|
| `oursteps` | 发现并读取帖子 | 无 | 扫小学、中学教育版的列表页，按标题关键词筛选，逐帖读回帖 |
| `user_import` | 导入你提供的材料 | 无 | 读 `data/community/inbox/*.json` |
| `brave` | 只发现线索 | `BRAVE_API_KEY` 和 `COMMUNITY_ALLOW_PAID=1` | Brave 条款不允许存储搜索结果，所以线索不落盘 |
| `openai_web_search` | 只发现线索 | `OPENAI_API_KEY` 和 `COMMUNITY_ALLOW_PAID=1` | 返回的是模型写的摘要，不是原帖，永远过不了门槛 |
| `reddit_api` | 读取 Reddit 帖子和评论 | 凭据和 `REDDIT_API_APPROVED=1` | 默认关闭；Reddit 批准之前不要打开 |

凭据只从环境变量读，不写进代码或配置。

## 数据文件（`data/community/`）

| 文件 | 内容 | 会不会计分 |
|---|---|---|
| `leads.jsonl` | 发现但没读到正文的线索 | 不会 |
| `candidates.jsonl` | 读到的回帖，等人工审核 | 不会 |
| `state.json` | 每个帖子读到哪里，保证重跑不重复 | — |
| `runs.jsonl` | 每次运行各来源的结果 | — |

每条候选的主要字段：`platform`、`url`、`centres`、`centre_named_in_post`、`published` 和 `published_precision`
（day / month / year / unknown，不猜）、`discovered_at`、`fetched_at`、`acquisition`（direct_fetch / official_api /
user_import / llm_search / search_index）、`completeness`、`independent`、`promo_suspect`、`first_hand`、
`verifiability`、`storage_allowed`、`duplicate_of`、`matches_evidence`、`scoring_units`、`passes_gate`、
`gate_reasons`、`eligible_for_scoring`、`review_status`。

## 质量门槛

一条候选要同时满足下面几条才算「过门槛」：是我们自己读到的（或你提供的）、是完整回帖、回帖正文里点了机构名、
有第一手经历的表述、不像广告、不是重复、不是证据库里已有的。过门槛只表示值得人工看，`eligible_for_scoring`
始终是 false，直到有人审核。

同一个帖子里多位家长讲同一家机构，`scoring_units` 相同（`机构@帖子`），审核时应合并成一条，不要累计。

## 把候选变成正式证据

```bash
python oc_tutoring/community_run.py review --key "oursteps:1234567#890" --set approved
```

```bash
python oc_tutoring/community_run.py export-approved
```

第二条命令输出 `add_evidence.py` 能读的行，立场、相关度和摘要留给人填。录入后评分才会变。

## 导入你提供的材料

在 `data/community/inbox/` 放一个 JSON 文件：

```json
{"platform": "xiaohongshu", "url": "https://...", "title": "笔记标题", "exported_at": "2026-11-14",
 "via": "手工复制", "posts": [{"id": "1", "published": "2026-10-02", "text": "正文……"}]}
```

没有公开链接时 `url` 留空。程序不会自己去读任何私人群组，只处理放在这里的文件。

## 不做的事

不绕过登录、验证码或反爬；不使用未经批准的第三方抓取服务；不采集私人群组；没有 `COMMUNITY_ALLOW_PAID=1` 不发任何付费请求。
