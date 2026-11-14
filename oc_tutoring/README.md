# OC 补习班口碑排行

把网上能找到的、关于 NSW OC（Opportunity Class）补习班的家长口碑收集成一条条**带出处的证据**，
再按「来源可信度 × 年份 × 核实程度 × 和 OC 的相关度」加权，算出每家机构的口碑分。

- 结果看 [`report.md`](report.md)：总排行 + 每家机构的全部证据、链接和每条证据的权重拆解。
- 程序可读的同一份结果在 `ranking.json`。

## 运行

```bash
python oc_tutoring/oc_ranker.py
```

```bash
python oc_tutoring/oc_ranker.py --check
```

```bash
python oc_tutoring/oc_ranker.py --year 2027
```

加 `--focus selective` 改按 Selective（精英中学）考试排，生成 `report_selective.md` / `ranking_selective.json`：
讲 Selective 的证据按 1.0 计，讲 OC 的按 0.7 计。

第一条重新生成 `report.md` / `ranking.json`；`--check` 只校验数据；
`--year` 换参考年份（明年再跑，旧证据会自动继续衰减）。只用 Python 标准库，不需要装依赖。

## 文件

| 文件 | 作用 |
|---|---|
| `data/centres.json` | 机构名单（名称、别名、地区、形式、备注） |
| `data/evidence.json` | 证据库：每条一个来源链接、年份、立场、摘要、原话 |
| `data/sources.json` | 已知的论坛帖子清单，`collect.py` 按它去抓 |
| `weights.json` | 全部权重，改这里就能换一套判断标准 |
| `oc_ranker.py` | 打分并生成报告 |
| `add_evidence.py` | 往证据库里追加机构和证据（见下） |
| `collect.py` | 抓取论坛帖子，找出提到机构的段落，生成待人工确认的候选证据 |

## 打分规则

每条证据的得分 = **立场分 × 来源权重 × 年份衰减 × 核实程度 × OC 相关度**。

- **立场分**：推荐 +1，正面 +0.7，仅被提及 +0.3，褒贬参半 0，负面 −0.7，劝退 −1。
  聚合评分（如 Google 4.8/5）按 (评分−3)/2 换算，并按评论条数打折。
- **来源权重**：家长论坛 1.0；家长群/小红书 0.9；学生论坛、点评站 0.7；媒体 0.5；
  榜单/导购博客 0.25；员工评价 0.2；**机构自述 0.05**。
- **年份衰减**：半衰期 3 年。今年的证据权重 1.0，3 年前 0.5，6 年前 0.25，最低 0.1。
- **核实程度**：读过原帖 1.0；只看到搜索引擎摘要 0.7；转述/推断 0.4。
- **OC 相关度**：直接讲 OC 的 1.0；讲小学阶段精英/Selective 备考的 0.7；泛泛的 0.4。
- **同帖阻尼**：同一个帖子对同一家机构的第 2、3… 条意见依次乘 0.6，防止一个帖子刷分。

排行里另有两列帮助判断：

- **好评占比**：加权后正面意见 ÷ 全部表态。分数高但占比低，说明争议大。
- **可信度**：独立来源 ≥4 且最近证据不早于 2023 为「高」；≥2 且不早于 2021 为「中」；其余「低」。

## 追加证据

`add_evidence.py` 从标准输入读，每行一条，用 `|` 分隔。把下面两行存成 `new.txt`：

```text
C|new_centre|New Centre Name|中文名|Epping,线上|小班|备注
E|new_centre|parent_group|微信群「2027 OC 备考」|2026|recommend|oc|original_read|wechat://2027-oc-备考|群聊截图|三位家长都说老师讲题细|讲题很细
```

然后：

```bash
python oc_tutoring/add_evidence.py < new.txt
```

```bash
python oc_tutoring/oc_ranker.py
```

- `C` 行：机构 id、英文名、中文名/别名、地区（逗号分隔）、形式、备注。
- `E` 行：机构 id、来源类型、平台、年份、立场、相关度、核实程度、链接、帖子标题、摘要、原话（可空）。
- 年份写 `2024?` 表示是估计值；留空表示不明（权重按 0.25）。
- 立场可写 `rating:4.8:103` 表示评分 4.8、共 103 条。
- 微信群、小红书、Facebook 群这类没有公开链接的来源，链接填群名或截图存放位置即可，
  来源类型选 `parent_group`，核实程度选 `original_read`（你自己看到的）。

## 自动采集

```bash
python oc_tutoring/collect.py
```

```bash
python oc_tutoring/collect.py --url "https://www.oursteps.com.au/bbs/forum.php?mod=viewthread&tid=1649709"
```

`collect.py` 下载 `data/sources.json` 里的帖子（或 `--url` 指定的一个），按机构名和别名找出提到机构的段落，
带上发帖年份和粗略的褒贬判断，写到 `data/candidates.json`。
**候选不会自动进入排行**：褒贬判断只是关键词匹配，需要人看过再用 `add_evidence.py` 录入。

## 社区口碑采集（自动）

新的采集流程在 `community/`，说明见 [`community/README.md`](community/README.md)。
它产生的线索和候选存在 `data/community/`，和正式证据库分开，不影响评分。

## 这份数据的局限

- **有口碑的机构只有三十多家**：新足迹「小学教育」版和「中学教育」版里所有和补习相关的帖子（约 330 个、一万多条回帖）
  都已抓取并检索过，被两个以上帖子点名的机构都已收录。其余五十多家来自导购名录、黄页和官网，
  只能证明它们存在，没有家长评价，在报告里单独列出、不参与排名。
- **回帖没有逐条读完**：早期抓的约 125 个帖子是通读的；后抓的约 200 个帖子只读了提到已知机构、
  且带评价词的回帖，每家机构优先读最近的二三十条。Pre-Uni 的回帖最多，约有四百多条没读。
- **论坛证据已对照原帖**：新足迹、OzBargain、Bored of Studies 的证据是读了原帖后按原话录入的。
  Whirlpool 和 china2au 的网站拒绝访问；ProductReview、Google 评分和名录信息仍来自搜索摘要或聚合页。
- **微信群、小红书、Facebook 群的内容不公开，采不到**。小型补习社和私人老师的口碑主要在这些地方，
  建议把自己群里看到的评价用 `add_evidence.py` 补进来。
- **论坛里有水军和推荐码**：多个帖子里有家长指某些机构在论坛有小号（帖中点名过 Scholarly、Eduvision），
  CTEdu、Kedu 的回帖常带推荐码。录入时已尽量只取有具体经历的发言，但无法完全排除。
- **有的机构已经变了**：帖子里说叶老师补习社在考试改革后已关闭，何老师已把 North Parramatta 的补习社卖掉，
  Eduvision 的招牌老师 2024 年似乎已离开。旧帖的好评不代表现状。
- 口碑随校区和老师差别很大，排行只能缩小范围，最后还是要试听。
