# market-matrix-updater

自动更新 Notion 市场数据矩阵，并生成 `data.json` 供 `index.html` 页面展示。

## 数据源

| 类别 | 来源（按顺序尝试，第一个之后都标「备用源」） |
|---|---|
| ETF、指数、比特币、VIX | Yahoo Finance |
| 2 年美债 | 美国财政部收益率曲线 → FRED `DGS2` |
| 10 年美债 | 美国财政部收益率曲线 → FRED `DGS10` → Yahoo `^TNX`（带单位检查） |
| 高收益债利差 | FRED `BAMLH0A0HYM2`（ICE BofA 数据只在 FRED 公开，没有免 key 的替代） |
| WTI 原油现货 | EIA `RWTC` → FRED `DCOILWTICO`（数据有滞后，过期阈值 12 天） |
| Total Put/Call Ratio | Cboe Daily Market Statistics（按日期取） |

美国财政部和 EIA 都是官方原始来源（FRED 的 DGS2/DGS10、DCOILWTICO 就是从这里转载），不需要 key。

## data.json 口径

每个资产自带口径字段，前端只负责渲染：

- `value_type`：price / yield / spread / index / ratio
- `change_type`：percent（涨跌幅）/ absolute（绝对变化）
- `unit`：USD / USD/bbl / points / pp / ratio
- `source`、`source_fallback`：实际数据源，是否用了备用源
- `as_of`：该资产最新数据日期

回看周期按日历日期取基准（该日或之前最近一个有数据的日子）；QTD、YTD 以上季末、上年末为基准。
最新数据超过阈值（默认 6 天）的资产不显示数字，在 `missing` 里写明原因。

## Secrets

- `FRED_API_KEY`：可选。目前只有高收益债利差必须依赖 FRED；配置后走 FRED API（失败再退到 CSV），不配置时直接下载 FRED CSV（在 GitHub Actions 上常超时）。凡是用了 CSV 的资产都会在 `data.json` 标 `source_fallback`，页面显示「备用源」；CSV 也失败则该资产记为缺失并写明原因。可在 https://fred.stlouisfed.org/docs/api/api_key.html 免费申请。
- `NOTION_API_KEY`：写 Notion 用。

## 运行

- 每个工作日 UTC 22:00 自动运行（`update_market_matrix.yml`）。
- 成功资产不足 60% 时保留旧 `data.json`、不写 Notion，任务失败。
- PR 会自动触发试跑（`dry_run.yml`）：先删掉仓库里旧的 `data.json`，再用真实数据跑一遍，上传本次生成的 `data.json` 和完整日志 `dry_run_report.txt`（末尾是核对表），不提交、不写 Notion。运行失败时附件里不会有 `data.json`，只有日志。试跑还会直接从生成的 `data.json` 汇总出概况、资产明细和缺失原因，显示在 PR 检查页的 Annotations 里。
- 本地试跑：`DRY_RUN=1 python market_matrix_updater.py`（可选加 `FRED_API_KEY=...`）
