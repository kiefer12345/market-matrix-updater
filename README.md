# market-matrix-updater

自动更新 Notion 市场数据矩阵，并生成 `data.json` 供 `index.html` 页面展示。

## 数据源

| 类别 | 来源 |
|---|---|
| ETF、指数、比特币、VIX | Yahoo Finance |
| 2 年美债 | FRED `DGS2` |
| 10 年美债 | FRED `DGS10`（失败时用 Yahoo `^TNX`，页面标「备用源」） |
| 高收益债利差 | FRED `BAMLH0A0HYM2` |
| WTI 原油现货 | FRED `DCOILWTICO`（EIA 数据有滞后，过期阈值 12 天） |
| Total Put/Call Ratio | Cboe Daily Market Statistics（按日期取） |

## data.json 口径

每个资产自带口径字段，前端只负责渲染：

- `value_type`：price / yield / spread / index / ratio
- `change_type`：percent（涨跌幅）/ absolute（绝对变化）
- `unit`：USD / USD/bbl / points / pp / ratio
- `source`、`source_fallback`：实际数据源，是否用了备用源
- `as_of`：该资产最新数据日期

回看周期按日历日期取基准（该日或之前最近一个有数据的日子）；QTD、YTD 以上季末、上年末为基准。
最新数据超过阈值（默认 6 天）的资产不显示数字，在 `missing` 里写明原因。

## 必须配置的 Secrets

- `FRED_API_KEY`：必需，没有时任务直接失败。在 https://fred.stlouisfed.org/docs/api/api_key.html 免费申请。
- `NOTION_API_KEY`：写 Notion 用。

## 运行

- 每个工作日 UTC 22:00 自动运行（`update_market_matrix.yml`）。
- 成功资产不足 60% 时保留旧 `data.json`、不写 Notion，任务失败。
- PR 会自动触发试跑（`dry_run.yml`）：用真实数据跑一遍、在日志末尾打印核对表、上传 `data.json`，不提交、不写 Notion。
- 本地试跑：`DRY_RUN=1 FRED_API_KEY=xxx python market_matrix_updater.py`
