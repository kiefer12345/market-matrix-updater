# market-matrix-updater
自动更新Notion市场数据矩阵

## 🎾 Cheltenham 网球场空位看板

查找 Cheltenham 附近可预订、连续空闲 ≥ 1 小时的网球场 (数据来自 LTA ClubSpark)。

ClubSpark 会拦截 GitHub Actions 等机房 IP, 所以需要在自己电脑上运行 (macOS / Linux, 需要 Python 3.9+):

```bash
git clone https://github.com/kiefer12345/market-matrix-updater.git ~/code/market-matrix-updater
cd ~/code/market-matrix-updater
./local/install_cron.sh            # 装依赖、先跑一次、加入每 30 分钟的定时任务、打开看板
```

- 看板: 双击 `tennis.html` 用浏览器打开 (开着会每 10 分钟自动刷新)
- 手动更新一次: `./local/run_tennis.sh`
- 日志: `local/tennis.log`
- 卸载定时任务: `./local/install_cron.sh --uninstall`
- 要查哪些场馆在 `tennis_court_finder.py` 的 `VENUES` 里改
- macOS: 仓库不要放在 Documents / Desktop / Downloads 里, 否则 cron 没有权限访问 (或者给 `/usr/sbin/cron` 开"完全磁盘访问权限")
