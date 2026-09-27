# market-matrix-updater
自动更新Notion市场数据矩阵

## 🎾 Cheltenham 网球场空位看板

查找 Cheltenham 附近可预订、连续空闲 ≥ 1 小时的网球场 (数据来自 LTA ClubSpark)。

ClubSpark 会拦截 GitHub Actions 等机房 IP, 所以需要在自己电脑上运行 (需要 Python 3.9+)。

### Windows

1. 安装 Python: https://www.python.org/downloads/ (安装时勾选 "Add python.exe to PATH")
2. 下载代码: GitHub 页面上点 Code → Download ZIP, 解压到比如 `C:\Users\<你>\tennis`
3. 双击 `local\windows\install_windows.bat`: 装依赖、先跑一次、加入每 30 分钟的"任务计划程序"、打开看板

- 看板: 双击 `tennis.html` (开着会每 10 分钟自动刷新)
- 马上更新一次: 双击 `local\windows\update_now.bat`
- 日志: `local\tennis.log`
- 卸载定时任务: 双击 `local\windows\uninstall_windows.bat`
- 后台运行不会弹窗; 电脑关机/睡眠时错过的更新, 开机后会补跑一次

### macOS / Linux

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
