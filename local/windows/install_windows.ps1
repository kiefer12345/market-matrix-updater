# 在 Windows 上安装定时任务: 每 30 分钟抓取一次 Cheltenham 网球场空位
#   安装:  双击 install_windows.bat   (或 powershell -ExecutionPolicy Bypass -File install_windows.ps1)
#   卸载:  双击 uninstall_windows.bat (或加 -Uninstall 参数)
param([switch]$Uninstall)

$ErrorActionPreference = 'Stop'
$TaskName = 'Cheltenham Tennis Courts'
$Repo = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path

if ($Uninstall) {
    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Write-Host "[OK] 已删除定时任务 '$TaskName'" -ForegroundColor Green
    } else {
        Write-Host "没有找到定时任务 '$TaskName', 不需要卸载"
    }
    return
}

# 从网上下载的 zip 解压出来的脚本会被标记为"来自网络", 先解除
Get-ChildItem -Path $Repo -Recurse -File -Include *.ps1, *.bat, *.py, *.html |
    Unblock-File -ErrorAction SilentlyContinue

# ---------- 找 Python (3.9 以上) ----------
function Find-Python {
    $candidates = @(
        @{ Exe = 'py';      Args = @('-3') },
        @{ Exe = 'python';  Args = @() },
        @{ Exe = 'python3'; Args = @() }
    )
    foreach ($c in $candidates) {
        if (-not (Get-Command $c.Exe -ErrorAction SilentlyContinue)) { continue }
        try {
            $pyArgs = $c.Args
            $out = & $c.Exe @pyArgs -c 'import sys; print(int(sys.version_info >= (3, 9)))' 2>$null
            if ($LASTEXITCODE -eq 0 -and "$out".Trim() -eq '1') { return $c }
        } catch { }
    }
    return $null
}

$py = Find-Python
if (-not $py) {
    Write-Host '[X] 没找到 Python 3.9 以上版本。' -ForegroundColor Red
    Write-Host '    请到 https://www.python.org/downloads/ 下载安装, 安装时勾选 "Add python.exe to PATH",'
    Write-Host '    或者在 PowerShell 里运行:  winget install Python.Python.3.12'
    Write-Host '    装好后重新双击 install_windows.bat'
    exit 1
}

# ---------- 虚拟环境 + 依赖 ----------
$venv = Join-Path $Repo '.venv'
$venvPython = Join-Path $venv 'Scripts\python.exe'
$venvPythonw = Join-Path $venv 'Scripts\pythonw.exe'

Write-Host '正在创建 Python 虚拟环境 (.venv) 并安装依赖...'
if (-not (Test-Path $venvPython)) {
    $pyArgs = $py.Args
    & $py.Exe @pyArgs -m venv $venv
    if ($LASTEXITCODE -ne 0) { throw '创建虚拟环境失败' }
}
# tzdata: Windows 没有自带时区数据库, 计算英国时间需要它
& $venvPython -m pip install -q --disable-pip-version-check --upgrade requests tzdata
if ($LASTEXITCODE -ne 0) { throw '安装依赖失败, 请检查网络后重试' }

# ---------- 先运行一次 ----------
$script = Join-Path $Repo 'tennis_court_finder.py'
$log = Join-Path $Repo 'local\tennis.log'

Write-Host ''
Write-Host '先运行一次:'
Push-Location $Repo
try {
    & $venvPython $script
    if ($LASTEXITCODE -ne 0) {
        Write-Host '[!] 这次运行有场馆获取失败, 详见上面的输出' -ForegroundColor Yellow
    }
} finally {
    Pop-Location
}

# ---------- 注册定时任务 ----------
# pythonw.exe 在后台运行, 不会每 30 分钟弹出黑色窗口; 输出写进 local\tennis.log
$action = New-ScheduledTaskAction -Execute $venvPythonw `
    -Argument "`"$script`" --log `"$log`"" -WorkingDirectory $Repo
# 每天 00:00 开始, 每 30 分钟重复一次, 持续 24 小时 (= 全天每 30 分钟)
$trigger = New-ScheduledTaskTrigger -Daily -At '00:00'
$trigger.Repetition = (New-ScheduledTaskTrigger -Once -At '00:00' `
    -RepetitionInterval (New-TimeSpan -Minutes 30) -RepetitionDuration (New-TimeSpan -Days 1)).Repetition
# 错过的运行 (关机/睡眠) 在开机后补跑一次; 用电池时也运行
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

try {
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings `
        -Description 'Cheltenham 网球场空位看板: 每 30 分钟更新 tennis_data.json' -Force | Out-Null
} catch {
    Write-Host "[X] 创建定时任务失败: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host '    请右键 install_windows.bat, 选"以管理员身份运行"再试一次'
    exit 1
}

$next = (Get-ScheduledTaskInfo -TaskName $TaskName).NextRunTime
Write-Host ''
Write-Host "[OK] 已安装: 每 30 分钟自动更新 (下次运行: $next)" -ForegroundColor Green
Write-Host "     看板:  $Repo\tennis.html   (双击用浏览器打开, 开着会每 10 分钟自动刷新)"
Write-Host "     日志:  $log"
Write-Host '     马上更新: 双击 local\windows\update_now.bat'
Write-Host '     卸载:     双击 local\windows\uninstall_windows.bat'

Start-Process (Join-Path $Repo 'tennis.html')
