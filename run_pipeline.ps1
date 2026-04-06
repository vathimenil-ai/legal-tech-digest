# run_pipeline.ps1 — Daily scheduler wrapper for Legal Tech Intelligence Digest
# Registered in Windows Task Scheduler: Mon–Fri at 06:00

$ProjectDir = "C:\Users\Nell\Documents\ClaudeCode_WeeklyDigest"
$Python     = "C:\Users\Nell\Documents\ClaudeCode_WeeklyDigest\.venv\Scripts\python.exe"
$LogFile    = Join-Path $ProjectDir "scheduled_run_$(Get-Date -Format 'yyyyMMdd_HHmm').txt"

Set-Location $ProjectDir

"=== Legal Tech Digest pipeline started: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" |
    Tee-Object -FilePath $LogFile

& $Python pipeline.py --mode daily 2>&1 | Tee-Object -FilePath $LogFile -Append

"=== Pipeline finished: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" |
    Tee-Object -FilePath $LogFile -Append
