# run_pipeline.ps1 — Daily scheduler wrapper for Legal Tech Intelligence Digest
# Registered in Windows Task Scheduler: Mon–Fri at 06:00

$ProjectDir = "C:\Users\Nell\Documents\ClaudeCode_WeeklyDigest"
$Python     = "C:\Users\Nell\Documents\ClaudeCode_WeeklyDigest\.venv\Scripts\python.exe"
$LogFile    = Join-Path $ProjectDir "scheduled_run_$(Get-Date -Format 'yyyyMMdd_HHmm').txt"

Set-Location $ProjectDir

"=== Legal Tech Digest pipeline started: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ===" |
    Tee-Object -FilePath $LogFile

$MaxAttempts = 3
$RetryDelaySecs = 180
$Attempt = 0
$ExitCode = 1

while ($Attempt -lt $MaxAttempts -and $ExitCode -ne 0) {
    $Attempt++
    if ($Attempt -gt 1) {
        "--- Attempt $Attempt of $MaxAttempts (retrying after SSL error) ---" |
            Tee-Object -FilePath $LogFile -Append
    }
    & $Python pipeline.py --mode daily 2>&1 | Tee-Object -FilePath $LogFile -Append
    $ExitCode = $LASTEXITCODE
    if ($ExitCode -ne 0 -and $Attempt -lt $MaxAttempts) {
        "--- Attempt $Attempt failed (exit $ExitCode). Waiting $RetryDelaySecs s before retry... ---" |
            Tee-Object -FilePath $LogFile -Append
        Start-Sleep -Seconds $RetryDelaySecs
    }
}

"=== Pipeline finished: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') (exit $ExitCode after $Attempt attempt(s)) ===" |
    Tee-Object -FilePath $LogFile -Append
