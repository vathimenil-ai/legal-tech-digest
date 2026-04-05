$ProjectDir = 'C:\Users\Nell\Documents\ClaudeCode_WeeklyDigest'
$PS1        = Join-Path $ProjectDir 'run_pipeline_weekly.ps1'

$action = New-ScheduledTaskAction -Execute 'powershell.exe' `
    -Argument "-NoProfile -WindowStyle Hidden -File `"$PS1`""

Set-ScheduledTask -TaskName 'Legal Tech Digest - Weekly' -Action $action
Write-Host 'Weekly task action updated to run_pipeline_weekly.ps1'
