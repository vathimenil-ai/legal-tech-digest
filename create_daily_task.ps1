$ProjectDir = 'C:\Users\Nell\Documents\ClaudeCode_WeeklyDigest'
$PS1        = Join-Path $ProjectDir 'run_pipeline.ps1'

# Daily trigger: Mon–Fri at 07:00
$trigger = New-ScheduledTaskTrigger -Weekly -WeeksInterval 1 `
    -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday -At '07:00'

$action = New-ScheduledTaskAction -Execute 'powershell.exe' `
    -Argument "-NoProfile -WindowStyle Hidden -File `"$PS1`""

$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -StartWhenAvailable

Register-ScheduledTask `
    -TaskName 'Legal Tech Digest - Daily' `
    -Trigger $trigger `
    -Action $action `
    -Settings $settings `
    -Force

Write-Host 'Daily task created: Mon-Fri 07:00'
