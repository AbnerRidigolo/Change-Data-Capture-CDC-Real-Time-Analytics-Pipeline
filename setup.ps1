# PowerShell Automation Script for CDC Pipeline

# Set script directory
$PSScriptRoot = Split-Path -Parent -Path $MyInvocation.MyCommand.Definition
Set-Location -Path $PSScriptRoot

Write-Host "=== INICIANDO SUBIDA DO AMBIENTE CDC ===" -ForegroundColor Green

# 1. Start docker containers
Write-Host "Iniciando Docker Containers..." -ForegroundColor Cyan
docker compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Error "Falha ao iniciar o Docker Compose. Certifique-se de que o Docker Desktop está rodando."
    exit 1
}

# 2. Wait for Debezium Kafka Connect to start
Write-Host "Aguardando o Kafka Connect (Debezium) iniciar no http://localhost:8083..." -ForegroundColor Cyan
$max_attempts = 30
$attempt = 1
$connect_ready = $false

while ($attempt -le $max_attempts) {
    try {
        $resp = Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Get -ErrorAction Stop
        $connect_ready = $true
        break
    } catch {
        Write-Host "Tentativa ${attempt}/${max_attempts}: Kafka Connect não está pronto ainda..." -ForegroundColor Yellow
        Start-Sleep -Seconds 5
        $attempt++
    }
}

if (-not $connect_ready) {
    Write-Error "Kafka Connect não subiu no tempo limite estabelecido."
    exit 1
}

Write-Host "Kafka Connect pronto!" -ForegroundColor Green

# 3. Register the Debezium Postgres Connector
Write-Host "Registrando o conector do PostgreSQL no Debezium..." -ForegroundColor Cyan

# Check if connector is already registered and healthy
$register_needed = $true
try {
    $existing = Invoke-RestMethod -Uri "http://localhost:8083/connectors/postgres-connector" -Method Get -ErrorAction SilentlyContinue
    if ($existing) {
        # Check connector and task states
        $status = Invoke-RestMethod -Uri "http://localhost:8083/connectors/postgres-connector/status" -Method Get -ErrorAction Stop
        $conn_state = $status.connector.state
        $task_state = $status.tasks[0].state
        
        if ($conn_state -eq "RUNNING" -and $task_state -eq "RUNNING") {
            Write-Host "Conector já registrado anteriormente e saudável (RUNNING). Pulando registro..." -ForegroundColor Yellow
            $register_needed = $false
        } else {
            Write-Host "Conector existente está em estado instável (Connector: $conn_state, Task: $task_state). Recriando..." -ForegroundColor Orange
            $null = Invoke-RestMethod -Uri "http://localhost:8083/connectors/postgres-connector" -Method Delete -ErrorAction Stop
            Start-Sleep -Seconds 3
        }
    }
} catch {
    # If endpoint throws or is missing, proceed to register
}

if ($register_needed) {
    $config_json = Get-Content -Raw -Path "debezium/register_connector.json"
    try {
        $resp = Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Post -Body $config_json -ContentType "application/json"
        Write-Host "Conector 'postgres-connector' registrado com sucesso!" -ForegroundColor Green
    } catch {
        Write-Error "Falha ao registrar o conector do Debezium: $_"
        exit 1
    }
}

Write-Host "`n=== AMBIENTE PRONTO E OPERACIONANTE! ===" -ForegroundColor Green
Write-Host "1. Interface Redpanda Console: http://localhost:8085" -ForegroundColor Cyan
Write-Host "2. Para rodar o Simulador de Transações: python scripts/generator.py" -ForegroundColor Cyan
Write-Host "3. Para rodar a Verificação de Consistência: python scripts/verify.py" -ForegroundColor Cyan
