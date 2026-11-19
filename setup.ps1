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

$registration = Get-Content -Raw -LiteralPath "debezium/register_connector.json" | ConvertFrom-Json
try {
    $configJson = $registration.config | ConvertTo-Json -Depth 10
    $null = Invoke-RestMethod -Uri "http://localhost:8083/connectors/$($registration.name)/config" -Method Put -Body $configJson -ContentType "application/json" -ErrorAction Stop
    Write-Host "Configuração do conector aplicada com sucesso!" -ForegroundColor Green
} catch {
    Write-Error "Falha ao configurar o conector do Debezium: $_"
    exit 1
}

Write-Host "`n=== AMBIENTE PRONTO E OPERACIONANTE! ===" -ForegroundColor Green
Write-Host "1. Interface Redpanda Console: http://localhost:8085" -ForegroundColor Cyan
Write-Host "2. Para rodar o Simulador de Transações: python scripts/generator.py" -ForegroundColor Cyan
Write-Host "3. Para rodar a Verificação de Consistência: python scripts/verify.py" -ForegroundColor Cyan
