# =============================================================================
# Octa ERP — Instalador do Agente de Migração (Windows / PowerShell)
# =============================================================================
# Uso:
#   powershell -ExecutionPolicy Bypass -Command "iwr -useb https://raw.githubusercontent.com/raphaeljordao86/octa-erp-octa-migracao-agente/main/install.ps1 | iex"
# =============================================================================

$ErrorActionPreference = "Stop"

$RepoUrl  = "https://github.com/raphaeljordao86/octa-erp-octa-migracao-agente.git"
$RepoDir  = Join-Path $env:LOCALAPPDATA "octa-migracao-agente"

function Write-Step($msg) {
    Write-Host ""
    Write-Host "==> $msg" -ForegroundColor Cyan
}
function Write-Ok($msg)   { Write-Host "    [ok] $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "    [aviso] $msg" -ForegroundColor Yellow }
function Test-Command($name) {
    return [bool](Get-Command $name -ErrorAction SilentlyContinue)
}

# --- 1. Python ---
Write-Step "Verificando Python..."
if (-not (Test-Command python)) {
    Write-Host ""
    Write-Host "Python não foi encontrado. Instale Python 3.10+ antes de continuar:" -ForegroundColor Red
    Write-Host "  https://www.python.org/downloads/windows/" -ForegroundColor Red
    Write-Host "  (marque 'Add python.exe to PATH' durante a instalação)" -ForegroundColor Red
    throw "Python ausente."
}
$pyVer = (python --version 2>&1).ToString().Trim()
Write-Ok "Python detectado: $pyVer"

# --- 2. Git ---
Write-Step "Verificando Git..."
if (-not (Test-Command git)) {
    Write-Host ""
    Write-Host "Git não foi encontrado. Instale Git para Windows antes de continuar:" -ForegroundColor Red
    Write-Host "  https://git-scm.com/download/win" -ForegroundColor Red
    throw "Git ausente."
}
Write-Ok "Git detectado."

# --- 3. Clonar / atualizar repositório ---
Write-Step "Baixando o agente..."
if (Test-Path $RepoDir) {
    Write-Ok "Já existe em $RepoDir — atualizando."
    Push-Location $RepoDir
    try {
        git fetch --all --quiet
        git reset --hard origin/main --quiet
    } finally { Pop-Location }
} else {
    git clone --depth 1 $RepoUrl $RepoDir
    Write-Ok "Clonado em $RepoDir"
}

# --- 4. Instalar via pip ---
Write-Step "Instalando dependências (pip install --user .)..."
Push-Location $RepoDir
try {
    python -m pip install --user --upgrade pip --quiet
    python -m pip install --user . --quiet
} finally { Pop-Location }
Write-Ok "Pacote instalado."

# --- 5. PATH ---
Write-Step "Configurando PATH..."
$ScriptsDir = Join-Path $env:APPDATA "Python\Scripts"
if (Test-Path $ScriptsDir) {
    if ($env:Path -notlike "*$ScriptsDir*") {
        $env:Path = "$ScriptsDir;$env:Path"
        Write-Ok "Adicionado $ScriptsDir ao PATH desta sessão."
    } else {
        Write-Ok "PATH já contém $ScriptsDir."
    }
} else {
    Write-Warn "Pasta $ScriptsDir não existe ainda. Se o comando 'octa-migracao' não for reconhecido, abra um novo PowerShell."
}

# --- 6. Validar ---
Write-Step "Validando instalação..."
if (Test-Command octa-migracao) {
    Write-Ok "Comando 'octa-migracao' disponível."
} else {
    Write-Warn "Comando ainda não está no PATH. Tente abrir um NOVO PowerShell e rodar de novo:"
    Write-Host "  octa-migracao parear --token <SEU_TOKEN>" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host " Pronto! Próximo passo: parear com o ERP." -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""