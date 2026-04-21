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
function Write-Err($msg)  { Write-Host "    [erro] $msg" -ForegroundColor Red }
function Test-Command($name) {
    return [bool](Get-Command $name -ErrorAction SilentlyContinue)
}

# --- 1. Python ---
Write-Step "Verificando Python..."
if (-not (Test-Command python)) {
    Write-Err "Python nao foi encontrado. Instale Python 3.10+ antes de continuar:"
    Write-Host "  https://www.python.org/downloads/windows/" -ForegroundColor Red
    Write-Host "  (marque 'Add python.exe to PATH' durante a instalacao)" -ForegroundColor Red
    throw "Python ausente."
}
$pyVer = (python --version 2>&1).ToString().Trim()
Write-Ok "Python detectado: $pyVer"

# --- 2. Git ---
Write-Step "Verificando Git..."
if (-not (Test-Command git)) {
    Write-Err "Git nao foi encontrado. Instale Git para Windows antes de continuar:"
    Write-Host "  https://git-scm.com/download/win" -ForegroundColor Red
    throw "Git ausente."
}
Write-Ok "Git detectado."

# --- 3. Clonar / atualizar repositorio ---
Write-Step "Baixando o agente..."
if (Test-Path $RepoDir) {
    Write-Ok "Ja existe em $RepoDir - atualizando."
    Push-Location $RepoDir
    try {
        git fetch --all --quiet
        git reset --hard origin/main --quiet
    } finally { Pop-Location }
} else {
    git clone --depth 1 $RepoUrl $RepoDir
    Write-Ok "Clonado em $RepoDir"
}

# --- 3b. Validar conteudo do repo ---
Write-Step "Validando conteudo do repositorio..."
$pyproject = Join-Path $RepoDir "pyproject.toml"
$setupPy   = Join-Path $RepoDir "setup.py"
if (-not (Test-Path $pyproject) -and -not (Test-Path $setupPy)) {
    Write-Err "O repositorio nao contem pyproject.toml nem setup.py."
    Write-Host ""
    Write-Host "  Conteudo encontrado em $RepoDir :" -ForegroundColor Yellow
    Get-ChildItem $RepoDir | Select-Object -ExpandProperty Name | ForEach-Object {
        Write-Host "    - $_" -ForegroundColor Yellow
    }
    Write-Host ""
    Write-Host "  Suba estes arquivos na RAIZ do repo no GitHub:" -ForegroundColor Yellow
    Write-Host "    install.ps1" -ForegroundColor Yellow
    Write-Host "    pyproject.toml" -ForegroundColor Yellow
    Write-Host "    README.md" -ForegroundColor Yellow
    Write-Host "    src/octa_migracao_agente/...    (pasta inteira)" -ForegroundColor Yellow
    throw "Repositorio incompleto."
}
Write-Ok "pyproject.toml encontrado."

# --- 4. Instalar via pip ---
Write-Step "Instalando dependencias (pip install --user .)..."
Push-Location $RepoDir
try {
    python -m pip install --user --upgrade pip
    if ($LASTEXITCODE -ne 0) { throw "Falha ao atualizar pip (exit $LASTEXITCODE)." }

    # Desinstala versão antiga primeiro (ignora erro se nao estiver instalada)
    python -m pip uninstall -y octa-migracao-agente 2>$null

    python -m pip install --user --no-warn-script-location --upgrade --force-reinstall --no-deps .
    if ($LASTEXITCODE -ne 0) { throw "Falha em 'pip install .' (exit $LASTEXITCODE)." }

    # Instala dependencias separadamente (sem force, mais rapido)
    python -m pip install --user --no-warn-script-location .
    if ($LASTEXITCODE -ne 0) { throw "Falha ao instalar dependencias (exit $LASTEXITCODE)." }
} finally { Pop-Location }
Write-Ok "Pacote instalado."

# --- 5. Descobrir pasta de Scripts e ajustar PATH ---
Write-Step "Configurando PATH..."

# Pergunta ao Python qual e a pasta real onde os entry-points foram instalados.
$ScriptsDir = (python -c "import sysconfig; print(sysconfig.get_path('scripts', f'{sysconfig.get_default_scheme()}_user'))" 2>$null).Trim()

if (-not $ScriptsDir -or -not (Test-Path $ScriptsDir)) {
    # Fallback: procura por octa-migracao.exe em qualquer Python\PythonXXX\Scripts
    $candidato = Get-ChildItem -Path (Join-Path $env:APPDATA "Python") -Filter "octa-migracao.exe" -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($candidato) { $ScriptsDir = $candidato.DirectoryName }
}

if ($ScriptsDir -and (Test-Path $ScriptsDir)) {
    Write-Ok "Pasta de scripts: $ScriptsDir"

    # PATH da sessao atual
    if ($env:Path -notlike "*$ScriptsDir*") {
        $env:Path = "$ScriptsDir;$env:Path"
        Write-Ok "Adicionado ao PATH desta sessao."
    }

    # PATH persistente (usuario) - sobrevive a fechar o PowerShell
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if (-not $userPath) { $userPath = "" }
    if ($userPath -notlike "*$ScriptsDir*") {
        $novoPath = if ($userPath) { "$userPath;$ScriptsDir" } else { $ScriptsDir }
        [Environment]::SetEnvironmentVariable("Path", $novoPath, "User")
        Write-Ok "Adicionado ao PATH permanente do usuario."
    } else {
        Write-Ok "PATH permanente ja contem essa pasta."
    }
} else {
    Write-Warn "Nao consegui localizar a pasta Scripts do Python automaticamente."
}

# --- 6. Validar comando ---
Write-Step "Validando instalacao..."
if (Test-Command octa-migracao) {
    Write-Ok "Comando 'octa-migracao' disponivel."
    $verInstalada = (python -c "import octa_migracao_agente; print(octa_migracao_agente.__version__)" 2>$null).Trim()
    if ($verInstalada) { Write-Ok "Versao instalada: $verInstalada" }
} else {
    Write-Warn "O comando ainda nao esta visivel nesta sessao do PowerShell."
    Write-Host "  FECHE este PowerShell, abra um NOVO e rode novamente o comando de pareamento." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host " Pronto! Proximo passo: parear com o ERP." -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""