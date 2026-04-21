"""CLI do agente — comandos parear, run, status, testar-pg, logout."""

from __future__ import annotations

import getpass
import platform
import socket
import sys
from typing import Optional

import typer
from rich.console import Console
from rich.prompt import Confirm, Prompt
from rich.table import Table

from . import __version__
from .config import CONFIG_FILE, AgenteConfig, PgConfig
from .pg_client import testar as pg_testar
from .runner import loop_polling
from .supabase_client import SupabaseClient

app = typer.Typer(
    add_completion=False,
    help="Agente de migração Octa ERP — extrai do banco legado e empurra pro Octa.",
)
console = Console()


def _supabase(cfg: AgenteConfig) -> SupabaseClient:
    return SupabaseClient(cfg.supabase.url, cfg.supabase.anon_key)


def _coletar_pg_interativo(pg: PgConfig) -> PgConfig:
    console.print("\n[bold]Credenciais do PostgreSQL legado[/] (ficam SÓ neste computador):")
    pg.host = Prompt.ask("Host", default=pg.host or "")
    pg.port = int(Prompt.ask("Porta", default=str(pg.port or 5432)))
    pg.database = Prompt.ask("Banco de dados", default=pg.database or "")
    pg.user = Prompt.ask("Usuário", default=pg.user or "")
    pg.password = getpass.getpass("Senha: ") or pg.password
    pg.ssl = Confirm.ask("Usar SSL?", default=pg.ssl)
    return pg


@app.command()
def parear(
    token: str = typer.Option(..., "--token", "-t", help="Token gerado no wizard"),
    nao_interativo: bool = typer.Option(
        False, "--nao-interativo", help="Não pedir credenciais PG (use OCTA_PG_*)"
    ),
) -> None:
    """Pareia este agente com uma sessão do wizard."""
    cfg = AgenteConfig.carregar()

    sb = _supabase(cfg)
    try:
        resp = sb.rpc(
            "fn_migracao_agente_parear",
            {
                "p_token": token,
                "p_nom_maquina": socket.gethostname(),
                "p_nom_usuario_so": getpass.getuser(),
                "p_des_versao_agente": __version__,
                "p_des_so": f"{platform.system()} {platform.release()}",
            },
        )
    finally:
        sb.close()

    row = resp[0] if isinstance(resp, list) and resp else (resp or {})
    status = row.get("des_status_sessao")
    msg = row.get("des_mensagem")
    cod = row.get("cod_agente_sessao")

    if status != "ATIVO":
        console.print(f"[bold red]✗ Pareamento falhou:[/] {msg}")
        raise typer.Exit(code=1)

    cfg.sessao.cod_agente_sessao = int(cod)
    cfg.sessao.token = token

    # PG legado
    if not nao_interativo and (not cfg.pg.host or not cfg.pg.password):
        cfg.pg = _coletar_pg_interativo(cfg.pg)

    cfg.salvar()
    console.print(f"[bold green]✓ Pareado![/] Sessão #{cod} ativa.")
    console.print("Use [bold]octa-migracao run[/] para começar a executar jobs.")


@app.command()
def run(
    intervalo: float = typer.Option(5.0, "--intervalo", help="Polling em segundos"),
) -> None:
    """Roda o loop de polling de jobs."""
    cfg = AgenteConfig.carregar()
    if not cfg.pareado:
        console.print("[red]Agente não pareado.[/] Rode `octa-migracao parear --token …` antes.")
        raise typer.Exit(code=1)
    loop_polling(cfg, intervalo=intervalo)


@app.command()
def status() -> None:
    """Mostra status da sessão e jobs recentes."""
    cfg = AgenteConfig.carregar()
    if not cfg.pareado:
        console.print("[yellow]Não pareado.[/]")
        raise typer.Exit()

    sb = _supabase(cfg)
    try:
        info = sb.select_view_sessao(cfg.sessao.cod_agente_sessao)  # type: ignore[arg-type]
    finally:
        sb.close()

    if not info:
        console.print("[red]Sessão não encontrada no servidor.[/]")
        raise typer.Exit(code=1)

    t = Table(title=f"Sessão #{info['cod_agente_sessao']}", show_header=False)
    t.add_column("Campo", style="bold")
    t.add_column("Valor")
    t.add_row("Status", info["des_status_sessao"])
    t.add_row("Online?", "✓ sim" if info["ind_online"] else "✗ não")
    t.add_row("Máquina", info.get("nom_maquina") or "—")
    t.add_row("SO", info.get("des_so") or "—")
    t.add_row("Versão agente", info.get("des_versao_agente") or "—")
    t.add_row("Pareamento", str(info.get("dta_pareamento") or "—"))
    t.add_row("Último heartbeat", str(info.get("dta_ultimo_heartbeat") or "—"))
    t.add_row("Jobs pendentes", str(info["qtd_jobs_pendentes"]))
    t.add_row("Jobs executando", str(info["qtd_jobs_executando"]))
    console.print(t)


@app.command("testar-pg")
def testar_pg() -> None:
    """Testa conexão com o PostgreSQL legado configurado."""
    cfg = AgenteConfig.carregar()
    if not cfg.pg.host:
        console.print("[red]PG não configurado.[/] Rode `octa-migracao parear` primeiro.")
        raise typer.Exit(code=1)
    try:
        meta = pg_testar(cfg.pg)
        console.print("[green]✓ Conectado![/]")
        console.print(meta)
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]✗ Falhou:[/] {exc}")
        raise typer.Exit(code=1)


@app.command("configurar-legado")
def configurar_legado(
    host: Optional[str] = typer.Option(None, "--host", help="Host do Postgres legado"),
    port: Optional[int] = typer.Option(None, "--port", help="Porta (padrão 5432)"),
    database: Optional[str] = typer.Option(None, "--database", "--db", help="Nome do banco"),
    user: Optional[str] = typer.Option(None, "--user", "-u", help="Usuário"),
    password: Optional[str] = typer.Option(
        None, "--password", "-p", help="Senha (se omitido, será pedido com prompt oculto)"
    ),
    ssl: Optional[bool] = typer.Option(
        None, "--ssl/--no-ssl", help="Usar SSL na conexão (padrão: ssl)"
    ),
    testar: bool = typer.Option(
        True, "--testar/--nao-testar", help="Testar conexão após salvar"
    ),
) -> None:
    """Configura (ou atualiza) a conexão com o Postgres legado.

    As credenciais ficam SÓ neste computador (~/.octa-migracao/config.toml, chmod 600).
    Nunca são enviadas para a Supabase.
    """
    cfg = AgenteConfig.carregar()

    # aplica flags fornecidas
    if host is not None:
        cfg.pg.host = host
    if port is not None:
        cfg.pg.port = port
    if database is not None:
        cfg.pg.database = database
    if user is not None:
        cfg.pg.user = user
    if password is not None:
        cfg.pg.password = password
    if ssl is not None:
        cfg.pg.ssl = ssl

    # fallback interativo para o que ficou faltando
    if not cfg.pg.host:
        cfg.pg.host = Prompt.ask("Host")
    if not cfg.pg.port:
        cfg.pg.port = int(Prompt.ask("Porta", default="5432"))
    if not cfg.pg.database:
        cfg.pg.database = Prompt.ask("Banco de dados")
    if not cfg.pg.user:
        cfg.pg.user = Prompt.ask("Usuário")
    if not cfg.pg.password:
        cfg.pg.password = getpass.getpass("Senha: ")
    if ssl is None and not cfg.pg.host.endswith("supabase.co"):
        # só pergunta se não veio por flag e não é supabase
        cfg.pg.ssl = Confirm.ask("Usar SSL?", default=cfg.pg.ssl)

    cfg.salvar()
    console.print(
        f"[green]✓ Configuração salva[/] em [dim]{CONFIG_FILE}[/]\n"
        f"  host={cfg.pg.host}  port={cfg.pg.port}  db={cfg.pg.database}  "
        f"user={cfg.pg.user}  ssl={cfg.pg.ssl}"
    )

    if testar:
        console.print("\n[bold]Testando conexão…[/]")
        try:
            meta = pg_testar(cfg.pg)
            console.print("[green]✓ Conectado![/]")
            t = Table(show_header=False)
            t.add_column("Campo", style="bold")
            t.add_column("Valor")
            for k, v in meta.items():
                t.add_row(k, str(v))
            console.print(t)
        except Exception as exc:  # noqa: BLE001
            console.print(f"[red]✗ Falhou na conexão:[/] {exc}")
            console.print(
                "[yellow]Configuração foi salva mesmo assim.[/] "
                "Corrija com `octa-migracao configurar-legado` e teste com `octa-migracao testar-pg`."
            )
            raise typer.Exit(code=1)


@app.command()
def logout() -> None:
    """Apaga sessão local. (Para revogar no servidor, use o wizard.)"""
    cfg = AgenteConfig.carregar()
    if Confirm.ask("Apagar config local (sessão e PG)?", default=False):
        cfg.limpar_sessao()
        cfg.pg = PgConfig()
        cfg.salvar()
        console.print("[green]Sessão local apagada.[/]")


@app.command()
def versao() -> None:
    """Mostra versão do agente."""
    console.print(f"octa-migracao-agente v{__version__}")


if __name__ == "__main__":
    app()


@app.command()
def versao() -> None:
    """Mostra versão do agente."""
    console.print(f"octa-migracao-agente v{__version__}")


if __name__ == "__main__":
    app()