"""Loop de execução de jobs."""

from __future__ import annotations

import time
import traceback
from typing import Optional

from rich.console import Console

from .config import AgenteConfig
from .entidades import get_entidade
from .pg_client import stream_rows
from .supabase_client import SupabaseClient

console = Console()
HEARTBEAT_INTERVAL = 20  # segundos


def executar_job(
    cfg: AgenteConfig, sb: SupabaseClient, job: dict
) -> None:
    cod_job = job["cod_agente_job"]
    entidade_nome = job["des_entidade"]
    tipo = job["des_tipo_job"]
    params = job.get("jsn_parametros") or {}
    batch_size = int(params.get("batch_size", 500))

    console.log(f"[bold cyan]▶ Job #{cod_job}[/] entidade=[bold]{entidade_nome}[/] tipo={tipo}")

    # claim
    claimed = sb.rpc(
        "fn_migracao_agente_job_claim",
        {
            "p_cod_agente_sessao": cfg.sessao.cod_agente_sessao,
            "p_token": cfg.sessao.token,
            "p_cod_agente_job": cod_job,
        },
    )
    if not claimed:
        console.log(f"[yellow]Job #{cod_job} não pôde ser reservado (já pego?). Pulando.[/]")
        return

    qtd_lidos = 0
    qtd_enviados = 0
    qtd_rejeitados = 0
    try:
        if tipo == "PING":
            console.log("[green]PING ok[/]")
        elif tipo == "TESTE_CONEXAO":
            from .pg_client import testar
            meta = testar(cfg.pg)
            console.log(f"[green]Banco legado:[/] {meta}")
        elif tipo == "EXTRACAO":
            ent = get_entidade(entidade_nome)
            staging = ent["staging"]
            transform = ent["transform"]
            sql = params.get("query") or ent["query"]

            for batch in stream_rows(cfg.pg, sql, batch_size=batch_size):
                qtd_lidos += len(batch)
                stg_rows = []
                for row in batch:
                    try:
                        stg_rows.append(transform(row))
                    except Exception:  # noqa: BLE001
                        qtd_rejeitados += 1
                if stg_rows:
                    enviados = sb.insert_migracao(staging, stg_rows)
                    qtd_enviados += enviados

                sb.rpc(
                    "fn_migracao_agente_job_progresso",
                    {
                        "p_cod_agente_sessao": cfg.sessao.cod_agente_sessao,
                        "p_token": cfg.sessao.token,
                        "p_cod_agente_job": cod_job,
                        "p_qtd_lidos": qtd_lidos,
                        "p_qtd_enviados": qtd_enviados,
                        "p_qtd_rejeitados": qtd_rejeitados,
                        "p_qtd_total": None,
                        "p_des_progresso": f"lidos={qtd_lidos} enviados={qtd_enviados}",
                    },
                )
                console.log(
                    f"  ↳ batch ok ({len(batch)}) — total lidos={qtd_lidos} "
                    f"enviados={qtd_enviados} rejeitados={qtd_rejeitados}"
                )
        else:
            raise ValueError(f"Tipo de job desconhecido: {tipo}")

        sb.rpc(
            "fn_migracao_agente_job_finalizar",
            {
                "p_cod_agente_sessao": cfg.sessao.cod_agente_sessao,
                "p_token": cfg.sessao.token,
                "p_cod_agente_job": cod_job,
                "p_sucesso": True,
                "p_qtd_lidos": qtd_lidos,
                "p_qtd_enviados": qtd_enviados,
                "p_qtd_rejeitados": qtd_rejeitados,
                "p_des_erro": None,
                "p_des_stack_trace": None,
            },
        )
        console.log(f"[bold green]✓ Job #{cod_job} concluído[/]")
    except Exception as exc:  # noqa: BLE001
        console.log(f"[bold red]✗ Job #{cod_job} falhou: {exc}[/]")
        sb.rpc(
            "fn_migracao_agente_job_finalizar",
            {
                "p_cod_agente_sessao": cfg.sessao.cod_agente_sessao,
                "p_token": cfg.sessao.token,
                "p_cod_agente_job": cod_job,
                "p_sucesso": False,
                "p_qtd_lidos": qtd_lidos,
                "p_qtd_enviados": qtd_enviados,
                "p_qtd_rejeitados": qtd_rejeitados,
                "p_des_erro": str(exc)[:2000],
                "p_des_stack_trace": traceback.format_exc()[:8000],
            },
        )


def loop_polling(cfg: AgenteConfig, intervalo: float = 5.0) -> None:
    if not cfg.pareado:
        raise RuntimeError("Agente não pareado — rode `octa-migracao parear` antes.")

    sb = SupabaseClient(cfg.supabase.url, cfg.supabase.anon_key)
    console.print(
        f"[bold green]Agente em execução[/] — sessão #{cfg.sessao.cod_agente_sessao}. "
        "Ctrl+C para sair."
    )
    ultimo_hb = 0.0
    try:
        while True:
            agora = time.monotonic()
            if agora - ultimo_hb >= HEARTBEAT_INTERVAL or ultimo_hb == 0.0:
                jobs = sb.rpc(
                    "fn_migracao_agente_heartbeat",
                    {
                        "p_cod_agente_sessao": cfg.sessao.cod_agente_sessao,
                        "p_token": cfg.sessao.token,
                    },
                ) or []
                ultimo_hb = agora
                for job in jobs:
                    executar_job(cfg, sb, job)
            time.sleep(intervalo)
    except KeyboardInterrupt:
        console.print("\n[yellow]Encerrado pelo usuário[/]")
    finally:
        sb.close()