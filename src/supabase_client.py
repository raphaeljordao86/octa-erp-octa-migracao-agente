"""Cliente HTTP para Supabase (REST + RPC). Síncrono, simples."""

from __future__ import annotations

from typing import Any

import httpx


class SupabaseClient:
    def __init__(self, url: str, anon_key: str, timeout: float = 30.0) -> None:
        self.url = url.rstrip("/")
        self.anon_key = anon_key
        self._client = httpx.Client(
            timeout=timeout,
            headers={
                "apikey": anon_key,
                "Authorization": f"Bearer {anon_key}",
                "Content-Type": "application/json",
            },
        )

    def close(self) -> None:
        self._client.close()

    # ---------- RPC ----------
    def rpc(self, fn_name: str, params: dict[str, Any]) -> Any:
        r = self._client.post(f"{self.url}/rest/v1/rpc/{fn_name}", json=params)
        r.raise_for_status()
        if r.text:
            return r.json()
        return None

    # ---------- INSERT em massa numa tabela do schema migracao ----------
    def insert_migracao(
        self, tabela: str, registros: list[dict[str, Any]]
    ) -> int:
        if not registros:
            return 0
        r = self._client.post(
            f"{self.url}/rest/v1/{tabela}",
            json=registros,
            headers={
                "Content-Profile": "migracao",
                "Prefer": "return=minimal",
            },
        )
        r.raise_for_status()
        return len(registros)

    # ---------- SELECT view migracao.vw_agente_sessao_status ----------
    def select_view_sessao(self, cod_agente_sessao: int) -> dict[str, Any] | None:
        r = self._client.get(
            f"{self.url}/rest/v1/vw_agente_sessao_status",
            params={
                "cod_agente_sessao": f"eq.{cod_agente_sessao}",
                "limit": "1",
            },
            headers={"Accept-Profile": "migracao"},
        )
        r.raise_for_status()
        rows = r.json()
        return rows[0] if rows else None