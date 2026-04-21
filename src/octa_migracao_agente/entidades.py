"""Mapeamento entidade legada → tabela staging + transformação.

Cada entrada define:
- query: SQL que extrai do legado (substitua para a estrutura real do cliente)
- staging: nome da tabela em migracao.tab_migracao_stg_*
- transform: função que recebe uma linha do legado e devolve dict pronto pro stg
"""

from __future__ import annotations

from typing import Callable

LegadoRow = dict
StgRow = dict


def _empresa_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_empresa_legado": row.get("cod_empresa") or row.get("id_empresa"),
        "nom_razao_social": row.get("nom_razao_social") or row.get("razao_social"),
        "nom_fantasia": row.get("nom_fantasia") or row.get("nome_fantasia"),
        "num_cnpj": row.get("num_cnpj") or row.get("cnpj"),
        "num_inscricao_estadual": row.get("num_inscricao_estadual") or row.get("ie"),
        "num_inscricao_municipal": row.get("num_inscricao_municipal") or row.get("im"),
        "ind_matriz": row.get("ind_matriz") or "N",
        "ind_ativo": row.get("ind_ativo") or "S",
    }


# Query "esqueleto" — ajustar para a tabela real do legado de cada cliente.
# Não usar SELECT * em produção; explicitar colunas para garantir contrato.
ENTIDADES: dict[str, dict] = {
    "empresa": {
        "query": (
            "SELECT cod_empresa, nom_razao_social, nom_fantasia, num_cnpj, "
            "       num_inscricao_estadual, num_inscricao_municipal, "
            "       ind_matriz, ind_ativo "
            "FROM tab_empresa "
            "ORDER BY cod_empresa"
        ),
        "staging": "tab_migracao_stg_empresa",
        "transform": _empresa_transform,
    },
}


def get_entidade(nome: str) -> dict:
    if nome not in ENTIDADES:
        raise KeyError(
            f"Entidade '{nome}' não mapeada no agente. "
            f"Disponíveis: {', '.join(ENTIDADES.keys())}"
        )
    return ENTIDADES[nome]


__all__ = ["ENTIDADES", "get_entidade"]