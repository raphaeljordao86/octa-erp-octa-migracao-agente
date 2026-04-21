"""Mapeamento entidade legada → tabela staging + transformação.

Cada entrada define:
- query: SQL que extrai do legado (estrutura real do cliente Monte Carlo / sistema antigo)
- staging: nome da tabela em migracao.tab_migracao_stg_*
- transform: função que recebe uma linha do legado e devolve dict pronto pro stg

IMPORTANTE: cada transform recebe `cod_lote` extra via parâmetro `params`
do job (jsn_parametros.cod_lote). O runner injeta `cod_lote` em cada row
antes de inserir no staging.
"""

from __future__ import annotations

LegadoRow = dict
StgRow = dict


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _sn(v) -> str:
    """Normaliza booleanos/strings em 'S' ou 'N'."""
    if v is None:
        return "N"
    if isinstance(v, bool):
        return "S" if v else "N"
    s = str(v).strip().upper()
    if s in ("S", "SIM", "Y", "YES", "T", "TRUE", "1"):
        return "S"
    return "N"


def _str(v, max_len: int | None = None) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if max_len:
        s = s[:max_len]
    return s


def _num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# GRUPO_EMPRESA
# --------------------------------------------------------------------------- #
def _grupo_empresa_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_grupo_empresa_legado": row.get("cod_grupo_empresa") or row.get("cod_grupo"),
        "nom_grupo_empresa": _str(row.get("nom_grupo_empresa") or row.get("nom_grupo"), 120),
        "ind_grupo_ativo": _sn(row.get("ind_grupo_ativo") or row.get("ind_ativo") or "S"),
    }


# --------------------------------------------------------------------------- #
# PESSOA (cabeçalho)
# --------------------------------------------------------------------------- #
def _pessoa_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_pessoa_legado": row.get("cod_pessoa"),
        "nom_pessoa": _str(row.get("nom_pessoa") or row.get("nom_razao_social"), 200),
        "nom_fantasia": _str(row.get("nom_fantasia"), 120),
        "num_cnpj_cpf": _str(row.get("num_cnpj_cpf") or row.get("num_cnpj") or row.get("num_cpf"), 20),
        "num_ie_rg": _str(row.get("num_ie_rg") or row.get("num_inscricao_estadual") or row.get("num_rg"), 30),
        "ind_natureza": _str(row.get("ind_natureza") or ("J" if row.get("num_cnpj") else "F"), 1),
        "ind_pessoa_ativa": _sn(row.get("ind_pessoa_ativa") or row.get("ind_ativo") or "S"),
        "ind_cliente": _sn(row.get("ind_cliente")),
        "ind_fornecedor": _sn(row.get("ind_fornecedor")),
        "ind_funcionario": _sn(row.get("ind_funcionario")),
        "ind_transportadora": _sn(row.get("ind_transportadora")),
        "ind_motorista": _sn(row.get("ind_motorista")),
        "ind_representante": _sn(row.get("ind_representante")),
        "ind_bloqueado": _sn(row.get("ind_bloqueado")),
    }


def _pessoa_fisica_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_pessoa_legado": row.get("cod_pessoa"),
        "num_cpf": _str(row.get("num_cpf"), 14),
        "num_rg": _str(row.get("num_rg"), 20),
        "des_orgao_emissor_rg": _str(row.get("des_orgao_emissor_rg") or row.get("orgao_emissor"), 20),
        "dta_nascimento": row.get("dta_nascimento"),
        "ind_sexo": _str(row.get("ind_sexo"), 1),
        "des_estado_civil": _str(row.get("des_estado_civil") or row.get("estado_civil"), 30),
        "nom_mae": _str(row.get("nom_mae"), 120),
        "nom_pai": _str(row.get("nom_pai"), 120),
    }


def _pessoa_juridica_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_pessoa_legado": row.get("cod_pessoa"),
        "num_cnpj": _str(row.get("num_cnpj"), 18),
        "num_inscricao_estadual": _str(row.get("num_inscricao_estadual") or row.get("num_ie"), 20),
        "num_inscricao_municipal": _str(row.get("num_inscricao_municipal") or row.get("num_im"), 20),
        "des_regime_tributario": _str(row.get("des_regime_tributario") or row.get("regime_tributario"), 30),
        "dta_abertura": row.get("dta_abertura") or row.get("dta_fundacao"),
    }


def _pessoa_endereco_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_pessoa_legado": row.get("cod_pessoa"),
        "des_tipo_endereco": _str(row.get("des_tipo_endereco") or "PRINCIPAL", 30),
        "des_logradouro": _str(row.get("des_logradouro") or row.get("endereco"), 200),
        "num_endereco": _str(row.get("num_endereco") or row.get("numero"), 20),
        "des_complemento": _str(row.get("des_complemento") or row.get("complemento"), 100),
        "nom_bairro": _str(row.get("nom_bairro") or row.get("bairro"), 100),
        "num_cep": _str(row.get("num_cep") or row.get("cep"), 10),
        "nom_cidade": _str(row.get("nom_cidade") or row.get("cidade"), 100),
        "sgl_estado": _str(row.get("sgl_estado") or row.get("uf"), 2),
        "ind_principal": _sn(row.get("ind_principal") or "S"),
    }


def _pessoa_contato_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_pessoa_legado": row.get("cod_pessoa"),
        "des_tipo_contato": _str(row.get("des_tipo_contato") or "TELEFONE", 30),
        "des_valor_contato": _str(
            row.get("des_valor_contato") or row.get("telefone") or row.get("email") or row.get("contato"),
            200,
        ),
        "des_observacao": _str(row.get("des_observacao"), 200),
        "ind_principal": _sn(row.get("ind_principal") or "S"),
    }


# --------------------------------------------------------------------------- #
# EMPRESA / UNIDADE
# --------------------------------------------------------------------------- #
def _empresa_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_empresa_legado": row.get("cod_empresa"),
        "cod_pessoa_legado": row.get("cod_pessoa"),
        "cod_grupo_empresa_legado": row.get("cod_grupo_empresa") or row.get("cod_grupo"),
        "cod_empresa_matriz_legado": row.get("cod_empresa_matriz"),
        "sgl_empresa": _str(row.get("sgl_empresa") or row.get("sigla"), 20),
        "nom_empresa": _str(row.get("nom_empresa") or row.get("nom_razao_social") or row.get("nom_fantasia"), 200),
        "num_cnpj": _str(row.get("num_cnpj"), 20),
        "ind_matriz": _sn(row.get("ind_matriz")),
        "ind_filial": _sn(row.get("ind_filial")),
        "ind_empresa_ativa": _sn(row.get("ind_empresa_ativa") or row.get("ind_ativo") or "S"),
        "ind_controla_estoque": _sn(row.get("ind_controla_estoque") or "S"),
        "ind_controla_financeiro": _sn(row.get("ind_controla_financeiro") or "S"),
        "ind_controla_fiscal": _sn(row.get("ind_controla_fiscal") or "S"),
        "ind_controla_contabil": _sn(row.get("ind_controla_contabil") or "S"),
    }


def _unidade_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_unidade_legado": row.get("cod_unidade"),
        "cod_empresa_legado": row.get("cod_empresa"),
        "sgl_unidade": _str(row.get("sgl_unidade") or row.get("sigla"), 20),
        "nom_unidade": _str(row.get("nom_unidade") or row.get("descricao"), 200),
        "ind_unidade_ativa": _sn(row.get("ind_unidade_ativa") or row.get("ind_ativo") or "S"),
        "ind_principal": _sn(row.get("ind_principal")),
    }


# --------------------------------------------------------------------------- #
# ITENS
# --------------------------------------------------------------------------- #
def _grupo_item_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_grupo_item_legado": row.get("cod_grupo_item") or row.get("cod_grupo"),
        "nom_grupo_item": _str(row.get("nom_grupo_item") or row.get("descricao"), 120),
        "ind_grupo_item_ativo": _sn(row.get("ind_ativo") or "S"),
    }


def _subgrupo_item_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_subgrupo_item_legado": row.get("cod_subgrupo_item") or row.get("cod_subgrupo"),
        "cod_grupo_item_legado": row.get("cod_grupo_item") or row.get("cod_grupo"),
        "nom_subgrupo_item": _str(row.get("nom_subgrupo_item") or row.get("descricao"), 120),
        "ind_subgrupo_item_ativo": _sn(row.get("ind_ativo") or "S"),
    }


def _secao_item_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_secao_item_legado": row.get("cod_secao_item") or row.get("cod_secao"),
        "nom_secao_item": _str(row.get("nom_secao_item") or row.get("descricao"), 120),
        "ind_secao_item_ativo": _sn(row.get("ind_ativo") or "S"),
    }


def _departamento_item_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_departamento_item_legado": row.get("cod_departamento_item") or row.get("cod_departamento"),
        "nom_departamento_item": _str(
            row.get("nom_departamento_item") or row.get("des_departamento_item") or row.get("descricao"),
            120,
        ),
        "ind_departamento_item_ativo": _sn(row.get("ind_ativo") or "S"),
    }


def _item_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_item_legado": row.get("cod_item") or row.get("cod_produto"),
        "nom_item": _str(row.get("nom_item") or row.get("descricao") or row.get("nom_produto"), 200),
        "des_item": _str(row.get("des_item") or row.get("descricao_complementar"), 500),
        "des_tipo_item": _str(row.get("des_tipo_item") or row.get("tipo_item") or "MERCADORIA", 30),
        "sgl_unidade_medida": _str(row.get("sgl_unidade_medida") or row.get("unidade") or "UN", 6),
        "cod_grupo_item_legado": row.get("cod_grupo_item") or row.get("cod_grupo"),
        "cod_subgrupo_item_legado": row.get("cod_subgrupo_item") or row.get("cod_subgrupo"),
        "cod_marca_legado": row.get("cod_marca"),
        "num_cod_barra": _str(row.get("num_cod_barra") or row.get("cod_barra") or row.get("ean"), 30),
        "num_ncm": _str(row.get("num_ncm"), 10),
        "ind_item_ativo": _sn(row.get("ind_item_ativo") or row.get("ind_ativo") or "S"),
    }


def _item_cod_barra_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_item_legado": row.get("cod_item") or row.get("cod_produto"),
        "num_cod_barra": _str(row.get("num_cod_barra") or row.get("cod_barra") or row.get("ean"), 30),
        "ind_principal": _sn(row.get("ind_principal") or "S"),
    }


def _item_empresa_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_item_legado": row.get("cod_item") or row.get("cod_produto"),
        "cod_empresa_legado": row.get("cod_empresa"),
        "cod_item_reduzido": _str(row.get("cod_item_reduzido") or row.get("cod_reduzido"), 20),
        "ind_compra": _sn(row.get("ind_compra") or "S"),
        "ind_venda": _sn(row.get("ind_venda") or "S"),
        "ind_controla_estoque": _sn(row.get("ind_controla_estoque") or "S"),
        "ind_combustivel": _sn(row.get("ind_combustivel")),
        "ind_loja": _sn(row.get("ind_loja")),
        "ind_pista": _sn(row.get("ind_pista")),
        "ind_lubrificante": _sn(row.get("ind_lubrificante")),
        "ind_item_empresa_ativo": _sn(row.get("ind_ativo") or "S"),
    }


def _item_empresa_estoque_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_item_legado": row.get("cod_item") or row.get("cod_produto"),
        "cod_empresa_legado": row.get("cod_empresa"),
        "qtd_estoque_minimo": _num(row.get("qtd_estoque_minimo") or row.get("estoque_minimo")),
        "qtd_estoque_maximo": _num(row.get("qtd_estoque_maximo") or row.get("estoque_maximo")),
        "qtd_estoque_seguranca": _num(row.get("qtd_estoque_seguranca")),
        "qtd_ponto_reposicao": _num(row.get("qtd_ponto_reposicao")),
    }


# --------------------------------------------------------------------------- #
# FISCAL
# --------------------------------------------------------------------------- #
def _ncm_transform(row: LegadoRow) -> StgRow:
    return {
        "num_ncm": _str(row.get("num_ncm") or row.get("ncm"), 10),
        "des_ncm": _str(row.get("des_ncm") or row.get("descricao"), 500),
        "ind_ncm_ativo": _sn(row.get("ind_ativo") or "S"),
    }


def _cest_transform(row: LegadoRow) -> StgRow:
    return {
        "num_cest": _str(row.get("num_cest") or row.get("cest"), 10),
        "des_cest": _str(row.get("des_cest") or row.get("descricao"), 500),
        "num_ncm": _str(row.get("num_ncm") or row.get("ncm"), 10),
        "ind_cest_ativo": _sn(row.get("ind_ativo") or "S"),
    }


def _cfop_transform(row: LegadoRow) -> StgRow:
    return {
        "num_cfop": _str(row.get("num_cfop") or row.get("cfop"), 5),
        "des_cfop": _str(row.get("des_cfop") or row.get("descricao"), 500),
        "ind_cfop_ativo": _sn(row.get("ind_ativo") or "S"),
    }


def _natureza_operacao_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_natureza_operacao_legado": row.get("cod_natureza_operacao") or row.get("cod_natop"),
        "nom_natureza_operacao": _str(
            row.get("nom_natureza_operacao") or row.get("descricao") or row.get("nom_natop"),
            200,
        ),
        "num_cfop": _str(row.get("num_cfop") or row.get("cfop"), 5),
        "ind_entrada_saida": _str(row.get("ind_entrada_saida") or row.get("tipo"), 1),
        "ind_natureza_operacao_ativa": _sn(row.get("ind_ativo") or "S"),
    }


# --------------------------------------------------------------------------- #
# CONTÁBIL
# --------------------------------------------------------------------------- #
def _ctb_conta_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_ctb_conta_legado": row.get("cod_ctb_conta") or row.get("cod_conta"),
        "num_ctb_conta": _str(row.get("num_ctb_conta") or row.get("num_conta") or row.get("conta"), 30),
        "nom_ctb_conta": _str(row.get("nom_ctb_conta") or row.get("descricao"), 200),
        "ind_tipo_conta": _str(row.get("ind_tipo_conta") or row.get("tipo"), 1),
        "ind_natureza": _str(row.get("ind_natureza") or row.get("natureza") or "D", 1),
        "ind_aceita_lancamento": _sn(row.get("ind_aceita_lancamento") or "S"),
        "ind_ctb_conta_ativa": _sn(row.get("ind_ativo") or "S"),
    }


def _ctb_centro_custo_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_ctb_centro_custo_legado": row.get("cod_ctb_centro_custo") or row.get("cod_centro_custo"),
        "nom_ctb_centro_custo": _str(
            row.get("nom_ctb_centro_custo") or row.get("descricao") or row.get("nom_centro_custo"),
            200,
        ),
        "ind_ctb_centro_custo_ativo": _sn(row.get("ind_ativo") or "S"),
    }


def _ctb_historico_padrao_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_ctb_historico_padrao_legado": row.get("cod_ctb_historico_padrao") or row.get("cod_historico"),
        "des_ctb_historico_padrao": _str(
            row.get("des_ctb_historico_padrao") or row.get("descricao") or row.get("historico"),
            500,
        ),
        "ind_ctb_historico_padrao_ativo": _sn(row.get("ind_ativo") or "S"),
    }


# --------------------------------------------------------------------------- #
# PREÇO DE VENDA
# --------------------------------------------------------------------------- #
def _preco_venda_transform(row: LegadoRow) -> StgRow:
    return {
        "cod_item_legado": row.get("cod_item") or row.get("cod_produto"),
        "cod_empresa_legado": row.get("cod_empresa"),
        "val_preco_venda": _num(row.get("val_preco_venda") or row.get("preco_venda") or row.get("preco")),
        "val_preco_promocional": _num(row.get("val_preco_promocional") or row.get("preco_promocao")),
        "dta_inicio_vigencia": row.get("dta_inicio_vigencia") or row.get("dta_vigencia"),
        "dta_fim_vigencia": row.get("dta_fim_vigencia"),
    }


# --------------------------------------------------------------------------- #
# Catálogo de entidades suportadas pelo agente
# --------------------------------------------------------------------------- #
# AJUSTE AS QUERIES conforme a estrutura real do banco legado de cada cliente.
# As queries abaixo são otimistas: tentam ler colunas comuns. Se o legado tiver
# nomes diferentes, edite aqui ou passe `params.query` no job para sobrescrever.
ENTIDADES: dict[str, dict] = {
    # ---------- Empresas ----------
    "grupo_empresa": {
        "query": (
            "SELECT cod_grupo_empresa, nom_grupo_empresa, "
            "       COALESCE(ind_grupo_ativo, 'S') AS ind_grupo_ativo "
            "FROM tab_grupo_empresa "
            "ORDER BY cod_grupo_empresa"
        ),
        "staging": "tab_migracao_stg_grupo_empresa",
        "transform": _grupo_empresa_transform,
    },
    "empresa": {
        "query": (
            "SELECT cod_empresa, cod_pessoa, cod_grupo_empresa, "
            "       cod_empresa_matriz, sgl_empresa, nom_empresa, num_cnpj, "
            "       ind_matriz, ind_filial, "
            "       COALESCE(ind_empresa_ativa, 'S') AS ind_empresa_ativa "
            "FROM tab_empresa "
            "ORDER BY cod_empresa"
        ),
        "staging": "tab_migracao_stg_empresa",
        "transform": _empresa_transform,
    },
    "unidade": {
        "query": (
            "SELECT cod_unidade, cod_empresa, sgl_unidade, nom_unidade, "
            "       COALESCE(ind_unidade_ativa, 'S') AS ind_unidade_ativa, "
            "       ind_principal "
            "FROM tab_unidade "
            "ORDER BY cod_unidade"
        ),
        "staging": "tab_migracao_stg_unidade",
        "transform": _unidade_transform,
    },
    # ---------- Pessoas ----------
    "pessoa": {
        "query": (
            "SELECT cod_pessoa, nom_pessoa, nom_fantasia, num_cnpj_cpf, "
            "       num_ie_rg, ind_natureza, "
            "       COALESCE(ind_pessoa_ativa, 'S') AS ind_pessoa_ativa, "
            "       ind_cliente, ind_fornecedor, ind_funcionario, "
            "       ind_transportadora, ind_motorista, ind_representante, "
            "       ind_bloqueado "
            "FROM tab_pessoa "
            "ORDER BY cod_pessoa"
        ),
        "staging": "tab_migracao_stg_pessoa",
        "transform": _pessoa_transform,
    },
    "pessoa_fisica": {
        "query": (
            "SELECT cod_pessoa, num_cpf, num_rg, des_orgao_emissor_rg, "
            "       dta_nascimento, ind_sexo, des_estado_civil, nom_mae, nom_pai "
            "FROM tab_pessoa_fisica ORDER BY cod_pessoa"
        ),
        "staging": "tab_migracao_stg_pessoa_fisica",
        "transform": _pessoa_fisica_transform,
    },
    "pessoa_juridica": {
        "query": (
            "SELECT cod_pessoa, num_cnpj, num_inscricao_estadual, "
            "       num_inscricao_municipal, des_regime_tributario, dta_abertura "
            "FROM tab_pessoa_juridica ORDER BY cod_pessoa"
        ),
        "staging": "tab_migracao_stg_pessoa_juridica",
        "transform": _pessoa_juridica_transform,
    },
    "pessoa_endereco": {
        "query": (
            "SELECT cod_pessoa, des_tipo_endereco, des_logradouro, num_endereco, "
            "       des_complemento, nom_bairro, num_cep, nom_cidade, sgl_estado, "
            "       ind_principal "
            "FROM tab_pessoa_endereco ORDER BY cod_pessoa"
        ),
        "staging": "tab_migracao_stg_pessoa_endereco",
        "transform": _pessoa_endereco_transform,
    },
    "pessoa_contato": {
        "query": (
            "SELECT cod_pessoa, des_tipo_contato, des_valor_contato, "
            "       des_observacao, ind_principal "
            "FROM tab_pessoa_contato ORDER BY cod_pessoa"
        ),
        "staging": "tab_migracao_stg_pessoa_contato",
        "transform": _pessoa_contato_transform,
    },
    # ---------- Itens ----------
    "grupo_item": {
        "query": (
            "SELECT cod_grupo_item, nom_grupo_item, "
            "       COALESCE(ind_ativo, 'S') AS ind_ativo "
            "FROM tab_grupo_item ORDER BY cod_grupo_item"
        ),
        "staging": "tab_migracao_stg_grupo_item",
        "transform": _grupo_item_transform,
    },
    "subgrupo_item": {
        "query": (
            "SELECT cod_subgrupo_item, cod_grupo_item, nom_subgrupo_item, "
            "       COALESCE(ind_ativo, 'S') AS ind_ativo "
            "FROM tab_subgrupo_item ORDER BY cod_subgrupo_item"
        ),
        "staging": "tab_migracao_stg_subgrupo_item",
        "transform": _subgrupo_item_transform,
    },
    "secao_item": {
        "query": (
            "SELECT cod_secao_item, nom_secao_item, "
            "       COALESCE(ind_ativo, 'S') AS ind_ativo "
            "FROM tab_secao_item ORDER BY cod_secao_item"
        ),
        "staging": "tab_migracao_stg_secao_item",
        "transform": _secao_item_transform,
    },
    "departamento_item": {
        "query": (
            "SELECT cod_departamento_item, "
            "       COALESCE(des_departamento_item, nom_departamento_item) AS nom_departamento_item, "
            "       COALESCE(ind_ativo, 'S') AS ind_ativo "
            "FROM tab_departamento_item ORDER BY cod_departamento_item"
        ),
        "staging": "tab_migracao_stg_departamento_item",
        "transform": _departamento_item_transform,
    },
    "item": {
        "query": (
            "SELECT cod_item, nom_item, des_item, des_tipo_item, "
            "       sgl_unidade_medida, cod_grupo_item, cod_subgrupo_item, "
            "       cod_marca, num_cod_barra, num_ncm, "
            "       COALESCE(ind_item_ativo, 'S') AS ind_item_ativo "
            "FROM tab_item ORDER BY cod_item"
        ),
        "staging": "tab_migracao_stg_item",
        "transform": _item_transform,
    },
    "item_cod_barra": {
        "query": (
            "SELECT cod_item, num_cod_barra, ind_principal "
            "FROM tab_item_cod_barra ORDER BY cod_item"
        ),
        "staging": "tab_migracao_stg_item_cod_barra",
        "transform": _item_cod_barra_transform,
    },
    "item_empresa": {
        "query": (
            "SELECT cod_item, cod_empresa, cod_item_reduzido, "
            "       ind_compra, ind_venda, ind_controla_estoque, "
            "       ind_combustivel, ind_loja, ind_pista, ind_lubrificante, "
            "       COALESCE(ind_ativo, 'S') AS ind_ativo "
            "FROM tab_item_empresa ORDER BY cod_empresa, cod_item"
        ),
        "staging": "tab_migracao_stg_item_empresa",
        "transform": _item_empresa_transform,
    },
    "item_empresa_estoque": {
        "query": (
            "SELECT cod_item, cod_empresa, qtd_estoque_minimo, qtd_estoque_maximo, "
            "       qtd_estoque_seguranca, qtd_ponto_reposicao "
            "FROM tab_item_empresa_estoque ORDER BY cod_empresa, cod_item"
        ),
        "staging": "tab_migracao_stg_item_empresa_estoque",
        "transform": _item_empresa_estoque_transform,
    },
    # ---------- Fiscal ----------
    "ncm": {
        "query": (
            "SELECT num_ncm, des_ncm, COALESCE(ind_ativo, 'S') AS ind_ativo "
            "FROM tab_ncm ORDER BY num_ncm"
        ),
        "staging": "tab_migracao_stg_ncm",
        "transform": _ncm_transform,
    },
    "cest": {
        "query": (
            "SELECT num_cest, des_cest, num_ncm, "
            "       COALESCE(ind_ativo, 'S') AS ind_ativo "
            "FROM tab_cest ORDER BY num_cest"
        ),
        "staging": "tab_migracao_stg_cest",
        "transform": _cest_transform,
    },
    "cfop": {
        "query": (
            "SELECT num_cfop, des_cfop, COALESCE(ind_ativo, 'S') AS ind_ativo "
            "FROM tab_cfop ORDER BY num_cfop"
        ),
        "staging": "tab_migracao_stg_cfop",
        "transform": _cfop_transform,
    },
    "natureza_operacao": {
        "query": (
            "SELECT cod_natureza_operacao, nom_natureza_operacao, num_cfop, "
            "       ind_entrada_saida, COALESCE(ind_ativo, 'S') AS ind_ativo "
            "FROM tab_natureza_operacao ORDER BY cod_natureza_operacao"
        ),
        "staging": "tab_migracao_stg_natureza_operacao",
        "transform": _natureza_operacao_transform,
    },
    # ---------- Contábil ----------
    "ctb_conta": {
        "query": (
            "SELECT cod_ctb_conta, num_ctb_conta, nom_ctb_conta, "
            "       ind_tipo_conta, ind_natureza, ind_aceita_lancamento, "
            "       COALESCE(ind_ativo, 'S') AS ind_ativo "
            "FROM tab_ctb_conta ORDER BY cod_ctb_conta"
        ),
        "staging": "tab_migracao_stg_ctb_conta",
        "transform": _ctb_conta_transform,
    },
    "ctb_centro_custo": {
        "query": (
            "SELECT cod_ctb_centro_custo, nom_ctb_centro_custo, "
            "       COALESCE(ind_ativo, 'S') AS ind_ativo "
            "FROM tab_ctb_centro_custo ORDER BY cod_ctb_centro_custo"
        ),
        "staging": "tab_migracao_stg_ctb_centro_custo",
        "transform": _ctb_centro_custo_transform,
    },
    "ctb_historico_padrao": {
        "query": (
            "SELECT cod_ctb_historico_padrao, des_ctb_historico_padrao, "
            "       COALESCE(ind_ativo, 'S') AS ind_ativo "
            "FROM tab_ctb_historico_padrao ORDER BY cod_ctb_historico_padrao"
        ),
        "staging": "tab_migracao_stg_ctb_historico_padrao",
        "transform": _ctb_historico_padrao_transform,
    },
    # ---------- Preços ----------
    "preco_venda": {
        "query": (
            "SELECT cod_item, cod_empresa, val_preco_venda, val_preco_promocional, "
            "       dta_inicio_vigencia, dta_fim_vigencia "
            "FROM tab_preco_venda ORDER BY cod_empresa, cod_item"
        ),
        "staging": "tab_migracao_stg_preco_venda",
        "transform": _preco_venda_transform,
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