# Octa Migração — Agente Local

Agente Python que **roda dentro da rede do cliente** (mesmo lugar onde o banco
PostgreSQL legado é acessível, geralmente atrás de VPN) e empurra os dados
para o ERP Octa via Supabase.

## Por que existe

A nuvem do Supabase **não enxerga** redes privadas/VPN da empresa. Em vez de
expor o banco legado para a internet, o agente:

1. roda no mesmo lado que vê o banco legado;
2. lê os dados via SQL paginado;
3. envia em chunks JSON pra Supabase (REST/RPC), gravando em
   `migracao.tab_migracao_stg_*`;
4. reporta progresso (lidos/enviados/erros) no banco do Octa.

Sem credenciais do banco no navegador, sem porta exposta, sem VPN reversa.

## Instalação

Requer Python ≥ 3.10.

```bash
# instalação isolada (recomendada — pipx põe num venv dedicado)
pipx install octa-migracao-agente

# ou no ambiente atual
pip install octa-migracao-agente
```

## Uso rápido

### 1. Gerar token no wizard

Abra o ERP → **Migração** → **Etapa 0 — Conexão** → **Gerar token de pareamento**.
Copie o comando exibido.

### 2. Parear

```bash
octa-migracao parear --token COLE_O_TOKEN_AQUI
```

Na primeira vez, o agente vai pedir interativamente as credenciais do PG legado
(host, porta, banco, usuário, senha, ssl) e guarda em `~/.octa-migracao/config.toml`
(somente leitura para o usuário). **Senha não vai pra nuvem.**

### 3. Rodar

```bash
octa-migracao run
```

Fica em loop fazendo polling de jobs criados pelo wizard. Encerre com `Ctrl+C`.

### Outros comandos

```bash
octa-migracao status            # ver sessão e jobs recentes
octa-migracao testar-pg         # apenas SELECT 1 no banco legado
octa-migracao logout            # apaga config local + revoga sessão
```

## Segurança

- **Credenciais do PG legado**: ficam só em `~/.octa-migracao/config.toml`
  (modo 600). Nunca trafegam para o Supabase.
- **Token de pareamento**: 32 bytes hex, validade 30min até parear.
  Após parear, vira o "id" da sessão e pode ser revogado a qualquer momento
  pelo wizard.
- **Conexão com Supabase**: HTTPS + chave anon (pública, mesma do app).
- **Recomendado**: criar usuário PostgreSQL **somente leitura** no legado.

## Configuração avançada

Variáveis de ambiente (sobrescrevem o config.toml):

```bash
export OCTA_SUPABASE_URL="https://aqkwwjtvdmpgvrjnmysh.supabase.co"
export OCTA_SUPABASE_ANON_KEY="..."
export OCTA_PG_HOST=db.empresa.local
export OCTA_PG_PORT=5432
export OCTA_PG_DATABASE=erp_legado
export OCTA_PG_USER=migracao_ro
export OCTA_PG_PASSWORD=...
export OCTA_PG_SSL=true
```

## Troubleshooting

| Problema | Causa provável | Solução |
|---|---|---|
| `Token expirou` | token vencido (>30min sem parear) | gere outro no wizard |
| `Sessão revogada` | desconectou no wizard | refazer `octa-migracao parear` |
| `connection refused` no PG | porta fechada / banco fora do ar | verificar firewall, `pg_hba.conf`, listen_addresses |
| `password authentication failed` | usuário/senha errado | revisar config |
| agente fica online mas wizard mostra offline | heartbeat preso | reiniciar (`Ctrl+C` → `octa-migracao run`) |

## Desenvolvimento local

```bash
git clone ... octa-migracao-agente
cd octa-migracao-agente
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
octa-migracao --help
```