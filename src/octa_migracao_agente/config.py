"""Configuração persistente do agente (banco legado, sessão pareada).

Tudo fica em ~/.octa-migracao/config.toml (chmod 600).
Nunca enviamos a senha do PG legado para a Supabase.
"""

from __future__ import annotations

import os
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

import tomli_w

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    import tomli as tomllib


CONFIG_DIR = Path.home() / ".octa-migracao"
CONFIG_FILE = CONFIG_DIR / "config.toml"

DEFAULT_SUPABASE_URL = "https://aqkwwjtvdmpgvrjnmysh.supabase.co"
DEFAULT_SUPABASE_ANON_KEY = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImFxa3d3anR2ZG1wZ3Zyam5teXNoIiwicm9sZSI6ImFub2"
    "4iLCJpYXQiOjE3NzY2MTU4NzMsImV4cCI6MjA5MjE5MTg3M30."
    "DcjcsAzbOV5FRqrwBxJa0MfwLeF6GXGEclnOVgQhnAo"
)


@dataclass
class PgConfig:
    host: str = ""
    port: int = 5432
    database: str = ""
    user: str = ""
    password: str = ""
    ssl: bool = True


@dataclass
class SessaoConfig:
    cod_agente_sessao: Optional[int] = None
    token: Optional[str] = None


@dataclass
class SupabaseConfig:
    url: str = DEFAULT_SUPABASE_URL
    anon_key: str = DEFAULT_SUPABASE_ANON_KEY


@dataclass
class AgenteConfig:
    pg: PgConfig = field(default_factory=PgConfig)
    sessao: SessaoConfig = field(default_factory=SessaoConfig)
    supabase: SupabaseConfig = field(default_factory=SupabaseConfig)

    # ---- I/O ----
    @classmethod
    def carregar(cls) -> "AgenteConfig":
        cfg = cls()
        if CONFIG_FILE.exists():
            with CONFIG_FILE.open("rb") as f:
                raw = tomllib.load(f)
            cfg.pg = PgConfig(**raw.get("pg", {}))
            cfg.sessao = SessaoConfig(**raw.get("sessao", {}))
            cfg.supabase = SupabaseConfig(**raw.get("supabase", cfg.supabase.__dict__))
        cfg._aplicar_env()
        return cfg

    def _aplicar_env(self) -> None:
        env = os.environ
        # Supabase
        self.supabase.url = env.get("OCTA_SUPABASE_URL", self.supabase.url)
        self.supabase.anon_key = env.get("OCTA_SUPABASE_ANON_KEY", self.supabase.anon_key)
        # PG
        self.pg.host = env.get("OCTA_PG_HOST", self.pg.host)
        if env.get("OCTA_PG_PORT"):
            self.pg.port = int(env["OCTA_PG_PORT"])
        self.pg.database = env.get("OCTA_PG_DATABASE", self.pg.database)
        self.pg.user = env.get("OCTA_PG_USER", self.pg.user)
        self.pg.password = env.get("OCTA_PG_PASSWORD", self.pg.password)
        if env.get("OCTA_PG_SSL"):
            self.pg.ssl = env["OCTA_PG_SSL"].lower() in ("1", "true", "yes", "y", "s")

    def salvar(self) -> None:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        data = {
            "pg": asdict(self.pg),
            "sessao": {k: v for k, v in asdict(self.sessao).items() if v is not None},
            "supabase": asdict(self.supabase),
        }
        with CONFIG_FILE.open("wb") as f:
            tomli_w.dump(data, f)
        try:
            CONFIG_FILE.chmod(0o600)
            CONFIG_DIR.chmod(0o700)
        except OSError:
            # Windows: chmod limitado
            pass

    def limpar_sessao(self) -> None:
        self.sessao = SessaoConfig()
        self.salvar()

    @property
    def pareado(self) -> bool:
        return bool(self.sessao.cod_agente_sessao and self.sessao.token)