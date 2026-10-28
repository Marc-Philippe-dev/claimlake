"""Source 1 - Policy Administration System (PAS) simulator and incremental extractor.

Locally the PAS is SQL Server in Docker; the extract command is the local twin of
the Azure Data Factory pipeline (high-watermark incremental copy to Parquet).

  python scripts/pas_source.py init     --data data      # create tables, load policies
  python scripts/pas_source.py update   --data data --month 2025-02-01
  python scripts/pas_source.py extract  --landing data/landing
  python scripts/pas_source.py status

Connection: PAS_DB_URL, default
  mssql+pymssql://sa:${MSSQL_SA_PASSWORD}@localhost:1433/pas
Any SQLAlchemy URL works (sqlite:///pas.db is used by the integration test).
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

POLICIES_DDL = """
CREATE TABLE policies (
    policy_id       BIGINT        NOT NULL PRIMARY KEY,
    start_date      DATE          NOT NULL,
    end_date        DATE          NOT NULL,
    exposure        DECIMAL(6,4)  NULL,
    area            VARCHAR(2)    NULL,
    region          VARCHAR(5)    NULL,
    postal_code     VARCHAR(5)    NULL,
    veh_power       INT           NULL,
    veh_age         INT           NULL,
    veh_brand       VARCHAR(5)    NULL,
    veh_gas         VARCHAR(10)   NULL,
    density         INT           NULL,
    bonus_malus     INT           NULL,
    driver_age      INT           NULL,
    annual_premium  DECIMAL(12,2) NULL,
    last_modified   DATETIME2(3)  NOT NULL
)
"""
WATERMARK_DDL = """
CREATE TABLE etl_watermark (
    table_name      VARCHAR(128) NOT NULL PRIMARY KEY,
    watermark_value DATETIME2(3) NOT NULL
)
"""


def db_url() -> str:
    default = f"mssql+pymssql://sa:{os.environ.get('MSSQL_SA_PASSWORD', '')}@localhost:1433/pas"
    return os.environ.get("PAS_DB_URL", default)


def _ensure_database(url: str) -> None:
    """SQL Server only: create the `pas` database if it does not exist."""
    if not url.startswith("mssql"):
        return
    master = create_engine(url.rsplit("/", 1)[0] + "/master", isolation_level="AUTOCOMMIT")
    with master.connect() as conn:
        conn.execute(text("IF DB_ID('pas') IS NULL CREATE DATABASE pas"))


def _ddl(engine: Engine, ddl: str) -> str:
    return ddl.replace("DATETIME2(3)", "TIMESTAMP") if engine.dialect.name == "sqlite" else ddl


def cmd_init(engine: Engine, data_dir: Path) -> None:
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS policies"))
        conn.execute(text("DROP TABLE IF EXISTS etl_watermark"))
        conn.execute(text(_ddl(engine, POLICIES_DDL)))
        conn.execute(text(_ddl(engine, WATERMARK_DDL)))
        conn.execute(
            text("INSERT INTO etl_watermark VALUES ('policies', :wm)"),
            {"wm": datetime(1900, 1, 1)},
        )
    df = pd.read_csv(data_dir / "pas" / "policies_initial.csv", parse_dates=["last_modified"])
    for col in ("start_date", "end_date"):
        df[col] = pd.to_datetime(df[col]).dt.date
    df.to_sql("policies", engine, if_exists="append", index=False, chunksize=2000)
    print(f"PAS initialised: {len(df):,} policies")


def cmd_update(engine: Engine, data_dir: Path, month: str) -> None:
    script = data_dir / "pas" / "updates" / f"updates_{month}.sql"
    statements = [
        line.replace("dbo.", "") if engine.dialect.name == "sqlite" else line
        for line in script.read_text().splitlines()
        if line.startswith("UPDATE")
    ]
    if engine.dialect.name == "sqlite":  # SQLite keeps timestamps as text: align the format
        statements = [s.replace("T00:00:00'", " 00:00:00.000000'") for s in statements]
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement.replace("dbo.policies", "policies")))
    print(f"PAS: {len(statements):,} policy changes applied for {month}")


def cmd_extract(engine: Engine, landing: Path) -> None:
    """High-watermark incremental extraction (same steps as the ADF pipeline)."""
    with engine.connect() as conn:
        old_wm = conn.execute(
            text("SELECT watermark_value FROM etl_watermark WHERE table_name = 'policies'")
        ).scalar_one()
        new_wm = conn.execute(text("SELECT MAX(last_modified) FROM policies")).scalar_one()
    if new_wm is None or str(new_wm) <= str(old_wm):
        print("PAS extract: no change since last watermark")
        return
    df = pd.read_sql(
        text("SELECT * FROM policies WHERE last_modified > :old AND last_modified <= :new"),
        engine,
        params={"old": old_wm, "new": new_wm},
    )
    out_dir = landing / "pas" / "policies"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    for col in ("start_date", "end_date", "last_modified"):
        df[col] = df[col].astype(str)
    df.to_parquet(out_dir / f"policies_{stamp}.parquet", index=False)
    # The watermark moves only after the file is written (replay-safe).
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE etl_watermark SET watermark_value = :wm WHERE table_name = 'policies'"),
            {"wm": new_wm},
        )
    print(f"PAS extract: {len(df):,} rows -> {out_dir.name}/policies_{stamp}.parquet")


def cmd_status(engine: Engine) -> None:
    with engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM policies")).scalar_one()
        wm = conn.execute(text("SELECT watermark_value FROM etl_watermark")).scalar_one()
    print(f"policies: {n:,} | watermark: {wm}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("command", choices=["init", "update", "extract", "status"])
    parser.add_argument("--data", default="data", type=Path)
    parser.add_argument("--landing", default="data/landing", type=Path)
    parser.add_argument("--month", help="for update: e.g. 2025-02-01")
    args = parser.parse_args()

    url = db_url()
    _ensure_database(url)
    engine = create_engine(url)
    if args.command == "init":
        cmd_init(engine, args.data)
    elif args.command == "update":
        cmd_update(engine, args.data, args.month)
    elif args.command == "extract":
        cmd_extract(engine, args.landing)
    else:
        cmd_status(engine)


if __name__ == "__main__":
    main()
