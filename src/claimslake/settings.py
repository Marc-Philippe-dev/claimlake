"""Settings: the only place where local and Databricks differ."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    env: str  # "local" or "databricks"
    catalog: str | None  # Unity Catalog catalog; None locally
    landing: str  # root of the incoming files
    system: str  # technical files: checkpoints, schemas
    table_format: str = "delta"  # "parquet" = offline fallback
    warehouse: str | None = None  # local Spark warehouse directory
    fnol_source: str = "files"  # "files" or "kafka" (Module 9)

    @property
    def is_local(self) -> bool:
        return self.env == "local"

    @property
    def use_delta(self) -> bool:
        return self.table_format == "delta"

    def table(self, schema: str, name: str) -> str:
        """catalog.schema.table on Databricks, schema.table locally."""
        if self.catalog:
            return f"{self.catalog}.{schema}.{name}"
        return f"{schema}.{name}"

    def checkpoint(self, name: str) -> str:
        return f"{self.system}/{name}/_checkpoint"
    
    def schema_location(self , name: str):
        return f"{self.system}/{name}/_schema"
    
    @classmethod
    def local(cls, root: str | Path = "data") -> Settings:
        """Local profile, overridable with CLAIMSLAKE_* variables."""
        getenv = os.environ.get
        root = Path(getenv("CLAIMSLAKE_ROOT", root)).resolve()
        return cls(
            env="local",
            catalog=None,
            landing=str(root / "landing"),
            system=str(root / "_system"),
            table_format=getenv("CLAIMSLAKE_TABLE_FORMAT", "delta"),
            warehouse=str(root / "warehouse"),
            fnol_source=getenv("CLAIMSLAKE_FNOL_SOURCE", "files"),
        )

    @classmethod
    def databricks(cls, catalog: str) -> Settings:
        """Databricks profile: Unity Catalog names and Volumes."""
        return cls(
            env="databricks",
            catalog=catalog,
            landing=f"/Volumes/{catalog}/landing/raw",
            system=f"/Volumes/{catalog}/ops/checkpoints",
        )
