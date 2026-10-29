"""Unit tests for Settings: pure Python, no Spark session needed."""

from dataclasses import FrozenInstanceError
import pytest
from claimslake.settings import Settings

def test_local_uses_two_level_names(monkeypatch , tmp_path):
	monkeypatch.setenv("CLAIMSLAKE_ROOT", str(tmp_path))
	settings = Settings.local()
	assert settings.is_local
	assert settings.table("silver", "policies") == "silver.policies"
 

def test_databricks_uses_unity_catalog_names():
	settings = Settings.databricks("workspace")
	assert not settings.is_local
	assert settings.table("gold", "kpi") == "workspace.gold.kpi"
	assert settings.landing.startswith("/Volumes/workspace/")

def test_settings_are_immutable():
	settings = Settings.databricks("workspace")
	with pytest.raises(FrozenInstanceError):
		settings.catalog = "prod"