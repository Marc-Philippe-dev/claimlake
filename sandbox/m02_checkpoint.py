"""Module 2 checkpoint: no hard-coded builder, names from Settings."""

from pathlib import Path
from claimslake.platform import get_local_spark
from claimslake.settings import Settings

settings = Settings.local()
spark = get_local_spark(settings)
print("env :", settings.env)
print("landing :", settings.landing)
print("local :", settings.table("silver", "policies"))

cloud = Settings.databricks("workspace")
print("databricks:", cloud.table("silver", "policies"))
print("master :", spark.sparkContext.master)
print("timezone :", spark.conf.get("spark.sql.session.timeZone"))


# The Module 1 table, found from the data root instead of a literal
root = Path(settings.landing).parent
path = str(root / "sandbox" / "pas_policies")
policies = spark.read.format("delta").load(path)
print(f"policies : {policies.count():,}")