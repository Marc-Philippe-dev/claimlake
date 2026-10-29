"""Platform adapters: what depends on where the code runs."""

from __future__ import annotations
import os
from pyspark.sql import SparkSession
from claimslake.settings import Settings

def get_local_spark(settings: Settings) -> SparkSession:
	"""Local SparkSession with Delta. On Databricks, it exists."""
	master = os.environ.get("SPARK_MASTER", "local[*]")
	builder = (
	SparkSession.builder.master(master)
	.appName("claimslake-local")
	.config("spark.sql.warehouse.dir", settings.warehouse)
	.config("spark.sql.shuffle.partitions", "8")
	.config("spark.sql.session.timeZone", "UTC")
	.config("spark.ui.showConsoleProgress", "false")
	)

	if settings.use_delta:
	# Imported here: the parquet fallback works without delta-spark
		from delta import configure_spark_with_delta_pip
		builder = builder.config(
		"spark.sql.extensions",
		"io.delta.sql.DeltaSparkSessionExtension",
		).config(
		"spark.sql.catalog.spark_catalog",
		"org.apache.spark.sql.delta.catalog.DeltaCatalog",
		)
		spark = configure_spark_with_delta_pip(builder).getOrCreate()
	else:
		spark = builder.getOrCreate()
		spark.sparkContext.setLogLevel("WARN")
	return spark
