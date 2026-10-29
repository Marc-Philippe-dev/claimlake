from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

builder = (
    SparkSession.builder.master("local[2]")
    .appName("module1-checkpoint")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

path = "data/sandbox/pas_policies"

policies = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/pas/policies_initial.csv")
)

policies.write.format("delta").mode("overwrite").save(path)  # version 0
table = DeltaTable.forPath(spark, path)

# version 1
table.update(condition="driver_age >= 75", set={"bonus_malus": "50"})

(
    table.history()
    .select(
        "version", "operation", F.col("operationMetrics")["numUpdatedRows"].alias("updated_rows")
    )
    .orderBy("version")
    .show(truncate=False)
)

for version in (0, 1):
    reader = spark.read.format("delta").option("versionAsOf", version)
    snapshot = reader.load(path)
    at_50 = snapshot.filter("bonus_malus = 50").count()
    total = snapshot.count()
    print(f"version {version}: {total:,} policies, {at_50:,} at 50")
