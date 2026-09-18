from pyspark.sql import SparkSession


def build_local_spark_session(app_name: str = "cinedata-tests") -> SparkSession:
    """Cria uma SparkSession local, usada apenas pelos testes (nunca pelos notebooks)."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
