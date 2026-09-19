import os
import time

from pyspark.sql import SparkSession


def build_local_spark_session(app_name: str = "cinedata-tests") -> SparkSession:
    """Cria uma SparkSession local, usada apenas pelos testes (nunca pelos notebooks).

    Força UTC também no relógio do processo Python (não só no `spark.sql.session.timeZone`):
    sem isso, a JVM local usa o fuso horário da máquina para converter timestamps de volta
    para Python no `collect()`, o que desalinha silenciosamente os valores em máquinas que
    não estão em UTC (ex.: America/Sao_Paulo, UTC-3).
    """
    os.environ["TZ"] = "UTC"
    time.tzset()
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
