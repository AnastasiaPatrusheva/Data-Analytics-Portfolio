"""
PySpark-трансформация ОДНОЙ бизнес-даты: сырая партиция -> аналитические витрины.

Читает партицию журнала за дату (`business_dt=<date>`) и справочник контента,
считает производные признаки, чистит, соединяет, дедуплицирует и собирает три витрины.
Результат пишется в партицию `<out>/<mart>/dt=<date>`.

Запуск:
  python src/transform.py --raw data/raw --out data/marts --date 2024-09-01
"""
import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

MARTS = ("mart_daily_content_type", "mart_geo", "mart_content_top")


def read_partition(spark, raw_dir, date):
    """Справочник целиком + журнал только за указанную бизнес-дату (partition pruning)."""
    content = spark.read.parquet(f"{raw_dir}/content")
    audition = spark.read.parquet(f"{raw_dir}/audition").filter(F.col("business_dt") == F.lit(date))
    return content, audition


def transform(content, audition):
    """Производные признаки, очистка, join со справочником, дедуп по audition_id."""
    audition = (
        audition
        .withColumn("minutes", (F.col("hours_sessions_long") * 60).cast(IntegerType()))
        .withColumn("is_weekend", F.dayofweek("business_dt").isin(1, 7))  # 1=Вс, 7=Сб
        .filter(F.col("adult_content_flg").isNotNull())
    )
    return (
        audition.join(F.broadcast(content), on="main_content_id", how="inner")
        .dropDuplicates(["audition_id"])
    )


def build_marts(joined):
    users = F.countDistinct("puid").alias("unique_users")
    minutes = F.sum("minutes").alias("total_minutes")
    sessions = F.count("*").alias("sessions")

    return {
        "mart_daily_content_type": joined.groupBy(
            "business_dt", "main_content_type",
            "adult_content_flg", "kids_content_flg", "is_weekend",
        ).agg(minutes, sessions, users),

        "mart_geo": joined.groupBy(
            "business_dt", "usage_country_name", "usage_geo_id_name",
        ).agg(minutes, sessions, users),

        "mart_content_top": joined.groupBy(
            "business_dt", "main_content_id", "main_content_name", "main_content_type",
        ).agg(minutes, sessions, users),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--date", required=True, help="бизнес-дата, YYYY-MM-DD")
    args = ap.parse_args()

    spark = SparkSession.builder.appName(f"streaming-etl-{args.date}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    content, audition = read_partition(spark, args.raw, args.date)
    marts = build_marts(transform(content, audition))
    for name, df in marts.items():
        path = f"{args.out}/{name}/dt={args.date}"
        df.write.mode("overwrite").parquet(path)
        print(f"[ok] {args.date} {name}: {df.count()} строк -> {path}")

    spark.stop()


if __name__ == "__main__":
    main()
