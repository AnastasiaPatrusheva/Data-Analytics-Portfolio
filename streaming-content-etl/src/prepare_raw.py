"""
Разовая подготовка «сырого» слоя: CSV -> Parquet, журнал событий партиционируется
по бизнес-дате.

Имитирует ежедневную поставку данных: в проде такие партиции появлялись бы в S3
по одной за день (`.../business_dt=2024-09-01/`), и DAG ждал бы файл за свою дату.

Запуск (один раз):
  python src/prepare_raw.py --content data/content.csv --audition data/audition.csv --out data/raw
"""
import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType,
)

CONTENT_SCHEMA = StructType([
    StructField("main_content_id", StringType()),
    StructField("main_content_type", StringType()),
    StructField("main_content_name", StringType()),
    StructField("main_content_duration_hours", DoubleType()),
    StructField("published_topic_title_list", StringType()),
    StructField("main_author_id", StringType()),
])

AUDITION_SCHEMA = StructType([
    StructField("usage_geo_id", IntegerType()),
    StructField("audition_id", IntegerType()),
    StructField("puid", StringType()),
    StructField("usage_platform_ru", StringType()),
    StructField("msk_business_dt_str", StringType()),
    StructField("app_version", StringType()),
    StructField("adult_content_flg", BooleanType()),
    StructField("hours", DoubleType()),
    StructField("hours_sessions_long", DoubleType()),
    StructField("kids_content_flg", BooleanType()),
    StructField("main_content_id", StringType()),
    StructField("usage_geo_id_name", StringType()),
    StructField("usage_country_name", StringType()),
])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--content", required=True)
    ap.add_argument("--audition", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    spark = SparkSession.builder.appName("streaming-prepare-raw").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    content = spark.read.csv(args.content, schema=CONTENT_SCHEMA, header=False)
    content.write.mode("overwrite").parquet(f"{args.out}/content")
    print(f"[ok] справочник контента: {content.count()} строк")

    audition = (
        spark.read.csv(args.audition, schema=AUDITION_SCHEMA, header=False)
        .withColumn("business_dt", F.to_date("msk_business_dt_str", "yyyy-MM-dd"))
        .filter(F.col("business_dt").isNotNull())
        .drop("msk_business_dt_str")
    )
    (
        audition.write.mode("overwrite")
        .partitionBy("business_dt")
        .parquet(f"{args.out}/audition")
    )
    days = audition.select("business_dt").distinct().count()
    print(f"[ok] журнал событий: {audition.count()} строк, {days} партиций по дате")

    spark.stop()


if __name__ == "__main__":
    main()
