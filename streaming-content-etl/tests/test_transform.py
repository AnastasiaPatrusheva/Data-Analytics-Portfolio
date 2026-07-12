"""
Юнит-тесты трансформаций (pytest + локальный Spark).
Проверяют производные признаки, очистку, join/дедуп и состав витрин
на маленьких синтетических DataFrame — без обращения к реальным данным.

Запуск:  pytest -q
"""
from datetime import date

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType, StringType, StructField, StructType,
)

from src.prepare_raw import CONTENT_SCHEMA
from src.transform import build_marts, transform

# схема журнала уже после prepare_raw: msk_business_dt_str заменён на business_dt
AUDITION_SCHEMA = StructType([
    StructField("usage_geo_id", IntegerType()),
    StructField("audition_id", IntegerType()),
    StructField("puid", StringType()),
    StructField("usage_platform_ru", StringType()),
    StructField("app_version", StringType()),
    StructField("adult_content_flg", BooleanType()),
    StructField("hours", DoubleType()),
    StructField("hours_sessions_long", DoubleType()),
    StructField("kids_content_flg", BooleanType()),
    StructField("main_content_id", StringType()),
    StructField("usage_geo_id_name", StringType()),
    StructField("usage_country_name", StringType()),
    StructField("business_dt", DateType()),
])


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield s
    s.stop()


def _content(spark):
    return spark.createDataFrame(
        [("c1", "Audiobook", "Book One", 5.0, "['Жанр']", "a1")],
        schema=CONTENT_SCHEMA,
    )


def _audition(spark, rows):
    return spark.createDataFrame(rows, schema=AUDITION_SCHEMA)


def test_derived_features_and_cleaning(spark):
    rows = [
        # geo, aud_id, puid, platform, ver, adult, hours, hsl, kids, content, city, country, dt
        (1, 1, "u1", "iOS", None, True, 1.0, 0.5, False, "c1", "Москва", "Россия", date(2026, 6, 6)),  # суббота
        (1, 2, "u2", "iOS", None, False, 2.0, 1.0, True, "c1", "Казань", "Россия", date(2026, 6, 8)),  # понедельник
        (1, 3, "u3", "iOS", None, None, 1.0, 1.0, False, "c1", "Уфа", "Россия", date(2026, 6, 8)),     # adult=null
    ]
    result = {r["audition_id"]: r for r in transform(_content(spark), _audition(spark, rows)).collect()}

    assert 3 not in result              # строка с adult_content_flg=null отфильтрована
    assert result[1]["minutes"] == 30   # 0.5 часа -> 30 минут
    assert result[1]["is_weekend"] is True
    assert result[2]["is_weekend"] is False


def test_join_and_dedup(spark):
    rows = [
        (1, 1, "u1", "iOS", None, True, 1.0, 1.0, False, "c1", "Москва", "Россия", date(2026, 6, 8)),
        (1, 1, "u1", "iOS", None, True, 1.0, 1.0, False, "c1", "Москва", "Россия", date(2026, 6, 8)),  # дубль
        (1, 2, "u2", "iOS", None, True, 1.0, 1.0, False, "cX", "Москва", "Россия", date(2026, 6, 8)),  # нет в справочнике
    ]
    ids = [r["audition_id"] for r in transform(_content(spark), _audition(spark, rows)).collect()]
    assert ids == [1]


def test_marts_contain_business_dt(spark):
    rows = [(1, 1, "u1", "iOS", None, True, 1.0, 1.0, False, "c1", "Москва", "Россия", date(2026, 6, 8))]
    marts = build_marts(transform(_content(spark), _audition(spark, rows)))

    assert set(marts) == {"mart_daily_content_type", "mart_geo", "mart_content_top"}
    for name, df in marts.items():
        assert df.count() == 1
        assert "business_dt" in df.columns, f"{name} без business_dt — партиционирование сломается"
