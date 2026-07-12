"""
Контроль качества витрин за конкретную бизнес-дату.

Проверки (падение = ненулевой код возврата, DAG останавливается до загрузки):
  * данные за дату есть (партиция читается и непустая);
  * нет отрицательных минут / сессий / пользователей;
  * unique_users <= sessions — инвариант, который ловит сломанную агрегацию
    (например, размноженные строки после join);
  * ключ витрины уникален — страховка на случай рассинхрона ключа и группировки.

Запуск:
  python src/data_quality.py --marts data/marts --date 2024-09-01
"""
import argparse
import sys

import pyarrow.dataset as ds

KEYS = {
    "mart_daily_content_type": [
        "business_dt", "main_content_type", "adult_content_flg", "kids_content_flg", "is_weekend",
    ],
    "mart_geo": ["business_dt", "usage_country_name", "usage_geo_id_name"],
    "mart_content_top": ["business_dt", "main_content_id"],
}
NON_NEGATIVE = ["total_minutes", "sessions", "unique_users"]


def check(marts_dir, date):
    problems = []
    for mart, key in KEYS.items():
        path = f"{marts_dir}/{mart}/dt={date}"
        try:
            df = ds.dataset(path, format="parquet").to_table().to_pandas()
        except Exception as exc:  # noqa: BLE001
            problems.append(f"{mart}: не читается партиция {path} ({exc})")
            continue

        if df.empty:
            problems.append(f"{mart}: пустая партиция за {date}")
            continue

        for col in NON_NEGATIVE:
            if col in df.columns and (df[col] < 0).any():
                problems.append(f"{mart}: отрицательные значения в {col}")

        # инвариант: уникальных слушателей не может быть больше, чем сессий
        if {"unique_users", "sessions"}.issubset(df.columns) and (df["unique_users"] > df["sessions"]).any():
            problems.append(f"{mart}: unique_users > sessions — сломана агрегация")

        dupes = int(df.duplicated(subset=key).sum())
        if dupes:
            problems.append(f"{mart}: {dupes} дублей по ключу {key}")

        print(f"[OK] {mart} за {date}: {len(df)} строк")

    return problems


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--marts", required=True)
    ap.add_argument("--date", required=True)
    args = ap.parse_args()

    problems = check(args.marts, args.date)
    if problems:
        print("\nПроверки качества НЕ пройдены:")
        for p in problems:
            print(f"  - {p}")
        sys.exit(1)

    print(f"\nВсе проверки качества за {args.date} пройдены.")


if __name__ == "__main__":
    main()
