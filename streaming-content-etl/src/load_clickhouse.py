"""
Загрузка витрин за бизнес-дату в ClickHouse — идемпотентно на уровне партиции.

Для каждой витрины: DROP PARTITION за дату -> INSERT партиции заново.
Повторный запуск за ту же дату не создаёт дублей (важно для backfill и ретраев).

Хост/креды берутся из аргументов или переменных окружения
(CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_DB).

Запуск:
  python src/load_clickhouse.py --marts data/marts --date 2024-09-01
"""
import argparse
import os

import clickhouse_connect
import pyarrow.dataset as ds

MARTS = ["mart_daily_content_type", "mart_geo", "mart_content_top"]


def load_mart(client, marts_dir, name, date):
    df = ds.dataset(f"{marts_dir}/{name}/dt={date}", format="parquet").to_table().to_pandas()
    for col in df.select_dtypes(include="bool").columns:
        df[col] = df[col].astype("uint8")  # bool -> UInt8 под DDL ClickHouse

    # идемпотентность: сносим партицию за дату и вставляем заново
    client.command(f"ALTER TABLE {name} DROP PARTITION '{date}'")
    client.insert_df(name, df)
    print(f"[ok] {name} за {date}: загружено {len(df)} строк")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--marts", required=True)
    ap.add_argument("--date", required=True)
    ap.add_argument("--host", default=os.environ.get("CH_HOST", "localhost"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("CH_PORT", "8123")))
    ap.add_argument("--user", default=os.environ.get("CH_USER", "default"))
    ap.add_argument("--password", default=os.environ.get("CH_PASSWORD", ""))
    ap.add_argument("--database", default=os.environ.get("CH_DB", "default"))
    args = ap.parse_args()

    client = clickhouse_connect.get_client(
        host=args.host, port=args.port, username=args.user,
        password=args.password, database=args.database,
    )
    for name in MARTS:
        load_mart(client, args.marts, name, args.date)


if __name__ == "__main__":
    main()
