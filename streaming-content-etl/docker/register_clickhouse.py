"""
Регистрирует в Superset подключение к ClickHouse и три витрины как датасеты.
Запускается один раз при старте контейнера (см. command в docker-compose.yml).
Идемпотентно: повторный запуск ничего не ломает.
"""
import os

from superset.app import create_app

# креды берутся из окружения (.env), в репозитории их нет
CH_USER = os.environ.get("CH_USER", "etl")
CH_PASSWORD = os.environ["CH_PASSWORD"]
CH_HOST = os.environ.get("CH_HOST", "clickhouse")
CH_PORT = os.environ.get("CH_PORT", "8123")
CH_DB = os.environ.get("CH_DB", "default")

URI = f"clickhousedb://{CH_USER}:{CH_PASSWORD}@{CH_HOST}:{CH_PORT}/{CH_DB}"
DB_NAME = "ClickHouse"
MARTS = ("mart_daily_content_type", "mart_geo", "mart_content_top")

app = create_app()
with app.app_context():
    from superset import db
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.core import Database

    database = db.session.query(Database).filter_by(database_name=DB_NAME).first()
    if database is None:
        database = Database(database_name=DB_NAME, sqlalchemy_uri=URI)
        db.session.add(database)
        db.session.commit()
        print(f"[ok] создано подключение {DB_NAME}")
    else:
        database.sqlalchemy_uri = URI
        db.session.commit()
        print(f"[ok] подключение {DB_NAME} уже существует")

    for table in MARTS:
        exists = (
            db.session.query(SqlaTable)
            .filter_by(table_name=table, database_id=database.id)
            .first()
        )
        if exists:
            print(f"[skip] датасет {table} уже есть")
            continue

        dataset = SqlaTable(table_name=table, database=database, schema="default")
        db.session.add(dataset)
        db.session.commit()
        try:
            dataset.fetch_metadata()
            db.session.commit()
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] не удалось собрать метаданные {table}: {exc}")
        print(f"[ok] датасет {table}")
