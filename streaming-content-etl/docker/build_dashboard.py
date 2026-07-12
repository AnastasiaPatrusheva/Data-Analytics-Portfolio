"""
Создаёт в Superset графики на витринах ClickHouse и собирает из них дашборд
«Streaming Content Analytics». Идемпотентно: пересоздаёт объекты с теми же именами.
"""
import json

from superset.app import create_app

TITLE = "Streaming Content Analytics"


def metric(col, agg="SUM"):
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col},
        "aggregate": agg,
        "label": f"{agg}({col})",
        "optionName": f"metric_{agg}_{col}",
    }


WEEKEND_LABEL = {
    "expressionType": "SQL",
    "sqlExpression": "if(is_weekend = 1, 'выходные', 'будни')",
    "label": "День недели",
    "optionName": "col_weekend_label",
}

app = create_app()
with app.app_context():
    from superset import db
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice

    def dataset(name):
        ds = db.session.query(SqlaTable).filter_by(table_name=name).first()
        if ds is None:
            raise SystemExit(f"[err] нет датасета {name}")
        try:  # подтягиваем актуальные колонки витрины
            ds.fetch_metadata()
            db.session.commit()
        except Exception as exc:  # noqa: BLE001
            print(f"[warn] метаданные {name}: {exc}")
        return ds

    ds_daily = dataset("mart_daily_content_type")
    ds_geo = dataset("mart_geo")
    ds_top = dataset("mart_content_top")

    minutes = metric("total_minutes")
    users = metric("unique_users")

    specs = [
        (
            "Динамика минут по дням",
            ds_daily,
            "echarts_timeseries_line",
            {
                "x_axis": "business_dt",
                "time_grain_sqla": "P1D",
                "metrics": [minutes],
                "groupby": [],
                "adhoc_filters": [],
                "row_limit": 1000,
                "color_scheme": "supersetColors",
                "show_legend": False,
                "markerEnabled": False,
            },
        ),
        (
            "Потребление: будни vs выходные",
            ds_daily,
            "pie",
            {
                "groupby": [WEEKEND_LABEL],
                "metric": minutes,
                "adhoc_filters": [],
                "row_limit": 100,
                "show_labels": True,
                "label_type": "key_percent",
                "color_scheme": "supersetColors",
            },
        ),
        (
            "Минуты по типу контента",
            ds_daily,
            "echarts_timeseries_bar",
            {
                "x_axis": "main_content_type",
                "metrics": [minutes],
                "groupby": [],
                "adhoc_filters": [],
                "row_limit": 50,
                "orientation": "vertical",
                "x_axis_sort": "SUM(total_minutes)",
                "x_axis_sort_asc": False,
                "show_legend": False,
                "color_scheme": "supersetColors",
            },
        ),
        (
            "Топ-10 регионов по минутам",
            ds_geo,
            "echarts_timeseries_bar",
            {
                "x_axis": "usage_geo_id_name",
                "metrics": [minutes],
                "groupby": [],
                "adhoc_filters": [],
                "row_limit": 10,
                "orientation": "horizontal",
                "x_axis_sort": "SUM(total_minutes)",
                "x_axis_sort_asc": False,
                "show_legend": False,
                "color_scheme": "supersetColors",
            },
        ),
        (
            "Топ-10 произведений",
            ds_top,
            "table",
            {
                "query_mode": "aggregate",
                "groupby": ["main_content_type", "main_content_name"],
                "metrics": [minutes, users],
                "adhoc_filters": [],
                "row_limit": 10,
                "order_desc": True,
                "server_pagination": False,
            },
        ),
    ]

    slices = []
    for name, ds, viz, extra in specs:
        db.session.query(Slice).filter_by(slice_name=name).delete()
        db.session.commit()

        params = {"datasource": f"{ds.id}__table", "viz_type": viz, **extra}
        sl = Slice(
            slice_name=name,
            viz_type=viz,
            datasource_type="table",
            datasource_id=ds.id,
            datasource_name=ds.table_name,
            params=json.dumps(params),
        )
        db.session.add(sl)
        db.session.commit()
        slices.append(sl)
        print(f"[ok] график: {name} (id={sl.id})")

    # --- дашборд: 1 широкий график сверху, дальше две строки по два ---
    db.session.query(Dashboard).filter_by(dashboard_title=TITLE).delete()
    db.session.commit()

    rows = {"CHART-1": ("ROW-1", 12), "CHART-2": ("ROW-2", 6), "CHART-3": ("ROW-2", 6),
            "CHART-4": ("ROW-3", 6), "CHART-5": ("ROW-3", 6)}

    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "parents": ["ROOT_ID"],
                    "children": ["ROW-1", "ROW-2", "ROW-3"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": TITLE}},
    }
    for row_id, children in (("ROW-1", ["CHART-1"]), ("ROW-2", ["CHART-2", "CHART-3"]),
                             ("ROW-3", ["CHART-4", "CHART-5"])):
        position[row_id] = {
            "type": "ROW", "id": row_id, "parents": ["ROOT_ID", "GRID_ID"],
            "children": children, "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
    for i, sl in enumerate(slices, start=1):
        chart_id = f"CHART-{i}"
        row_id, width = rows[chart_id]
        position[chart_id] = {
            "type": "CHART", "id": chart_id, "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {"chartId": sl.id, "width": width, "height": 50, "sliceName": sl.slice_name},
        }

    dash = Dashboard(
        dashboard_title=TITLE,
        slug="streaming-content-analytics",
        slices=slices,
        position_json=json.dumps(position),
        json_metadata=json.dumps({"color_scheme": "supersetColors", "refresh_frequency": 0}),
        published=True,
    )
    db.session.add(dash)
    db.session.commit()
    print(f"[ok] дашборд «{TITLE}» (id={dash.id}, slug={dash.slug})")
