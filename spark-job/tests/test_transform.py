import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from spark_ads_job import create_spark, transform  # noqa: E402


def test_transform_gmv_orders_buyers():
    spark = create_spark("test-transform")
    try:
        schema = "dt date, channel string, user_id long, amount decimal(12,2)"
        rows = [
            (date(2026, 11, 20), "douyin", 101, Decimal("100.00")),
            (date(2026, 11, 20), "douyin", 101, Decimal("50.00")),
            (date(2026, 11, 20), "douyin", 102, Decimal("30.00")),
            (date(2026, 11, 20), "wechat", 103, Decimal("20.00")),
            (date(2026, 11, 21), "douyin", 104, Decimal("10.00")),
        ]
        df = spark.createDataFrame(rows, schema)
        out = {(r["dt"].isoformat(), r["channel"]): r for r in transform(df).collect()}

        douyin = out[("2026-11-20", "douyin")]
        assert douyin["gmv"] == Decimal("180.00")
        assert douyin["orders"] == 3
        assert douyin["buyers"] == 2
        assert douyin["avg_buyer_value"] == Decimal("90.00")

        wechat = out[("2026-11-20", "wechat")]
        assert wechat["gmv"] == Decimal("20.00")
        assert wechat["buyers"] == 1
    finally:
        spark.stop()


def test_transform_filters_null_channel():
    spark = create_spark("test-null-channel")
    try:
        schema = "dt date, channel string, user_id long, amount decimal(12,2)"
        df = spark.createDataFrame(
            [(date(2026, 11, 20), None, 101, Decimal("1.00"))], schema
        )
        assert transform(df).count() == 0
    finally:
        spark.stop()
