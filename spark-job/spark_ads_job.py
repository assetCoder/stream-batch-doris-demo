#!/usr/bin/env python3
"""Spark 4.1 离线作业：Doris DWD -> 日/渠道 ADS -> Doris。

设计原则：
1. 主流程只做三件事：读、算、写；聚合逻辑放在纯函数 transform() 里，方便本地测试。
2. 默认用 Spark Doris Connector（列式读取、Stream Load 写入）；
   如果集群没有装 connector，可用 --reader jdbc / --writer jdbc 退回 JDBC。
3. dry-run 不连 Doris，用本地 JSON 验证指标口径，适合 CI 和初学者。

Spark 4.1 对应 connector：
  org.apache.doris:spark-doris-connector-spark-4.1:26.1.0
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class JobConfig:
    fenodes: str
    database: str
    dwd_table: str
    ads_table: str
    user: str
    password: str
    dt: str | None
    mode: str          # doris | jdbc
    input_path: str | None
    dry_run: bool

    @property
    def dwd_identifier(self) -> str:
        return f"{self.database}.{self.dwd_table}"

    @property
    def ads_identifier(self) -> str:
        return f"{self.database}.{self.ads_table}"

    @property
    def jdbc_url(self) -> str:
        # fenodes 是 8030（HTTP）；JDBC 必须走 9030（MySQL 协议）。
        host = self.fenodes.split(",")[0].strip().replace(":8030", ":9030")
        return (f"jdbc:mysql://{host}/{self.database}"
                "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai")


def parse_args(argv: list[str] | None = None) -> JobConfig:
    parser = argparse.ArgumentParser(description="Doris DWD -> ADS 离线聚合")
    parser.add_argument("--fenodes", default=os.getenv("DORIS_FENODES", "127.0.0.1:8030"))
    parser.add_argument("--database", default=os.getenv("DORIS_DATABASE", "demo"))
    parser.add_argument("--dwd-table", default=os.getenv("DORIS_DWD_TABLE", "order_events"))
    parser.add_argument("--ads-table", default=os.getenv("DORIS_ADS_TABLE", "ads_daily_channel"))
    parser.add_argument("--user", default=os.getenv("DORIS_USERNAME", "root"))
    parser.add_argument("--password", default=os.getenv("DORIS_PASSWORD", ""))
    parser.add_argument("--dt", default=os.getenv("ADS_DT"), help="只处理某一天，格式 yyyy-MM-dd；不传则全量")
    parser.add_argument("--mode", choices=["doris", "jdbc"], default=os.getenv("SPARK_DORIS_MODE", "doris"))
    parser.add_argument("--input", dest="input_path", help="dry-run 用的本地 JSON 文件")
    parser.add_argument("--dry-run", action="store_true", help="不连 Doris，只打印聚合结果")
    args = parser.parse_args(argv)
    if args.dt:
        # 提前校验，避免把非法日期带到 Spark 里才报错
        date.fromisoformat(args.dt)
    return JobConfig(
        fenodes=args.fenodes,
        database=args.database,
        dwd_table=args.dwd_table,
        ads_table=args.ads_table,
        user=args.user,
        password=args.password,
        dt=args.dt,
        mode=args.mode,
        input_path=args.input_path,
        dry_run=args.dry_run,
    )


def create_spark(app_name: str = "doris-ads-job") -> SparkSession:
    """本地/集群通用；Doris 的 DATETIME 不带时区，统一用上海时区解释。"""
    # 本机同时装了 3.12 和 3.14 时，Spark worker 必须和 driver 用同一个解释器。
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "Asia/Shanghai")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )


def read_dwd(spark: SparkSession, cfg: JobConfig) -> DataFrame:
    """按模式读取 DWD；doris 模式走 connector，jdbc 模式走 MySQL 协议。"""
    if cfg.dry_run:
        if not cfg.input_path:
            raise SystemExit("dry-run 需要 --input 指定本地 JSON 文件")
        return spark.read.option("multiLine", "true").json(cfg.input_path)

    if cfg.mode == "jdbc":
        reader = (
            spark.read.format("jdbc")
            .option("url", cfg.jdbc_url)
            .option("dbtable", cfg.dwd_table)
            .option("user", cfg.user)
            .option("password", cfg.password)
            .option("driver", "com.mysql.cj.jdbc.Driver")
        )
        return reader.load()

    return (
        spark.read.format("doris")
        .option("doris.fenodes", cfg.fenodes)
        .option("doris.table.identifier", cfg.dwd_identifier)
        .option("user", cfg.user)
        .option("password", cfg.password)
        .load()
    )


def transform(df: DataFrame, dt: str | None = None) -> DataFrame:
    """纯聚合逻辑：按天 + 渠道算 GMV、订单数、买家数、客单价。

    输入列：dt(DATE), channel(STRING), user_id(BIGINT), amount(DECIMAL)
    输出列：dt, channel, gmv, orders, buyers, avg_buyer_value
    """
    # JSON 里 dt 可能是字符串，Doris 里是 DATE；统一转成 DATE 再比较/分组。
    df = df.withColumn("dt", F.to_date(F.col("dt")))
    if dt:
        df = df.filter(F.col("dt") == F.lit(dt).cast("date"))

    return (
        df.filter(F.col("dt").isNotNull() & F.col("channel").isNotNull())
        .groupBy("dt", "channel")
        .agg(
            F.sum("amount").cast("decimal(18,2)").alias("gmv"),
            F.count("*").cast("long").alias("orders"),
            F.countDistinct("user_id").cast("long").alias("buyers"),
            (F.sum("amount") / F.countDistinct("user_id"))
            .cast("decimal(18,2)").alias("avg_buyer_value"),
        )
        .orderBy("dt", "channel")
    )


def write_ads(df: DataFrame, cfg: JobConfig) -> None:
    """ADS 用 Unique Key(dt, channel)，append 即 upsert；重跑同一天不会翻倍。"""
    if cfg.dry_run:
        df.show(50, truncate=False)
        return

    if cfg.mode == "jdbc":
        (
            df.write.format("jdbc")
            .option("url", cfg.jdbc_url)
            .option("dbtable", cfg.ads_table)
            .option("user", cfg.user)
            .option("password", cfg.password)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .mode("append")
            .save()
        )
        return

    (
        df.write.format("doris")
        .option("doris.fenodes", cfg.fenodes)
        .option("doris.table.identifier", cfg.ads_identifier)
        .option("user", cfg.user)
        .option("password", cfg.password)
        .option("doris.sink.label.prefix", "spark-ads-")
        .option("doris.sink.enable-2pc", "true")
        .option("doris.sink.batch.size", "1000")
        .option("doris.sink.max-retries", "3")
        .mode("append")
        .save()
    )


def main(argv: list[str] | None = None) -> int:
    cfg = parse_args(argv)
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        dwd = read_dwd(spark, cfg)
        ads = transform(dwd, cfg.dt)
        if cfg.dry_run:
            print("dry-run 聚合结果：")
        write_ads(ads, cfg)
        if cfg.dry_run:
            # 额外给一个默认日期窗口示例，方便手工验证
            print("默认日期窗口示例（不传 --dt 时由调用方决定）：",
                  (date.today() - timedelta(days=1)).isoformat())
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
