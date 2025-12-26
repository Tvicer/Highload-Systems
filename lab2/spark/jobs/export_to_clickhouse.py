import argparse
import sys
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, year, month, to_date,
    sum as spark_sum,
    avg as spark_avg,
    count as spark_count,
    desc, lag, when
)
from pyspark.sql.window import Window


def init_spark(app_name, master=None, jars=None, log_level="WARN"):
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    if jars:
        builder = (
            builder
            .config("spark.jars", jars)
            .config("spark.driver.extraClassPath", jars)
            .config("spark.executor.extraClassPath", jars)
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark


def read_pg(spark, url, table, user, password):
    return (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def write_ch(df: DataFrame, url: str, table: str, user: str, password: str, mode: str = "append"):
    (
        df.write
        .format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .mode(mode)
        .save()
    )


def product_vitrines(fact: DataFrame, product: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    joined = fact.join(product, "product_id", "left")

    top10 = (
        joined.groupBy("product_id", "product_name")
        .agg(
            spark_sum("sale_quantity").alias("total_quantity"),
            spark_sum("sale_total_price").alias("total_revenue"),
        )
        .orderBy(desc("total_quantity"))
        .limit(10)
    )

    revenue_by_category = (
        joined.groupBy("product_category")
        .agg(spark_sum("sale_total_price").alias("total_revenue"))
        .orderBy(desc("total_revenue"))
    )

    rating_stats = (
        product.groupBy("product_id", "product_name")
        .agg(
            spark_avg("product_rating").alias("avg_rating"),
            spark_sum("product_reviews").alias("total_reviews")
        )
    )

    return top10, revenue_by_category, rating_stats


def customer_vitrines(fact: DataFrame, customer: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    joined = fact.join(customer, "customer_id", "left")

    top10_customers = (
        joined.groupBy("customer_id", "customer_email")
        .agg(spark_sum("sale_total_price").alias("total_spent"))
        .orderBy(desc("total_spent"))
        .limit(10)
    )

    distribution_by_country = (
        joined.groupBy("customer_country")
        .agg(
            spark_count("sale_id").alias("orders_count"),
            spark_sum("sale_total_price").alias("total_revenue")
        )
        .orderBy(desc("total_revenue"))
    )

    avg_check_by_customer = (
        joined.groupBy("customer_id", "customer_email")
        .agg(
            spark_sum("sale_total_price").alias("total_spent"),
            spark_count("sale_id").alias("orders_count")
        )
        .withColumn("avg_check", col("total_spent") / col("orders_count"))
    )

    return top10_customers, distribution_by_country, avg_check_by_customer


def time_vitrines(fact: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    f = fact.withColumn("sale_date", to_date("sale_date"))
    f = f.withColumn("year", year("sale_date")).withColumn("month", month("sale_date"))

    monthly = (
        f.groupBy("year", "month")
        .agg(
            spark_sum("sale_total_price").alias("total_revenue"),
            spark_sum("sale_quantity").alias("total_quantity")
        )
        .orderBy("year", "month")
    )

    yearly = (
        f.groupBy("year")
        .agg(
            spark_sum("sale_total_price").alias("total_revenue"),
            spark_sum("sale_quantity").alias("total_quantity")
        )
        .orderBy("year")
    )

    w = Window.partitionBy("year").orderBy("month")
    revenue_comp = (
        monthly
        .withColumn("prev_revenue", lag("total_revenue").over(w))
        .withColumn(
            "mom_growth",
            (col("total_revenue") - col("prev_revenue")) / col("prev_revenue")
        )
    )

    avg_order = (
        f.groupBy("year", "month", "sale_id")
        .agg(spark_sum("sale_total_price").alias("order_total"))
        .groupBy("year", "month")
        .agg(spark_avg("order_total").alias("avg_order_value"))
        .orderBy("year", "month")
    )

    return monthly, yearly, revenue_comp, avg_order


def store_vitrines(fact: DataFrame, store: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    joined = fact.join(store, "store_id", "left")

    top5_stores = (
        joined.groupBy("store_id", "store_name")
        .agg(spark_sum("sale_total_price").alias("total_revenue"))
        .orderBy(desc("total_revenue"))
        .limit(5)
    )

    store_geo = (
        joined.groupBy("store_country", "store_city")
        .agg(spark_sum("sale_total_price").alias("total_revenue"))
        .orderBy(desc("total_revenue"))
    )

    avg_check_store = (
        joined.groupBy("store_id", "store_name", "sale_id")
        .agg(spark_sum("sale_total_price").alias("order_total"))
        .groupBy("store_id", "store_name")
        .agg(spark_avg("order_total").alias("avg_check"))
    )

    return top5_stores, store_geo, avg_check_store


def supplier_vitrines(fact: DataFrame, supplier: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
    joined = fact.join(supplier, "supplier_id", "left")

    top5_suppliers = (
        joined.groupBy("supplier_id", "supplier_name")
        .agg(spark_sum("sale_total_price").alias("total_revenue"))
        .orderBy(desc("total_revenue"))
        .limit(5)
    )

    avg_price_by_supplier = (
        joined
        .withColumn(
            "unit_price",
            when(col("sale_quantity") > 0, col("sale_total_price") / col("sale_quantity"))
        )
        .groupBy("supplier_id", "supplier_name")
        .agg(spark_avg("unit_price").alias("avg_unit_price"))
    )

    suppliers_by_country = (
        joined.groupBy("supplier_country")
        .agg(spark_sum("sale_total_price").alias("total_revenue"))
        .orderBy(desc("total_revenue"))
    )

    return top5_suppliers, avg_price_by_supplier, suppliers_by_country


def quality_vitrines(fact: DataFrame, product: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    joined = fact.join(product, "product_id", "left")

    agg = (
        joined.groupBy("product_id", "product_name")
        .agg(
            spark_avg("product_rating").alias("avg_rating"),
            spark_sum("product_reviews").alias("total_reviews"),
            spark_sum("sale_quantity").alias("total_quantity")
        )
    )

    best_products = agg.orderBy(desc("avg_rating")).limit(10)
    worst_products = agg.orderBy("avg_rating").limit(10)
    most_reviewed = agg.orderBy(desc("total_reviews")).limit(20)

    try:
        corr_value = agg.stat.corr("avg_rating", "total_quantity")
    except Exception:
        corr_value = None

    corr_df = fact.sql_ctx.sparkSession.createDataFrame(
        [(corr_value,)],
        ["rating_sales_correlation"]
    )

    return best_products, worst_products, corr_df, most_reviewed


def main(argv):
    p = argparse.ArgumentParser()
    p.add_argument("--pg-url", required=True)
    p.add_argument("--pg-user", required=True)
    p.add_argument("--pg-password", required=True)
    p.add_argument("--ch-url", required=True)
    p.add_argument("--ch-user", default="default")
    p.add_argument("--ch-password", default="1234")
    p.add_argument("--mode", default="append", choices=["append", "overwrite"])
    p.add_argument("--jars", default=None)
    p.add_argument("--master", default=None)
    args = p.parse_args(argv)

    spark = init_spark("star_to_clickhouse", master=args.master, jars=args.jars)

    # args.ch_password = ""

    print("args:", args)

    fact = read_pg(spark, args.pg_url, "dwh3.fact_sales", args.pg_user, args.pg_password)
    product = read_pg(spark, args.pg_url, "dwh3.dim_product", args.pg_user, args.pg_password)
    customer = read_pg(spark, args.pg_url, "dwh3.dim_customer", args.pg_user, args.pg_password)
    store = read_pg(spark, args.pg_url, "dwh3.dim_store", args.pg_user, args.pg_password)
    supplier = read_pg(spark, args.pg_url, "dwh3.dim_supplier", args.pg_user, args.pg_password)

    monthly, yearly, revenue_comp, avg_order = time_vitrines(fact)
    write_ch(monthly, args.ch_url, "time_monthly", args.ch_user, args.ch_password, mode=args.mode)
    write_ch(yearly, args.ch_url, "time_yearly", args.ch_user, args.ch_password, mode=args.mode)
    write_ch(revenue_comp, args.ch_url, "time_revenue_comp", args.ch_user, args.ch_password, mode=args.mode)
    write_ch(avg_order, args.ch_url, "time_avg_order", args.ch_user, args.ch_password, mode=args.mode)

    top10, revenue_by_cat, rating_stats = product_vitrines(fact, product)
    write_ch(top10, args.ch_url, "products_top10", args.ch_user, args.ch_password, mode=args.mode)
    write_ch(revenue_by_cat, args.ch_url, "revenue_by_category", args.ch_user, args.ch_password, mode=args.mode)
    write_ch(rating_stats, args.ch_url, "product_rating_stats", args.ch_user, args.ch_password, mode=args.mode)

    c_top10, c_by_country, c_avg_check = customer_vitrines(fact, customer)
    write_ch(c_top10, args.ch_url, "customers_top10", args.ch_user, args.ch_password, mode=args.mode)
    write_ch(c_by_country, args.ch_url, "customers_by_country", args.ch_user, args.ch_password, mode=args.mode)
    write_ch(c_avg_check, args.ch_url, "customer_avg_check", args.ch_user, args.ch_password, mode=args.mode)

    s_top5, s_geo, s_avg_check = store_vitrines(fact, store)
    write_ch(s_top5, args.ch_url, "stores_top5", args.ch_user, args.ch_password, mode=args.mode)
    write_ch(s_geo, args.ch_url, "stores_geo", args.ch_user, args.ch_password, mode=args.mode)
    write_ch(s_avg_check, args.ch_url, "stores_avg_check", args.ch_user, args.ch_password, mode=args.mode)

    sup_top5, sup_avg_price, sup_by_country = supplier_vitrines(fact, supplier)
    write_ch(sup_top5, args.ch_url, "suppliers_top5", args.ch_user, args.ch_password, mode=args.mode)
    write_ch(sup_avg_price, args.ch_url, "suppliers_avg_price", args.ch_user, args.ch_password, mode=args.mode)
    write_ch(sup_by_country, args.ch_url, "suppliers_by_country", args.ch_user, args.ch_password, mode=args.mode)

    best, worst, corr_df, most_reviewed = quality_vitrines(fact, product)
    write_ch(best, args.ch_url, "products_best", args.ch_user, args.ch_password, mode=args.mode)
    write_ch(worst, args.ch_url, "products_worst", args.ch_user, args.ch_password, mode=args.mode)
    write_ch(corr_df, args.ch_url, "rating_sales_corr", args.ch_user, args.ch_password, mode=args.mode)
    write_ch(most_reviewed, args.ch_url, "products_most_reviewed", args.ch_user, args.ch_password, mode=args.mode)

    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
