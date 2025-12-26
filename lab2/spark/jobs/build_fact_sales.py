import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit
from spark.utils.db import pg_options
from spark.utils.transform import cast_common

def main(args):
    spark = SparkSession.builder.appName("build_fact_sales").getOrCreate()

    raw = spark.read.format("jdbc").options(**pg_options(args.pg_url, "public.mock_data", args.pg_user, args.pg_password)).load()

    raw = raw.withColumnRenamed("id", "source_id")

    df_cust = spark.read.format("jdbc").options(**pg_options(args.pg_url, "dwh3.dim_customer", args.pg_user, args.pg_password)).load().select("customer_id","customer_email")
    df_seller = spark.read.format("jdbc").options(**pg_options(args.pg_url, "dwh3.dim_seller", args.pg_user, args.pg_password)).load().select("seller_id","seller_email")
    df_product = spark.read.format("jdbc").options(**pg_options(args.pg_url, "dwh3.dim_product", args.pg_user, args.pg_password)).load().select(
        "product_id","product_name","product_category","product_release_date","product_brand","product_color","product_size"
    )
    df_store = spark.read.format("jdbc").options(**pg_options(args.pg_url, "dwh3.dim_store", args.pg_user, args.pg_password)).load().select("store_id","store_email","store_phone")
    df_supplier = spark.read.format("jdbc").options(**pg_options(args.pg_url, "dwh3.dim_supplier", args.pg_user, args.pg_password)).load().select("supplier_id","supplier_email")

    fact = raw.join(df_cust, raw.customer_email == df_cust.customer_email, how="left") \
             .join(df_seller, raw.seller_email == df_seller.seller_email, how="left") \
             .join(df_product,
                   (raw.product_name == df_product.product_name) &
                   (raw.product_category == df_product.product_category) &
                   (raw.product_release_date == df_product.product_release_date) &
                   (coalesce(raw.product_brand, lit("Unknown")) == df_product.product_brand) &
                   (coalesce(raw.product_color, lit("Unknown")) == df_product.product_color) &
                   (coalesce(raw.product_size, lit("Standard")) == df_product.product_size),
                   how="left") \
             .join(df_store, (raw.store_email == df_store.store_email) & (raw.store_phone == df_store.store_phone), how="left") \
             .join(df_supplier, raw.supplier_email == df_supplier.supplier_email, how="left")

    fact_to_write = fact.select(
        col("customer_id"),
        col("seller_id"),
        col("product_id"),
        col("store_id"),
        col("supplier_id"),
        col("sale_date"),
        col("sale_quantity"),
        col("sale_total_price"),
        col("source_id")
    )

    fact_to_write.write.format("jdbc").options(**pg_options(args.pg_url, "dwh3.fact_sales", args.pg_user, args.pg_password)).mode("append").save()
    print("Fact rows written:", fact_to_write.count())

    spark.stop()

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--pg-url", default="jdbc:postgresql://postgres:5432/snowflake_db")
    p.add_argument("--pg-user", default="user")
    p.add_argument("--pg-password", default="password")
    args = p.parse_args()
    main(args)
