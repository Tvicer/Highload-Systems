import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit
from spark.utils.db import pg_options
from spark.utils.transform import cast_common

def read_table(spark, pg_url, table, user, password):
    try:
        return spark.read.format("jdbc").options(**pg_options(pg_url, table, user, password)).load()
    except Exception as e:
        print(f"Cannot read {table}: {e}")
        return None

def anti_join_new(existing_df, new_df, keys):
    if existing_df is None or existing_df.rdd.isEmpty():
        return new_df
    return new_df.join(existing_df.select(*keys), on=keys, how="left_anti")

def main(args):
    spark = SparkSession.builder.appName("build_dimensions").getOrCreate()

    raw = spark.read.format("jdbc").options(**pg_options(args.pg_url, "public.mock_data", args.pg_user, args.pg_password)).load()
    raw = cast_common(raw)

    dim_customer_new = raw.select(
        "customer_first_name","customer_last_name","customer_age",
        "customer_email","customer_country","customer_postal_code",
        "customer_pet_type","customer_pet_name","customer_pet_breed"
    ).where(col("customer_email").isNotNull()).dropDuplicates(["customer_email"])

    dim_customer_existing = read_table(spark, args.pg_url, "dwh3.dim_customer", args.pg_user, args.pg_password)
    to_insert_cust = anti_join_new(dim_customer_existing, dim_customer_new, ["customer_email"])
    if to_insert_cust and not to_insert_cust.rdd.isEmpty():
        to_insert_cust.write.format("jdbc").options(**pg_options(args.pg_url, "dwh3.dim_customer", args.pg_user, args.pg_password)).mode("append").save()
        print("Inserted customers:", to_insert_cust.count())
    else:
        print("No new customers")

    dim_seller_new = raw.select(
        "seller_first_name","seller_last_name","seller_email",
        "seller_country","seller_postal_code"
    ).where(col("seller_email").isNotNull()).dropDuplicates(["seller_email"])

    dim_seller_existing = read_table(spark, args.pg_url, "dwh3.dim_seller", args.pg_user, args.pg_password)
    to_insert_seller = anti_join_new(dim_seller_existing, dim_seller_new, ["seller_email"])
    if to_insert_seller and not to_insert_seller.rdd.isEmpty():
        to_insert_seller.write.format("jdbc").options(**pg_options(args.pg_url, "dwh3.dim_seller", args.pg_user, args.pg_password)).mode("append").save()
        print("Inserted sellers:", to_insert_seller.count())
    else:
        print("No new sellers")

    product_keys = ["product_name","product_category","product_release_date","product_brand","product_color","product_size"]
    dim_product_new = raw.select(
        "product_name","product_category","product_release_date",
        coalesce(col("product_brand"), lit("Unknown")).alias("product_brand"),
        coalesce(col("product_color"), lit("Unknown")).alias("product_color"),
        coalesce(col("product_size"), lit("Standard")).alias("product_size"),
        "product_price","product_quantity","pet_category","product_weight",
        "product_material","product_description","product_rating","product_reviews","product_expiry_date"
    ).where(col("product_name").isNotNull() & col("product_category").isNotNull() & col("product_release_date").isNotNull()) \
     .dropDuplicates(product_keys)

    dim_product_existing = read_table(spark, args.pg_url, "dwh3.dim_product", args.pg_user, args.pg_password)
    if dim_product_existing is None:
        to_insert_product = dim_product_new
    else:
        join_cond = [dim_product_new[k] == dim_product_existing[k] for k in product_keys]
        to_insert_product = dim_product_new.join(dim_product_existing.select(*product_keys), on=product_keys, how="left_anti")

    if to_insert_product and not to_insert_product.rdd.isEmpty():
        to_insert_product.write.format("jdbc").options(**pg_options(args.pg_url, "dwh3.dim_product", args.pg_user, args.pg_password)).mode("append").save()
        print("Inserted products:", to_insert_product.count())
    else:
        print("No new products")

    dim_store_new = raw.select(
        "store_name", "store_location", "store_city", "store_state",
        "store_country", "store_phone", "store_email"
    ).where(col("store_email").isNotNull() & col("store_phone").isNotNull()).dropDuplicates(["store_email","store_phone"])

    dim_store_existing = read_table(spark, args.pg_url, "dwh3.dim_store", args.pg_user, args.pg_password)
    to_insert_store = anti_join_new(dim_store_existing, dim_store_new, ["store_email","store_phone"])
    if to_insert_store and not to_insert_store.rdd.isEmpty():
        to_insert_store.write.format("jdbc").options(**pg_options(args.pg_url, "dwh3.dim_store", args.pg_user, args.pg_password)).mode("append").save()
        print("Inserted stores:", to_insert_store.count())
    else:
        print("No new stores")

    dim_supplier_new = raw.select(
        "supplier_name","supplier_contact","supplier_email","supplier_phone",
        "supplier_address","supplier_city","supplier_country"
    ).where(col("supplier_email").isNotNull()).dropDuplicates(["supplier_email"])

    dim_supplier_existing = read_table(spark, args.pg_url, "dwh3.dim_supplier", args.pg_user, args.pg_password)
    to_insert_supplier = anti_join_new(dim_supplier_existing, dim_supplier_new, ["supplier_email"])
    if to_insert_supplier and not to_insert_supplier.rdd.isEmpty():
        to_insert_supplier.write.format("jdbc").options(**pg_options(args.pg_url, "dwh3.dim_supplier", args.pg_user, args.pg_password)).mode("append").save()
        print("Inserted suppliers:", to_insert_supplier.count())
    else:
        print("No new suppliers")

    spark.stop()

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--pg-url", default="jdbc:postgresql://postgres:5432/snowflake_db")
    p.add_argument("--pg-user", default="user")
    p.add_argument("--pg-password", default="password")
    args = p.parse_args()
    main(args)
