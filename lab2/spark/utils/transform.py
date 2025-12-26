from pyspark.sql.functions import col, to_date
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    StringType
)

def cast_common(df):
    return df \
        .withColumn("customer_age", col("customer_age").cast(IntegerType())) \
        .withColumn("product_quantity", col("product_quantity").cast(IntegerType())) \
        .withColumn("sale_customer_id", col("sale_customer_id").cast(IntegerType())) \
        .withColumn("sale_seller_id", col("sale_seller_id").cast(IntegerType())) \
        .withColumn("sale_product_id", col("sale_product_id").cast(IntegerType())) \
        .withColumn("sale_quantity", col("sale_quantity").cast(IntegerType())) \
        .withColumn("product_reviews", col("product_reviews").cast(IntegerType())) \
        .withColumn("product_price", col("product_price").cast(DoubleType())) \
        .withColumn("sale_total_price", col("sale_total_price").cast(DoubleType())) \
        .withColumn("product_weight", col("product_weight").cast(DoubleType())) \
        .withColumn("product_rating", col("product_rating").cast(DoubleType())) \
        .withColumn("sale_date", to_date(col("sale_date"), "M/d/yyyy")) \
        .withColumn("product_release_date", to_date(col("product_release_date"), "M/d/yyyy")) \
        .withColumn("product_expiry_date", to_date(col("product_expiry_date"), "M/d/yyyy")) \
        .withColumn("customer_first_name", col("customer_first_name").cast(StringType())) \
        .withColumn("customer_last_name", col("customer_last_name").cast(StringType())) \
        .withColumn("customer_email", col("customer_email").cast(StringType())) \
        .withColumn("customer_country", col("customer_country").cast(StringType())) \
        .withColumn("customer_postal_code", col("customer_postal_code").cast(StringType())) \
        .withColumn("customer_pet_type", col("customer_pet_type").cast(StringType())) \
        .withColumn("customer_pet_name", col("customer_pet_name").cast(StringType())) \
        .withColumn("customer_pet_breed", col("customer_pet_breed").cast(StringType())) \
        .withColumn("seller_first_name", col("seller_first_name").cast(StringType())) \
        .withColumn("seller_last_name", col("seller_last_name").cast(StringType())) \
        .withColumn("seller_email", col("seller_email").cast(StringType())) \
        .withColumn("seller_country", col("seller_country").cast(StringType())) \
        .withColumn("seller_postal_code", col("seller_postal_code").cast(StringType())) \
        .withColumn("product_name", col("product_name").cast(StringType())) \
        .withColumn("product_category", col("product_category").cast(StringType())) \
        .withColumn("store_name", col("store_name").cast(StringType())) \
        .withColumn("store_location", col("store_location").cast(StringType())) \
        .withColumn("store_city", col("store_city").cast(StringType())) \
        .withColumn("store_state", col("store_state").cast(StringType())) \
        .withColumn("store_country", col("store_country").cast(StringType())) \
        .withColumn("store_phone", col("store_phone").cast(StringType())) \
        .withColumn("store_email", col("store_email").cast(StringType())) \
        .withColumn("pet_category", col("pet_category").cast(StringType())) \
        .withColumn("product_color", col("product_color").cast(StringType())) \
        .withColumn("product_size", col("product_size").cast(StringType())) \
        .withColumn("product_brand", col("product_brand").cast(StringType())) \
        .withColumn("product_material", col("product_material").cast(StringType())) \
        .withColumn("product_description", col("product_description").cast(StringType())) \
        .withColumn("supplier_name", col("supplier_name").cast(StringType())) \
        .withColumn("supplier_contact", col("supplier_contact").cast(StringType())) \
        .withColumn("supplier_email", col("supplier_email").cast(StringType())) \
        .withColumn("supplier_phone", col("supplier_phone").cast(StringType())) \
        .withColumn("supplier_address", col("supplier_address").cast(StringType())) \
        .withColumn("supplier_city", col("supplier_city").cast(StringType())) \
        .withColumn("supplier_country", col("supplier_country").cast(StringType()))
