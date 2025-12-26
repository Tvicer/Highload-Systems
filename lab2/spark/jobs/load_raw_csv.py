import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from spark.utils.transform import cast_common
from spark.utils.db import pg_options

def main(args):
    spark = SparkSession.builder.appName("load_raw_csv").getOrCreate()

    input_pattern = f"{args.input}/MOCK_DATA*.csv"
    df = spark.read \
        .option("header", "true") \
        .option("sep", ",") \
        .option("multiline", "true") \
        .csv(input_pattern)
    # if "id" in df.columns:
    #     df = df.withColumnRenamed("id", "source_id")

    if "id" in df.columns:
        df = df.drop("id")

    df = cast_common(df)

    options = pg_options(args.pg_url, "public.mock_data", args.pg_user, args.pg_password)
    write_mode = "append" if not args.overwrite else "overwrite"

    # write to postgres
    df.write.format("jdbc").options(**options).mode(write_mode).save()
    print("WROTE raw rows:", df.count())
    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="/app/data", help="input dir with CSVs")
    p.add_argument("--pg-url", default="jdbc:postgresql://postgres:5432/snowflake_db")
    p.add_argument("--pg-user", default="user")
    p.add_argument("--pg-password", default="password")
    p.add_argument("--overwrite", action="store_true", help="overwrite raw table")
    args = p.parse_args()
    main(args)
