set -e

PG_HOST="postgres"
PG_PORT=5432

CH_URL="${CH_URL:-jdbc:clickhouse://clickhouse:8123/default}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-}"


spark-submit /app/spark/jobs/load_raw_csv.py \
    --input /app/data \
    --pg-url jdbc:postgresql://postgres:5432/snowflake_db \
    --pg-user user \
    --pg-password password
echo "RAW loading complete."

spark-submit /app/spark/jobs/build_dimensions.py \
    --pg-url jdbc:postgresql://postgres:5432/snowflake_db \
    --pg-user user \
    --pg-password password
echo "Dimensions built."

spark-submit /app/spark/jobs/build_fact_sales.py \
    --pg-url jdbc:postgresql://postgres:5432/snowflake_db \
    --pg-user user \
    --pg-password password
echo "Fact table built."

spark-submit \
  --jars /opt/spark/jars/clickhouse-jdbc-0.9.4-all.jar \
  /app/spark/jobs/export_to_clickhouse.py \
  --pg-url jdbc:postgresql://postgres:5432/snowflake_db \
  --pg-user user \
  --pg-password password \
  --ch-url jdbc:clickhouse://ch:8123/default \
  --ch-user default \
  --ch-password 1234 \
  --mode overwrite

# Чтобы контейнер не закрылся:
echo "Spark ETL finished"
tail -f /dev/null
