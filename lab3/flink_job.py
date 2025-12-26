import json
import os
from datetime import datetime
from typing import Any, Dict, Optional

import psycopg2

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import MapFunction
from pyflink.common.watermark_strategy import WatermarkStrategy

_PG_CONN = None

def pg_conn():
    global _PG_CONN
    if _PG_CONN is None or _PG_CONN.closed:
        _PG_CONN = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        _PG_CONN.autocommit = True
    return _PG_CONN

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales_data")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "flink-streaming-etl")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "flink_db")
PG_USER = os.getenv("PG_USER", "flink_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "flink_password")
PG_SCHEMA = os.getenv("PG_SCHEMA", "dwh3")


def _parse_date(s: Any) -> Optional[datetime]:
    if not s:
        return None
    if isinstance(s, datetime):
        return s
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(str(s), fmt)
        except ValueError:
            continue
    return None


def _to_int(x: Any) -> Optional[int]:
    if x is None or x == "":
        return None
    try:
        return int(float(x))
    except Exception:
        return None


def _to_float(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None


class JsonToDict(MapFunction):
    def map(self, value: str) -> Dict[str, Any]:
        return json.loads(value)


class UpsertToPostgres(MapFunction):

    def open(self, runtime_context):
        self.conn = pg_conn()
        self.conn.autocommit = True
        print(f"[UpsertToPostgres] Connected to PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DB}")

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def _ensure_schema(self, table: str) -> str:
        return f"{PG_SCHEMA}.{table}"

    def _upsert_dim_customer(self, cur, row: Dict[str, Any]) -> Optional[int]:
        email = (row.get("customer_email") or "").strip()
        if not email:
            return None

        first_name = (row.get("customer_first_name") or "").strip() or None
        last_name = (row.get("customer_last_name") or "").strip() or None
        age = _to_int(row.get("customer_age"))
        country = (row.get("customer_country") or "").strip() or None
        postal_code = (row.get("customer_postal_code") or "").strip() or None
        pet_type = (row.get("customer_pet_type") or "").strip() or None
        pet_name = (row.get("customer_pet_name") or "").strip() or None
        pet_breed = (row.get("customer_pet_breed") or "").strip() or None

        cur.execute(
            f"""
            INSERT INTO {self._ensure_schema("dim_customer")}
              (customer_first_name, customer_last_name, customer_age, customer_email,
               customer_country, customer_postal_code, customer_pet_type, customer_pet_name, customer_pet_breed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_email) DO UPDATE
              SET customer_first_name = EXCLUDED.customer_first_name,
                  customer_last_name = EXCLUDED.customer_last_name,
                  customer_age = EXCLUDED.customer_age,
                  customer_country = EXCLUDED.customer_country,
                  customer_postal_code = EXCLUDED.customer_postal_code,
                  customer_pet_type = EXCLUDED.customer_pet_type,
                  customer_pet_name = EXCLUDED.customer_pet_name,
                  customer_pet_breed = EXCLUDED.customer_pet_breed
            RETURNING customer_id
            """,
            (first_name, last_name, age, email, country, postal_code, pet_type, pet_name, pet_breed),
        )
        return cur.fetchone()[0]

    def _upsert_dim_seller(self, cur, row: Dict[str, Any]) -> Optional[int]:
        email = (row.get("seller_email") or "").strip()
        if not email:
            return None

        first_name = (row.get("seller_first_name") or "").strip() or None
        last_name = (row.get("seller_last_name") or "").strip() or None
        country = (row.get("seller_country") or "").strip() or None
        postal_code = (row.get("seller_postal_code") or "").strip() or None

        cur.execute(
            f"""
            INSERT INTO {self._ensure_schema("dim_seller")}
              (seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (seller_email) DO UPDATE
              SET seller_first_name = EXCLUDED.seller_first_name,
                  seller_last_name = EXCLUDED.seller_last_name,
                  seller_country = EXCLUDED.seller_country,
                  seller_postal_code = EXCLUDED.seller_postal_code
            RETURNING seller_id
            """,
            (first_name, last_name, email, country, postal_code),
        )
        return cur.fetchone()[0]

    def _upsert_dim_product(self, cur, row: Dict[str, Any]) -> Optional[int]:
        name = (row.get("product_name") or "").strip()
        category = (row.get("product_category") or "").strip()
        if not name or not category:
            return None

        release_date = _parse_date(row.get("product_release_date"))
        brand = (row.get("product_brand") or "Unknown").strip()
        color = (row.get("product_color") or "Unknown").strip()
        size = (row.get("product_size") or "Standard").strip()
        price = _to_float(row.get("product_price"))
        quantity = _to_int(row.get("product_quantity"))
        pet_category = (row.get("pet_category") or "").strip() or None
        weight = _to_float(row.get("product_weight"))
        material = (row.get("product_material") or "").strip() or None
        description = (row.get("product_description") or "").strip() or None
        rating = _to_float(row.get("product_rating"))
        reviews = _to_int(row.get("product_reviews"))
        expiry_date = _parse_date(row.get("product_expiry_date"))

        cur.execute(
            f"""
            INSERT INTO {self._ensure_schema("dim_product")}
              (product_name, product_category, product_release_date, product_brand, product_color, product_size,
               product_price, product_quantity, pet_category, product_weight, product_material,
               product_description, product_rating, product_reviews, product_expiry_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_name, product_category, product_release_date, product_brand, product_color, product_size) 
            DO UPDATE
              SET product_price = EXCLUDED.product_price,
                  product_quantity = EXCLUDED.product_quantity,
                  pet_category = EXCLUDED.pet_category,
                  product_weight = EXCLUDED.product_weight,
                  product_material = EXCLUDED.product_material,
                  product_description = EXCLUDED.product_description,
                  product_rating = EXCLUDED.product_rating,
                  product_reviews = EXCLUDED.product_reviews,
                  product_expiry_date = EXCLUDED.product_expiry_date
            RETURNING product_id
            """,
            (name, category, release_date, brand, color, size, price, quantity,
             pet_category, weight, material, description, rating, reviews, expiry_date),
        )
        return cur.fetchone()[0]

    def _upsert_dim_store(self, cur, row: Dict[str, Any]) -> Optional[int]:
        email = (row.get("store_email") or "").strip()
        phone = (row.get("store_phone") or "").strip()
        if not email or not phone:
            return None

        name = (row.get("store_name") or "").strip() or None
        location = (row.get("store_location") or "").strip() or None
        city = (row.get("store_city") or "").strip() or None
        state = (row.get("store_state") or "").strip() or None
        country = (row.get("store_country") or "").strip() or None

        cur.execute(
            f"""
            INSERT INTO {self._ensure_schema("dim_store")}
              (store_name, store_location, store_city, store_state, store_country, store_phone, store_email)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (store_email, store_phone) DO UPDATE
              SET store_name = EXCLUDED.store_name,
                  store_location = EXCLUDED.store_location,
                  store_city = EXCLUDED.store_city,
                  store_state = EXCLUDED.store_state,
                  store_country = EXCLUDED.store_country
            RETURNING store_id
            """,
            (name, location, city, state, country, phone, email),
        )
        return cur.fetchone()[0]

    def _upsert_dim_supplier(self, cur, row: Dict[str, Any]) -> Optional[int]:
        email = (row.get("supplier_email") or "").strip()
        if not email:
            return None

        name = (row.get("supplier_name") or "").strip() or None
        contact = (row.get("supplier_contact") or "").strip() or None
        phone = (row.get("supplier_phone") or "").strip() or None
        address = (row.get("supplier_address") or "").strip() or None
        city = (row.get("supplier_city") or "").strip() or None
        country = (row.get("supplier_country") or "").strip() or None

        cur.execute(
            f"""
            INSERT INTO {self._ensure_schema("dim_supplier")}
              (supplier_name, supplier_contact, supplier_email, supplier_phone,
               supplier_address, supplier_city, supplier_country)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (supplier_email) DO UPDATE
              SET supplier_name = EXCLUDED.supplier_name,
                  supplier_contact = EXCLUDED.supplier_contact,
                  supplier_phone = EXCLUDED.supplier_phone,
                  supplier_address = EXCLUDED.supplier_address,
                  supplier_city = EXCLUDED.supplier_city,
                  supplier_country = EXCLUDED.supplier_country
            RETURNING supplier_id
            """,
            (name, contact, email, phone, address, city, country),
        )
        return cur.fetchone()[0]

    def _insert_fact(self, cur, row: Dict[str, Any], keys: Dict[str, Optional[int]]):
        sale_date = _parse_date(row.get("sale_date"))
        if sale_date is None:
            return

        quantity = _to_int(row.get("sale_quantity")) or 0
        total_price = _to_float(row.get("sale_total_price")) or 0.0

        cur.execute(
            f"""
            INSERT INTO {self._ensure_schema("fact_sales")}
              (customer_id, seller_id, product_id, store_id, supplier_id,
               sale_date, sale_quantity, sale_total_price)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                keys.get("customer_id"),
                keys.get("seller_id"),
                keys.get("product_id"),
                keys.get("store_id"),
                keys.get("supplier_id"),
                sale_date,
                quantity,
                total_price,
            ),
        )

    def map(self, row: Dict[str, Any]) -> str:
        try:
            with self.conn.cursor() as cur:
                keys = {
                    "customer_id": self._upsert_dim_customer(cur, row),
                    "seller_id": self._upsert_dim_seller(cur, row),
                    "product_id": self._upsert_dim_product(cur, row),
                    "store_id": self._upsert_dim_store(cur, row),
                    "supplier_id": self._upsert_dim_supplier(cur, row),
                }
                self._insert_fact(cur, row, keys)
            return f"✓ OK"
        except Exception as e:
            return f"✗ Error: {type(e).__name__}: {e}"


def main():
    print("=" * 80)
    print("=== Flink Streaming ETL to Star Schema ===")
    print("=" * 80)
    print(f"Kafka: {KAFKA_BOOTSTRAP} | Topic: {KAFKA_TOPIC}")
    print(f"PostgreSQL: {PG_HOST}:{PG_PORT}/{PG_DB} | Schema: {PG_SCHEMA}")
    print("=" * 80)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "KafkaSource")

    result = (
        ds.map(JsonToDict(), output_type=Types.PICKLED_BYTE_ARRAY())
          .map(UpsertToPostgres(), output_type=Types.STRING())
    )

    result.print()

    print("Starting Flink job...")
    env.execute("Streaming ETL: Kafka → Flink → PostgreSQL Star Schema")


if __name__ == "__main__":
    main()
