-- clickhouse

SHOW TABLES;


-- Витрина качества продукции

-- postgres
SELECT
  p.product_id,
  p.product_name,
  SUM(f.sale_quantity)        AS total_quantity,
  SUM(f.sale_total_price)     AS total_revenue
FROM dwh3.fact_sales f
JOIN dwh3.dim_product p ON p.product_id = f.product_id
GROUP BY p.product_id, p.product_name
ORDER BY total_quantity DESC
LIMIT 10;

-- clickhouse

SELECT product_id, product_name, total_quantity, total_revenue
FROM products_top10
ORDER BY total_quantity DESC
LIMIT 10;

-- Общая выручка по категориям продуктов

-- postgres
SELECT
  p.product_category,
  SUM(f.sale_total_price) AS total_revenue,
  SUM(f.sale_quantity)    AS total_quantity
FROM dwh3.fact_sales f
JOIN dwh3.dim_product p ON p.product_id = f.product_id
GROUP BY p.product_category
ORDER BY total_revenue DESC;

-- clickhouse
SELECT product_category, total_revenue
FROM revenue_by_category
ORDER BY total_revenue DESC;

--Средний рейтинг и количество отзывов для каждого продукта

-- postgres
SELECT
  product_id,
  product_name,
  AVG(product_rating)    AS avg_rating,
  SUM(product_reviews)   AS total_reviews
FROM dwh3.dim_product
GROUP BY product_id, product_name
ORDER BY avg_rating DESC;


-- clickhouse

SELECT product_id, product_name, product_rating AS avg_rating, product_reviews AS total_reviews
FROM dwh3.dim_product
ORDER BY avg_rating DESC;