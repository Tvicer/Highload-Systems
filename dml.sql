insert into public.dim_customer(
	customer_first_name,
	customer_last_name,
	customer_age,
	customer_email,
	customer_country,
	customer_postal_code,
	customer_pet_type,
	customer_pet_name,
	customer_pet_breed
)
select 
	customer_first_name,
	customer_last_name,
	customer_age,
	customer_email,
	customer_country,
    customer_postal_code,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed
from public.mock_data;


insert into public.dim_seller(
	seller_first_name,
	seller_last_name,
	seller_email,
    seller_country,
    seller_postal_code
)
select 
	seller_first_name,
	seller_last_name,
	seller_email,
    seller_country,
    seller_postal_code
from public.mock_data;


insert into public.dim_product(
    product_name, 
    product_category,
    product_price,
    product_quantity,
    pet_category,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date
)
select 
    product_name, 
    product_category,
    product_price,
    product_quantity,
    pet_category,
    product_weight,
    product_color,
    product_size,
    product_brand,
    product_material,
    product_description,
    product_rating,
    product_reviews,
    product_release_date,
    product_expiry_date
from public.mock_data;


insert into public.dim_store(
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
)
select 
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
from public.mock_data;


insert into public.dim_supplier(
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
)
select 
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
from public.mock_data
where supplier_email is not null;


insert into public.fact_sales(
    customer_id,
    seller_id,
    product_id,
    store_id,
    supplier_id,
    sale_date,
    sale_quantity,
    sale_total_price,
    source_id
)
select 
    public.dim_customer.customer_id,
    public.dim_seller.seller_id,
    public.dim_product.product_id,
    public.dim_store.store_id,
    public.dim_supplier.supplier_id,
    public.mock_data.sale_date,
    public.mock_data.sale_quantity,
    public.mock_data.sale_total_price,
    public.mock_data.id as source_id
from public.mock_data
left join public.dim_customer on public.mock_data.customer_email = public.dim_customer.customer_email
left join public.dim_seller on public.mock_data.seller_email = public.dim_seller.seller_email
left join public.dim_product on public.mock_data.product_name = public.dim_product.product_name 
    and public.mock_data.product_category = public.dim_product.product_category 
    and public.mock_data.product_release_date = public.dim_product.product_release_date
    and public.mock_data.product_brand = public.dim_product.product_brand
    and public.mock_data.product_color = public.dim_product.product_color
    and public.mock_data.product_size = public.dim_product.product_size
left join public.dim_store ON public.mock_data.store_email = public.dim_store.store_email 
    and public.mock_data.store_phone = public.dim_store.store_phone
left join public.dim_supplier ON public.mock_data.supplier_email = public.dim_supplier.supplier_email;
