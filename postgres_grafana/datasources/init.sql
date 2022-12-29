CREATE TABLE if not exists yellow_tripdata (
	vendorid varchar(1) NULL,
	tpep_pickup_datetime timestamp NULL,
	tpep_dropoff_datetime timestamp NULL,
	passenger_count int2 NULL,
	trip_distance float4 NULL,
	ratecodeid int2 NULL,
	store_and_fwd_flag varchar NULL,
	pulocationid int2 NULL,
	dolocationid int2 NULL,
	payment_type int2 NULL,
	fare_amount float4 NULL,
	extra float4 NULL,
	mta_tax float4 NULL,
	tip_amount float4 NULL,
	tolls_amount float4 NULL,
	improvement_surcharge float4 NULL,
	total_amount float4 NULL,
	congestion_surcharge float4 NULL
);

CREATE TABLE if not exists parquet (	
	date timestamp NULL,
	percentage_zero float8 NULL,
	percentage_1p float8 NULL,
	percentage_2p float8 NULL,
	percentage_3p float8 NULL,
	percentage_4p_plus float8 NULL,
	max_pay_zero float4 NULL,
	max_pay_1p float4 NULL,
	max_pay_2p float4 NULL,
	max_pay_3p float4 NULL,
	max_pay_4p_plus float4 NULL,
	min_pay_zero float4 NULL,
	min_pay_1p float4 NULL,
	min_pay_2p float4 NULL,
	min_pay_3p float4 NULL,
	min_pay_4p_plus float4 NULL
);