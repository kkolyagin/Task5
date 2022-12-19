CREATE TABLE if not exists yellow_tripdata (
	vendorid varchar(10) NULL,
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

CREATE TABLE if not exists web (
	id integer,
	timestamp integer,
	type varchar(20),
	page_id integer,
	tag varchar(10),
	sign bool
);
CREATE TABLE if not exists lk (
	id integer,
	user_id integer,
	fio varchar(50),
	dob integer,
	doc integer
);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12345, 1667627426, 'click', 101, 'Sport', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12345, 1438732800, 'scroll', 101, 'sport', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12345, 1380412800, 'move', 102, 'medicine', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12348, 1211760000, 'visit', 103, 'hitech', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12348, 1522713600, 'scroll', 104, 'medicine', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12346, 1423612800, 'click', 105, 'medicine', False);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12348, 1337644800, 'scroll', 103, 'hitech', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12346, 1026000000, 'click', 102, 'medicine', False);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12346, 1332460800, 'move', 101, 'sport', False);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12345, 1276473600, 'visit', 103, 'hitech', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12347, 1289347200, 'click', 101, 'medicine', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12345, 985910400, 'move', 104, 'sport', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12349, 1542153600, 'scroll', 112, 'medicine', False);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12349, 1557100800, 'click', 103, 'hitech', False);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12345, 1547424000, 'click', 107, 'sport', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12348, 1489968000, 'click', 103, 'politics', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12348, 1366502400, 'move', 104, 'sport', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12345, 1297036800, 'scroll', 107, 'hitech', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12348, 1137715200, 'move', 108, 'medicine', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12348, 1495324800, 'click', 109, 'hitech', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12349, 1359936000, 'click', 101, 'politics', False);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12347, 1125532800, 'move', 101, 'medicine', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12345, 1015372800, 'scroll', 102, 'sport', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12346, 1371772800, 'click', 103, 'politics', False);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12350, 1105660800, 'scroll', 104, 'politics', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12346, 1456617600, 'scroll', 105, 'hitech', False);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12346, 1268784000, 'click', 106, 'medicine', False);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12347, 1371600000, 'visit', 107, 'medicine', True);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12349, 1033171200, 'move', 108, 'hitech', False);
insert into web  (id, timestamp, type, page_id,	tag, sign) values (12349, 1171670400, 'scroll', 109, 'hitech', False);

insert into  lk (id, user_id, fio, dob,	doc) values (1, 12345,'Алексеев Борис Владимирович', 578707200, 1438732800);
insert into  lk (id, user_id, fio, dob,	doc) values (2, 12346,'Борисов Владимир Геннадьевич', 346723200, 1631732800);
insert into  lk (id, user_id, fio, dob,	doc) values (3, 12347,'Воробьев Геннадий Дмитриевич', 462240000, 1334732800);
insert into  lk (id, user_id, fio, dob,	doc) values (4, 12348,'Евстифеева Галина Андреевна', 496281600, 1238324800);
insert into  lk (id, user_id, fio, dob,	doc) values (5, 12349,'Хабибулин Марат Адамович', 568944000, 1428732800);
insert into  lk (id, user_id, fio, dob,	doc) values (6, 12350,'Зайцева Анна Михайловна', 486691200, 1528632800);