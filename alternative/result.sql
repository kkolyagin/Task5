with p_count_by_day as (
	select date(tpep_pickup_datetime) as tpep_date_a, count(passenger_count) as passenger_count_a, max(Total_amount) as max_ta
		from yellow_tripdata yt	
		group by tpep_date_a order by tpep_date_a),
	p0_count_by_day as (
	select date(tpep_pickup_datetime) as tpep_date0, count(passenger_count) as passenger_count0, max(Total_amount) as max_ta0  
		from yellow_tripdata yt
		where passenger_count =0
		group by tpep_date0 order by tpep_date0),		
	p1_count_by_day as (
	select date(tpep_pickup_datetime) as tpep_date1, count(passenger_count) as passenger_count1, max(Total_amount) as max_ta1  
		from yellow_tripdata yt
		where passenger_count =1
		group by tpep_date1 order by tpep_date1),		
	p2_count_by_day as (
	select date(tpep_pickup_datetime) as tpep_date2, count(passenger_count) as passenger_count2, max(Total_amount) as max_ta2  
		from yellow_tripdata yt
		where passenger_count =2
		group by tpep_date2 order by tpep_date2),		
	p3_count_by_day as (
	select date(tpep_pickup_datetime) as tpep_date3, count(passenger_count) as passenger_count3, max(Total_amount) as max_ta3  
		from yellow_tripdata yt
		where passenger_count =3
		group by tpep_date3 order by tpep_date3),		
	p4_plus_count_by_day as (
	select date(tpep_pickup_datetime) as tpep_date4, count(passenger_count) as passenger_count4, max(Total_amount) as max_ta4  
		from yellow_tripdata yt
		where passenger_count >=4
		group by tpep_date4 order by tpep_date4)
select pcd.tpep_date_a,
		(100 * p0cd.passenger_count0 / cast(passenger_count_a as decimal(9,2))) as percentage_zero,
		(100 * p1cd.passenger_count1 / cast(passenger_count_a  as decimal(9,2))) as percentage_p1,
		(100 * p2cd.passenger_count2 / cast(passenger_count_a as decimal(9,2))) as percentage_p2,
		(100 * p3cd.passenger_count3 / cast(passenger_count_a as decimal(9,2))) as percentage_p3,
		(100 * p4cd.passenger_count4 / cast(passenger_count_a as decimal(9,2))) as percentage_p4_plus,
		p0cd.max_ta0, p1cd.max_ta1, p2cd.max_ta2, p3cd.max_ta3, p4cd.max_ta4
	from p_count_by_day pcd
	left join p0_count_by_day p0cd on pcd.tpep_date_a = p0cd.tpep_date0
	left join p1_count_by_day p1cd on pcd.tpep_date_a = p1cd.tpep_date1
	left join p2_count_by_day p2cd on pcd.tpep_date_a = p2cd.tpep_date2	
	left join p3_count_by_day p3cd on pcd.tpep_date_a = p3cd.tpep_date3		
	left join p4_plus_count_by_day p4cd on pcd.tpep_date_a = p4cd.tpep_date4
	order by pcd.tpep_date_a