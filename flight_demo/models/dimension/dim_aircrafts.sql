
with aircrafts as(
		select aircraft_code ,
			model ->> 'en' as model_english,
			model ->> 'ru' as model_russian,	
			"range" 
		,'{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
	from {{source ('stg','aircrafts_data')}}),
seats as (
			select *
			from  {{source ('stg','seats')}})
	select air.*, se.seat_no, se.fare_conditions ,
	(case when range >5600 then 'high' else 'low' end)as range_compared_5600
	from aircrafts air 
	left join seats se on air.aircraft_code = se.aircraft_code 