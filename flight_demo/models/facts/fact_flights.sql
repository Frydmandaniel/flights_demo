{{config(materialized='incremental',unique_key='flight_id') }}

with flights as (
				select *,
				extract(EPOCH from scheduled_departure ::timestamp) as scheduled_departure_epoch,
				extract(EPOCH from scheduled_arrival ::timestamp) as scheduled_arrival_epoch,
				extract(EPOCH from scheduled_arrival ::timestamp) - extract(EPOCH from scheduled_departure ::timestamp) as flight_duration_expected,
				extract(EPOCH from actual_departure ::timestamp) as actual_departure_epoch,
				extract(EPOCH from actual_arrival ::timestamp) as actual_arrival_epoch,
				extract(EPOCH from actual_departure ::timestamp) - extract(EPOCH from actual_departure ::timestamp) as flight_duration
				from {{ source('stg','flights')}})
,dim_aircrafts as (
				select *from {{ ref('dim_aircrafts') }})
,dim_airports as (
				select *from {{ ref('dim_airports')}})
select distinct fl.flight_id,fl.flight_no,fl.aircraft_code ,fl.departure_airport ,fl.arrival_airport ,fl.status,flight_duration ,flight_duration_expected ,
(case when flight_duration_expected > flight_duration then 'longer'
 	  when flight_duration_expected < flight_duration then 'shorter' else 'expected' end) as compared_expected_actual,
	  		(case when dim_aircrafts.aircraft_code is not null then dim_aircrafts.aircraft_code else '-1' end) as aircraft_code_check,
			(case when da.airport_code is not null then da.airport_code else '-1' end) as airport_code_check,
			(case when dai.airport_code is not null then dai.airport_code else '-1' end) as second_airport_code_check,
       '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
from flights fl 
join dim_aircrafts on fl.aircraft_code = dim_aircrafts.aircraft_code
join dim_airports da on fl.arrival_airport = da.airport_code
join dim_airports dai on fl.departure_airport  = dai.airport_code