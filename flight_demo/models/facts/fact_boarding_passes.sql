{{config(materialized='incremental',unique_key='flight_id') }}

with ticket_flights as (
						select *
						from {{ source('stg','ticket_flights') }}),
						
	boarding_passes as (
						select *
						from  {{ source('stg','boarding_passes') }})
select tf.ticket_no ,tf.flight_id,tf.fare_conditions,tf.amount,bp.boarding_no,bp.seat_no,
'{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
from ticket_flights as tf
join boarding_passes as bp on tf.ticket_no =bp.ticket_no and tf.flight_id = bp.flight_id 