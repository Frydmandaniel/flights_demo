{{config(materialized='incremental',unique_key='ticket_no') }}
with tickets as(
				select * 
				from  {{ source('stg','tickets') }}) ,
bookings as (
			select *
			from  {{ source('stg','bookings') }})
select ticket_no,book.book_ref,passenger_id,passenger_name,book.book_date,book.total_amount ,contact_data ->> 'phone'as phone,
 contact_data  ->> 'email' as email,
'{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
from tickets tic 
join bookings book on tic.book_ref = book.book_ref 