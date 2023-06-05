
with airports as (
				select  airport_code ,
						airport_name ->> 'en' as airport_english,
						airport_name ->> 'ru'as airport_russian,
						city ->> 'en'as city_english,
						city  ->> 'ru' as city_russian,
						timezone,
						'{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
				from {{ source('stg', 'airports_data') }})
select airport_code,
		airport_english,airport_russian,city_english,city_russian,dbt_time
from airports