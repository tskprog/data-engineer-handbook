--TASK 4 A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column

/*
 * Summary: 
 * Generated series of dates as we have 1 month i.e, January
 * Fetched the previous data with filtering the last date
 * Performed cross join to determine the date_list_int column with the help of pow function
 */

create table user_devices_cumulated_datelist as --optional step to create table
with user_devices as(
	select * from user_devices_cumulated udc 
	where curr_date = date('2023-01-31') --getting all the cumulated data of the latest day
),
date_series as(
	select generate_series(date('2023-01-01'),date('2023-01-31'), interval '1 day') as series_date 
),
date_list_vals as(
	select 
		*,
		case 
			when array[date(series_date)] <@ device_activity_datelist --checking existence of the generated date in the activity dates array
				then pow(2, 32 - (curr_date - date(series_date))) --subtracted from 32 to ensure recent activity sticks to the left side of bits
			else 0
		end as date_list_val
	from user_devices ud
	cross join date_series ds
)

select 
	user_id, 
	browser_type,
	device_activity_datelist,
	sum(date_list_val) as date_list_values,
	cast(cast(sum(date_list_val) as bigint) as bit(32)) as date_list_int
from date_list_vals
group by user_id, browser_type, device_activity_datelist
