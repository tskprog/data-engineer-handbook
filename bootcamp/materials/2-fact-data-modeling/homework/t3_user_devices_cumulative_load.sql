--TASK 3 Cumulative table generation query which populates the user_devices_cumulated  from events table.

/*
 * Summary: 
 * min and max dates will be stored as start and end dates. Looping over from start date to end date, data will be populated into user_devices_cumulated table.
 * device_activity_datelist is formed using case statement for checking existence of previous data and concatenated today's date accordingly
 */

do $$
declare
	start_date date; -- var for minimum date in events table
	end_date date; --var for max date in events table
	present_date date; --var to get present date data while performing loop
begin
--	Calculate start and end dates to loop over them
	select date(min(event_time)), date(max(event_time)) --2023-01-01, 2023-01-02 testing dates
	into start_date, end_date from events;
	present_date := start_date; --initializing present_date to start_date
--	looping over all the dates
	while present_date <= end_date loop
--		insert query for user_devices_cumulated table for the selected date
		insert into user_devices_cumulated(
			with previous_data as(
				select * from user_devices_cumulated
					where curr_date = present_date - 1 --fetching previous date's data
			),
			current_data as(
				select 
					user_id,
					browser_type,
					date(event_time) as activity_date
				from devices d
				join (select * from events
						where date(event_time) = present_date --fetching current date's data
						and user_id is not null  --filtering invalid users
						and device_id is not null) e --filtering before join helps in memory utilization
				on d.device_id = e.device_id
				group by user_id, browser_type, date(event_time)
			)
			select 
				coalesce(cd.user_id, pd.user_id) as user_id,
				coalesce(cd.browser_type, pd.browser_type) as browser_type,
				case 
					when pd.device_activity_datelist is null
						then array[cd.activity_date]
					when cd.activity_date is null
						then pd.device_activity_datelist
					else array[cd.activity_date] || pd.device_activity_datelist
				end as device_activity_datelist,
				coalesce(cd.activity_date, pd.curr_date + interval '1 day') as curr_date
			from current_data cd
			full outer join previous_data pd
			on cd.user_id = pd.user_id 
				and cd.browser_type = pd.browser_type 
		);
		present_date := present_date + INTERVAL '1 day'; --incrementing date
	end loop;
end $$
