--Task 8 An incremental query that loads host_activity_reduced day by day

/*
* Retrieved the maximum date and stored it as end_date.
* Initialized start_date to the first day of the month and iterated day by day using a WHILE loop to process data incrementally.
* Fetched previous data from the host_activity_reduced table, filtering using the month_start column.
* Retrieved current data from the events table for each loop date, calculating hit_array and unique visitors using respective counts.
* Performed a full join to merge the previous and current data, forming the latest dataset.
* Achieved an upsert operation using ON CONFLICT to update conflicting records and insert new ones.
*/

do $$
declare
	end_date date;
	present_date date := date('2023-01-01');
begin
	select max(date(event_time)) from events into end_date; --to get the max date helpful to update data till this data
	while present_date <= end_date loop
		insert into host_activity_reduced(	
			with previous_data as(
				select * from host_activity_reduced
				where month_start = date(date_trunc('month', present_date))
			),
			current_data as(
				select 
					host,
					date(event_time) event_date,
					count(1) as hit_array, --number of hits to the host
					count(distinct user_id) as unique_visitors --number of unique users who hit the host
				from events e 
				where date(event_time) = present_date --filtering today's date and invalid users
					and user_id is not null 
				group by host, date(event_time)
			)
			select 
				coalesce(cd.host, pd.host) as host,
				coalesce(pd.month_start, date_trunc('month',cd.event_date)) as month_start,
				case 
					when pd.hit_array is not null
						then pd.hit_array || array[coalesce(cd.hit_array,0)]
					else 
						array_fill(0, array[coalesce(event_date - date(date_trunc('month', event_date)) ,0)]) 
						|| array[coalesce(cd.hit_array,0)]
				end as hit_array,
				case
					when pd.unique_visitors is not null
						then pd.unique_visitors || array[coalesce(cd.unique_visitors, 0)]
					else
						array_fill(0, array[coalesce(event_date - date(date_trunc('month', event_date)) , 0)])
						|| array[coalesce(cd.unique_visitors, 0)]
				end as unique_visitors	
			from current_data cd
				full outer join previous_data pd
					on cd.host = pd.host
			) --latest dataset formed
		ON CONFLICT(host, month_start) --merge strategy declaration i.e., updating conflicting records
		DO UPDATE 
			set hit_array = excluded.hit_array, unique_visitors = excluded.unique_visitors; --updating only hit metrics as host is same
		present_date := present_date + 1; --incrementing date
	end loop;
end $$;
