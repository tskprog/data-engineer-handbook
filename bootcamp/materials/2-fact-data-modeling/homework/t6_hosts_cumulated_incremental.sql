-- Task 6 The incremental query to generate host_activity_datelist
-- Implemented query that combines the previous date's SCD data with new incoming data from the events table.

/*
1. Three datasets are fetched from the host_activity_datelist table:
   - historic_data: Contains data prior to the previous date.
   - previous_data: Contains only the last date's data.
   - previous_hosts: Similar to previous_data including date column to compare with the current_data

2. One dataset is fetched from the events table:
   - current_hosts: host and activity_date grouping to get unique results
   - current_data: Represents this date's data.

3. Unchanged data is identified by:
   - Joining previous_data and current_data and comparing their properties.
   - The resulting dataset represents historic data prior to the last date.

4. Changed data is identified by:
   - Performing the opposite comparison of the unchanged data step.
   - The resulting dataset includes today's data.

5. New data is identified by:
   - Left joining current_data with previous_data and filtering where previous_id is NULL.
   - This represents newly inserted data.

6. By combining these five datasets (historic_data, previous_data, changeless_data, changed_data, and new_data), an incremental SCD Type 2 process is achieved.

Query will perform incremental for 2023-01-02 date
*/


insert into hosts_cumulated(
	with historic_data as(
		select host, host_activity_datelist from hosts_cumulated
		where curr_date < date('2023-01-01')
	),
	previous_hosts as(
		select * from hosts_cumulated
		where curr_date = date('2023-01-01')
	),
	previous_data as(
		select host, host_activity_datelist from hosts_cumulated
		where curr_date = date('2023-01-01')
	),
	current_hosts as(
		select 
			host,
			date(event_time) as activity_date
		from events e 
		where date(event_time) = date('2023-01-02')
		group by host, date(event_time)
	),
	current_data as(
		select
			coalesce(ch.host, ph.host) as host,
			case 
				when ph.host_activity_datelist is null
					then array[ch.activity_date]
				when ch.activity_date is null
					then ph.host_activity_datelist 
				else array[ch.activity_date] || ph.host_activity_datelist
			end as host_activity_datelist,	
			coalesce(ch.activity_date, ph.curr_date + interval '1 day') as curr_date
		from current_hosts ch
		full outer join previous_hosts ph
			on ch.host = ph.host
	),
	changeless_data as(
		select cd.host, cd.host_activity_datelist 
		from current_data cd
			join previous_hosts ph
			on cd.host = ph.host
			where cd.host_activity_datelist = ph.host_activity_datelist
	),
	changed_data as(
		select cd.host, cd.host_activity_datelist
		from current_data cd
			join previous_hosts ph
			on cd.host = ph.host
			where cd.host_activity_datelist <> ph.host_activity_datelist
	),
	new_data as(
		select cd.host, cd.host_activity_datelist
		from current_data cd
		left join previous_hosts ph
			on cd.host = ph.host
			where ph.host is null	
	)
	
	select *, date('2023-01-02') as curr_date from historic_data
	union all
	select *, date('2023-01-02') as curr_date from previous_data
	union all
	select *, date('2023-01-02') as curr_date from changeless_data
	union all
	select *, date('2023-01-02') as curr_date from changed_data
	union all
	select *, date('2023-01-02') as curr_date from new_data
);
