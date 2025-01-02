--Task 5 Incremental query for actors_history_scd: query that combines the previous year's SCD data with new incoming data from the actors table.

--The following create a actor_scd_type struct helps to hold the tracking actor properties
create type actor_scd_type as(
	quality_class performance,
	is_active boolean,
	start_year integer,
	end_year integer
);

/*
1. Two datasets are fetched from the actors_history_scd table:
   - historic_data: Contains data prior to the previous year.
   - previous_data: Contains only the last year's data.

2. One dataset is fetched from the actors table:
   - current_data: Represents this year's data.

3. Unchanged data is identified by:
   - Joining previous_data and current_data and comparing their properties.
   - The resulting dataset represents historic data prior to the last year.

4. Changed data is identified by:
   - Performing the opposite comparison of the unchanged data step.
   - The resulting dataset includes both last year's and this year's data, with updated end_year values.

5. New data is identified by:
   - Left joining current_data with previous_data and filtering where previous_id is NULL.
   - This represents newly inserted data.

6. By combining these four datasets (historic_data, changeless_data, changed_data, and new_data), an incremental SCD Type 2 process is achieved.
*/

insert into actors_history_scd (

with ahs_latest_year as(
	select max(current_year) as latest_year from actors_history_scd ahs
),

historic_data as(
	select 
		actor_id, actor_name,
		quality_class, is_active,
		start_year, end_year
	from actors_history_scd
	where end_year < (select latest_year from ahs_latest_year)
	and current_year = (select latest_year from ahs_latest_year)
),
previous_data as(
	select * from actors_history_scd
	where end_year = (select latest_year from ahs_latest_year)
	and current_year = (select latest_year from ahs_latest_year)
),
current_data as(
	select * from actors
	where current_year = (select latest_year+1 from ahs_latest_year)
),

changeless_data as(
	select cd.actor_id,
		cd.actor_name, cd.quality_class, cd.is_active,
		pd.start_year,
		cd.current_year as end_year
	from current_data cd
		join previous_data pd
		on cd.actor_id = pd.actor_id
		where cd.quality_class = pd.quality_class
		and cd.is_active = pd.is_active
),
changed_records as(
	select cd.actor_id, cd.actor_name,
		unnest(array[
			row(
				pd.quality_class, pd.is_active,
				pd.start_year, pd.end_year
			)::actor_scd_type,
			row(
				cd.quality_class, cd.is_active,
				cd.current_year, cd.current_year
			)::actor_scd_type
		]) as records
	from current_data cd
		join previous_data pd
		on cd.actor_id = pd.actor_id
		where cd.quality_class <> pd.quality_class
		or cd.is_active <> pd.is_active
),
changed_data as(
	select actor_id, actor_name,
		(records::actor_scd_type).quality_class,
		(records::actor_scd_type).is_active,
		(records::actor_scd_type).start_year,
		(records::actor_scd_type).end_year
	from changed_records
),
new_data as(
	select
		cd.actor_id, cd.actor_name,
		cd.quality_class, cd.is_active,
		cd.current_year as start_year,
		cd.current_year as end_year
	from current_data cd
	left join previous_data pd
	on cd.actor_id = pd.actor_id
	where pd.actor_id is null
)

select *, (select latest_year+1 from ahs_latest_year) as current_year from historic_data
union all
select *, (select latest_year+1 from ahs_latest_year) as current_year from changeless_data
union all
select *, (select latest_year+1 from ahs_latest_year) as current_year from changed_data
union all
select *, (select latest_year+1 from ahs_latest_year) as current_year from new_data
);
