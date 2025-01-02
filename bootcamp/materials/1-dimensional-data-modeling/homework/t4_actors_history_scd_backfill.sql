-- TASK 4 Backfill query for actors_history_scd, query that can populate the entire actors_history_scd table at once

/* Summary
 * lead and lag functions are used to detect the change and populate is_changed column for indicating the change.
 * Streak has been calculated as sum of the is_changed that can help in knowing the changes over years
 * The final values are resulted by grouping over actor and changed_streak so that the same data across years will be in single record. 
 */

insert into actors_history_scd(
with historic_data as(
	select 
		actor_name, actor_id, quality_class, is_active, current_year,
		case
			when quality_class <> lag(quality_class, 1) over(partition by actor_id order by current_year) then 1
			when is_active <> lag(is_active, 1) over(partition by actor_id order by current_year) then 1
			else 0
		end as is_changed --column to indicate the modification
	from actors a 
	where current_year <= (select date_part('year', current_date)) --also useful to backfill till the specific year by mentioning the respective year in place of query
),
change_streak as(
	select *, sum(is_changed) over(partition by actor_id order by current_year) as changed_streak
	from historic_data
)

select 
	actor_id,
	actor_name,
	quality_class,
	is_active,
	min(current_year) as start_year,
	max(current_year) as end_year,
	(select date_part('year', current_date)) as current_year
from change_streak
group by actor_id, actor_name, changed_streak, quality_class, is_active
order by actor_id, changed_streak
);