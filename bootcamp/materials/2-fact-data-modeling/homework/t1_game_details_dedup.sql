-- Task 1: A query to deduplicate game_details so there's no duplicates

/* Summary:
 * Identified duplicate records — 289 entries were found duplicated(each twice). These 289 records, along with the row_num column, were stored in a temporary table.
 * Removed the row_num column from the temporary table as it is no longer needed.
 * Deleted rows associated with duplicate IDs from the original table, resulting in the removal of 578 rows.
 * Reinserted the cleaned data from the temporary table back into the original table, ensuring no duplicate records remain.
 */

 --The following will create a table with all the duplicated data
create table dup_game_details as (
	with row_nums as(
		select *, row_number() over(partition by game_id, team_id, player_id) as row_num from game_details gd
	)
	select distinct * from row_nums where row_num > 1 --distinct is used to avoid if there are more than 2 duplicates
);

--The following query drops the row_num column
alter table dup_game_details drop column row_num;

--The following block removes the duplicated data from the table based on the ids in the temporary table
with dup_ids as (
	select game_id, team_id, player_id from dup_game_details
)
delete from game_details gd
where exists (
	select 1 from dup_ids di
		where gd.game_id = di.game_id
		and gd.team_id  = di.team_id
		and gd.player_id = di.player_id
);

--Inserting data from temp table into the original table so that all the data will be there without any duplicates
insert into game_details(
	select * from dup_game_details
);

-- optional step: dropping temporary table
drop table dup_game_details;
