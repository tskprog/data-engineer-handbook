/*
TASK 7: A monthly, reduced fact table DDL host_activity_reduced having
	month_start
	host
	hit_array - think COUNT(1)
	unique_visitors array - think COUNT(DISTINCT user_id)
*/

-- The following is the DDL of host_activity_reduced table
create table host_activity_reduced(
	 host text,
	 month_start date,
	 hit_array int[],
	 unique_visitors int[],
	 primary key(host, month_start)
);
