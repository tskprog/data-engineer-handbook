-- TASK 2: A DDL for an user_devices_cumulated table that has device_activity_datelist which tracks users active days by browser_type

-- The following is the DDL of users_devices_cumulated table
create table user_devices_cumulated(
	user_id numeric,
	browser_type text,
	device_activity_datelist date[],
	curr_date date,
	primary key(user_id, browser_type, curr_date)
);
