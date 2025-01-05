-- TASK 5: A DDL for hosts_cumulated table having host_activity_datelist which logs to see which dates each host is experiencing any activity

-- The following is the DDL of hosts_cumulated  table
create table hosts_cumulated(
	host text,
	host_activity_datelist date[],
	curr_date date,
	primary key(host, host_activity_datelist, curr_date)
);
