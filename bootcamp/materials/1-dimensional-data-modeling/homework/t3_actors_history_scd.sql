-- TASK 3 DDL for actors_history_scd table

--The following create a performance enum type to represents an actor's performance quality
create type performance as ENUM('star', 'good', 'average', 'bad');

--The following is the DDL of actors_history_scd table to track quality_class and is_Active for each actor in the actors table
create table actors_history_scd(
	actor_id text,
	actor_name text,
	quality_class performance,
	is_active boolean,
	start_year integer,
	end_year integer,
	current_year integer,
	primary key(actor_id, start_year, current_year)
);
