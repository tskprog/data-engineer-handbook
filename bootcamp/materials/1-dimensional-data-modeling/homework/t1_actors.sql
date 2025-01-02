-- TASK 1 DDL for actors table

--The following create a film_stats struct type helps to hold the film properties
create type film_stats as (
	film_year integer,
	film text,
	votes integer,
	rating float,
	film_id text
);

--The following create a performance enum type to represents an actor's performance quality
create type performance as ENUM('star', 'good', 'average', 'bad');

--The following is the DDL of actors table where we also used above two types
create table actors_tmp(
	actor_name text,
	actor_id text,
	films film_stats[],
	quality_class performance,
	is_active boolean,
	current_year integer,
	primary key(actor_id, current_year)
);
