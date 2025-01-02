--TASK 2 Cumulative table generation query which populates the actors table one year at a time.

/*
 * Summary: 
 * min and max years will be stored as start and end years. Looping over from start year to end year, data will be populated into actors table.
 * All the film stats will be aggregated by grouping over actor and year followed by removal of nulls in the formed array resulting in films column.
 * Quality class is determined based on the average_rating calculated by grouping actor and year.
 */

do $$
declare
	start_year int; -- var for minimum year in actor_films table
	end_year int; --var for max year in actors_films table
	present_year int; --var to iterate over years that helps in fetching present data from source dataset
	previous_year int; --var to get previous years data
begin
--	Calculate start and end years to loop over them
	select min(year), max(year) --1970,1971 testing years
	into start_year, end_year from actor_films af;
--	looping over all the years
	for present_year in start_year..end_year loop
		previous_year := present_year - 1; --setting previous year by decrementing 1
		
--		insert query for actors table for the selected year
		insert into actors(
			with historic_data as(
				select * from actors
				where current_year = previous_year
			),
			present_data as(
				select 
					actor, actorid,
					array_remove(
						array_agg(
							row(
								year, film, votes, rating, filmid
							)::film_stats),
						null
					) as films,
					year,
					avg(rating) as avg_rating
				from actor_films
				where year = present_year --filtering present year
				group by actor, actorid, year --grouping to get data of an actor in an year
			)
			
			select 
				coalesce(pd.actor, hd.actor_name) as actor_name,
				coalesce(pd.actorid, hd.actor_id) as actor_id,
				case 
					when hd.films is null
						then pd.films
					when pd.year is not null
						then hd.films || pd.films
					else hd.films
				end as films,
				case 
					when pd.year is not null then
					case
						when pd.avg_rating > 8 then 'star'
						when pd.avg_rating > 7 then 'good'
						when pd.avg_rating > 6 then 'average'
						else 'bad'
					end::performance
					else hd.quality_class
				end as quality_class,
				case 
					when pd.year is not null then true else false
				end as is_active,
				coalesce(pd.year, hd.current_year + 1) as current_year
			from present_data pd
				full outer join historic_data hd
				on pd.actorid = hd.actor_id
				order by actor_id
			);		
		raise notice 'Processing % year', present_year;
	end loop;
end $$;
