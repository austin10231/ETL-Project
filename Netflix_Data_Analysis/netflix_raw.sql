DROP TABLE IF EXISTS netflix_raw;

CREATE TABLE netflix_raw (
    show_id VARCHAR(10) PRIMARY KEY,
    type VARCHAR(10),
    title TEXT,
    director VARCHAR(250),
    cast_members VARCHAR(1000),
    country VARCHAR(150),
    date_added VARCHAR(20),
    release_year INTEGER,
    rating VARCHAR(10),
    duration VARCHAR(10),
    listed_in VARCHAR(100),
    description VARCHAR(500)
);

UPDATE netflix_raw
SET cast_members = cast;

ALTER TABLE netflix_raw
DROP COLUMN cast;

ALTER TABLE netflix_raw
RENAME COLUMN cast_members TO cast;


select * from netflix_raw;

select * from netflix_raw
where concat(upper(title), type) in (
select concat(upper(title), type)
from netflix_raw
group by upper(title), type
having count(*) > 1
)
order by upper(title)


with cte as(
select *
, row_number() over (PARTITION by title, type order by show_id) as rn
from netflix_raw
)
select * from cte
where rn=1;


SELECT 
    show_id,
    unnest(string_to_array(director, ',')) AS director
FROM netflix_raw;

CREATE TABLE netflix_directors AS
SELECT 
    show_id,
    TRIM(unnest(string_to_array(director, ','))) AS director
FROM netflix_raw;

SELECT 
    show_id,
    TRIM(unnest(string_to_array(country, ','))) AS country
INTO netflix_country
FROM netflix_raw;

SELECT 
    show_id,
    TRIM(unnest(string_to_array("cast", ','))) AS cast
INTO netflix_cast
FROM netflix_raw;

SELECT 
    show_id,
    TRIM(unnest(string_to_array(listed_in, ','))) AS genre
INTO netflix_genre
FROM netflix_raw;

select show_id, type, title, cast(date_added as date) as date_added, release_year, rating, duration, description
from cte
where rn=1;

insert into netflix_country
select show_id, m.country
from netflix_raw nr
inner join(
select director, country
from netflix_country nc
inner join netflix_directors nd on nc.show_id = nd.show_id
group by director, country
order by director
) m on nr.director = m.director
where nr.country is null;

select director, country
from netflix_country nc
inner join netflix_directors nd on nc.show_id = nd.show_id
group by director, country
order by director;

select * from netflix_raw where duration is null;


with cte as(
select *
, row_number() over (PARTITION by title, type order by show_id) as rn
from netflix_raw
)
select show_id, type, title, cast(date_added as date) as date_added, release_year, rating
, case when duration is null then rating else duration end as duration, description
into netflix
from cte

select * from netflix_country;


select * from netflix_directors;

select nd.director,
count(distinct case when n.type='Movie' then n.show_id end) as no_of_movies,
count(distinct case when n.type='TV Show' then n.show_id end) as no_of_tvshow
from netflix n
inner join netflix_directors nd
	on n.show_id = nd.show_id
group by director
having count(distinct n.type) > 1;

select max(nc.country), count(distinct nc.show_id) as number_of_comedy
from netflix_country nc
inner join netflix_genre ng 
	on nc.show_id = ng.show_id
inner join netflix n
 	on n.show_id = nc.show_id
where ng.genre ilike '%comedies%' and n.type = 'Movie'
group by nc.country
order by number_of_comedy desc;

with cte as (
select 
	extract(year from n.date_added) as date_year, 
	nd.director, 
	count(n.show_id) as no_of_movies
from netflix n
inner join netflix_directors nd
	on n.show_id = nd.show_id
where n.type = 'Movie'
group by extract(year from n.date_added), nd.director 
), cte2 as (
select *, row_number() over(partition by date_year order by no_of_movies desc) as rnk
from cte
)
select * from cte2 where rnk=1;

with cte as (
select 
	extract(year from n.date_added) as date_year, 
	nd.director, 
	count(n.show_id) as no_of_movies,
	row_number() over(partition by extract(year from n.date_added) order by count(n.show_id) desc) as rnk
from netflix n
inner join netflix_directors nd
	on n.show_id = nd.show_id
where n.type = 'Movie'
group by extract(year from n.date_added), nd.director 
)
select * from cte where rnk=1;

select 
	ng.genre, 
	round(avg(cast(replace(duration, 'min', '') as int)),2) as avg_duration
	--avg(n.duration) as avg_duration 
from netflix n
inner join netflix_genre ng
	on n.show_id = ng.show_id
where n.type = 'Movie'
group by ng.genre;

select 
	nd.director,
	count(distinct case when ng.genre = 'Comedies' then n.show_id end) as no_of_comedy,
	count(distinct case when ng.genre = 'Horror Movies' then n.show_id end) as no_of_horror
from netflix n
inner join netflix_genre ng
	on n.show_id = ng.show_id
inner join netflix_directors nd
	on n.show_id = nd.show_id
where n.type = 'Movie' and ng.genre in ('Comedies', 'Horror Movies')
group by nd.director
having count(distinct ng.genre)=2;
















