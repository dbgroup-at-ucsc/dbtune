select afirstname, alastname, year(dateofb)
from movies.actors
where year(dateofb) >= 40;

select movieid, avg(rate), count(rate)
from movies.ratings
group by movieid;

select movieid
from movies.genres
where mgenre like '%fiction%';

select userid, creditnum, expdate
from movies.creditcards
where expdate < '2013-12-31';

select title, yearofr
from movies.movies
where summary like '%dystopian%' and summary like '%revolution%';
