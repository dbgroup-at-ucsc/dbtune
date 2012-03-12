--NOTE: the following procedure has to be defined in the DB2
--      instance in order to execute the functional tests correctly

--CREATE PROCEDURE db2perf_quiet_drop( IN statement VARCHAR(1000) )
--LANGUAGE SQL
--BEGIN
   --DECLARE SQLSTATE CHAR(5);
   --DECLARE NotThere    CONDITION FOR SQLSTATE '42704';
   --DECLARE NotThereSig CONDITION FOR SQLSTATE '42883';

   --DECLARE EXIT HANDLER FOR NotThere, NotThereSig
      --SET SQLSTATE = '     ';

   --SET statement = 'DROP ' || statement;
   --EXECUTE IMMEDIATE statement;
--END

CALL db2perf_quiet_drop('TABLE movies.ratings');
CALL db2perf_quiet_drop('TABLE movies.queue');
CALL db2perf_quiet_drop('TABLE movies.casts');
CALL db2perf_quiet_drop('TABLE movies.actors');
CALL db2perf_quiet_drop('TABLE movies.genres');
CALL db2perf_quiet_drop('TABLE movies.movies');
CALL db2perf_quiet_drop('TABLE movies.creditcards');
CALL db2perf_quiet_drop('TABLE movies.users');
CALL db2perf_quiet_drop('SCHEMA movies RESTRICT');

CREATE SCHEMA movies;

CREATE TABLE movies.users (
    userid      INTEGER NOT NULL,
    email       VARCHAR(30),
    password    VARCHAR(20),
    ufirstname  VARCHAR(20),
    ulastname   VARCHAR(20),
    PRIMARY KEY (userid)
);

CREATE TABLE movies.creditcards (
    userid      INTEGER NOT NULL,
    creditnum   BIGINT,
    credittype  VARCHAR(16),
    expdate     DATE,
    PRIMARY KEY (userid),
    FOREIGN KEY (userid) REFERENCES movies.users(userid) ON DELETE CASCADE
);

CREATE TABLE movies.movies (
    movieid     INTEGER NOT NULL, 
    title       VARCHAR(50),
    yearofr     DATE,
    summary     VARCHAR(1024),
    url         VARCHAR(50) DEFAULT 'unknown',
    PRIMARY KEY (movieid)
);

CREATE TABLE movies.genres (
    mgenre      VARCHAR(20) NOT NULL,
    movieid     INTEGER NOT NULL,
    PRIMARY KEY (mgenre, movieid),
    FOREIGN KEY (movieid) REFERENCES movies.movies(movieid) ON DELETE CASCADE
);

CREATE TABLE movies.actors (
    aid         INTEGER NOT NULL,
    afirstname  VARCHAR(20),
    alastname   VARCHAR(20),
    dateofb     DATE,
    PRIMARY KEY (aid)
);

CREATE TABLE movies.casts (
    aid         INTEGER NOT NULL,
    movieid     INTEGER NOT NULL,
    PRIMARY KEY (aid, movieid),
    FOREIGN KEY (aid) REFERENCES movies.actors(aid) ON DELETE CASCADE,
    FOREIGN KEY (movieid) REFERENCES movies.movies(movieid) ON DELETE CASCADE
);

CREATE TABLE movies.queue (
    userid      INTEGER NOT NULL,
    movieid     INTEGER NOT NULL,
    position    INTEGER NOT NULL,
    times       timestamp NOT NULL,
    PRIMARY KEY (times, movieid, userid),
    FOREIGN KEY (movieid) REFERENCES movies.movies(movieid) ON DELETE CASCADE,
    FOREIGN KEY (userid) REFERENCES movies.users(userid) ON DELETE CASCADE
);

CREATE TABLE movies.ratings (
    userid      INTEGER NOT NULL,
    movieid     INTEGER,
    rate        INTEGER,
    review      VARCHAR(256) NOT NULL,
    FOREIGN KEY (userid) REFERENCES movies.users(userid) ON DELETE CASCADE,
    FOREIGN KEY (movieid) REFERENCES movies.movies(movieid) ON DELETE CASCADE
);

INSERT INTO movies.users(userid, email, password, ufirstname, ulastname) VALUES (1,'john.doe@unknown.com', '123456','john','doe');
INSERT INTO movies.users(userid, email, password, ufirstname, ulastname) VALUES (2, 'joe.bloggs@unknown.com', '234567', 'joe', 'bloggs');
INSERT INTO movies.users(userid, email, password, ufirstname, ulastname) VALUES (3, 'jane.doe@unknown.com', '345678', 'jane', 'doe');
INSERT INTO movies.users(userid, email, password, ufirstname, ulastname) VALUES (4, 'mary.mayor@unknown.com', '456789', 'mary', 'mayor');
INSERT INTO movies.users(userid, email, password, ufirstname, ulastname) VALUES (5, 'richard.miles@unknown.com', '567890', 'richard', 'miles');
INSERT INTO movies.users(userid, email, password, ufirstname, ulastname) VALUES (6, 'lucy.roe@unknown.com', '678901', 'lucy', 'roe');

INSERT INTO movies.creditcards(userid, creditnum, credittype, expdate) VALUES (1, 6781234567812345, 'mastercard', '2014-10-31');
INSERT INTO movies.creditcards(userid, creditnum, credittype, expdate) VALUES (2, 5678123456781234, 'visa', '2013-08-31');
INSERT INTO movies.creditcards(userid, creditnum, credittype, expdate) VALUES (3, 4567812345678123, 'visa', '2014-06-30');
INSERT INTO movies.creditcards(userid, creditnum, credittype, expdate) VALUES (4, 3456781234567812, 'american express', '2015-01-31');
INSERT INTO movies.creditcards(userid, creditnum, credittype, expdate) VALUES (5, 1234567812345678, 'visa', '2013-01-31');
INSERT INTO movies.creditcards(userid, creditnum, credittype, expdate) VALUES (6, 2345678123456781, 'mastercard', '2014-01-31');

INSERT INTO movies.movies(movieid, title, yearofr, summary) VALUES (1, 'the matrix', '1999-01-01', 'the film depicts a future in which reality as perceived by most humans is actually a simulated reality created by sentient machines to pacify and subdue the human population, while their bodies'' heat and electrical activity are used as an energy source. upon learning this, computer programmer "neo" is drawn into a rebellion against the machines, involving other people who have been freed from the "dream world" and into reality. the film contains many REFERENCES to the cyberpunk and hacker subcultures; philosophical and religious ideas such as rené descartes''s evil genius, the allegory of the cave, the brain in a vat thought experiment; and homages to alice''s adventures in wonderland, hong kong action cinema, spaghetti westerns, dystopian fiction, and japanese animation.');
INSERT INTO movies.movies(movieid, title, yearofr, summary) VALUES (2, 'v for vendetta', '2006-01-01', 'v for vendetta is a 2006 dystopian thriller film directed by james mcteigue and produced by joel silver and the wachowski brothers, who also wrote the screenplay. it is an adaptation of the comic book series of the same name by alan moore and david lloyd. set in london in a near-future dystopian society, natalie portman stars as evey, a working-class girl who must determine if her hero has become the very menace she is fighting against. hugo weaving plays va bold, charismatic freedom fighter driven to exact revenge on those who disfigured him. stephen rea portrays the detective leading a desperate quest to capture v before he ignites a revolution.');
INSERT INTO movies.movies(movieid, title, yearofr, summary) VALUES (3, 'nineteen eighty-four', '1984-01-01', 'nineteen eighty-four is a 1984 british science fiction film, based upon george orwell''s novel of the same name, following the life of winston smith in oceania, a country run by a totalitarian government. the film was written and directed by michael radford and stars john hurt, richard burton (in his last film role), and suzanna hamilton');

INSERT INTO movies.genres(mgenre, movieid) VALUES ('science fiction', 1);
INSERT INTO movies.genres(mgenre, movieid) VALUES ('action', 1);
INSERT INTO movies.genres(mgenre, movieid) VALUES ('science fiction', 2);
INSERT INTO movies.genres(mgenre, movieid) VALUES ('action', 2);
INSERT INTO movies.genres(mgenre, movieid) VALUES ('science fiction', 3);

INSERT INTO movies.actors(aid, afirstname, alastname, dateofb) VALUES (1, 'keanu', 'reeves', '1964-09-02');
INSERT INTO movies.actors(aid, afirstname, alastname, dateofb) VALUES (2, 'laurence', 'fishburne', '1961-07-30');
INSERT INTO movies.actors(aid, afirstname, alastname, dateofb) VALUES (3, 'carrie-ann', 'moss', '1967-08-21');
INSERT INTO movies.actors(aid, afirstname, alastname, dateofb) VALUES (4, 'hugo', 'weaving', '1960-04-04');
INSERT INTO movies.actors(aid, afirstname, alastname, dateofb) VALUES (5, 'natalie', 'portman', '1981-06-09');
INSERT INTO movies.actors(aid, afirstname, alastname, dateofb) VALUES (6, 'john-vincent', 'hurt', '1940-01-22');
INSERT INTO movies.actors(aid, afirstname, alastname, dateofb) VALUES (7, 'richard', 'burton', '1925-11-10');
INSERT INTO movies.actors(aid, afirstname, alastname, dateofb) VALUES (8, 'suzanna', 'hamilton', '1960-01-01');

INSERT INTO movies.casts(aid, movieid) VALUES (1, 1);
INSERT INTO movies.casts(aid, movieid) VALUES (2, 1);
INSERT INTO movies.casts(aid, movieid) VALUES (3, 1);
INSERT INTO movies.casts(aid, movieid) VALUES (4, 1);
INSERT INTO movies.casts(aid, movieid) VALUES (5, 2);
INSERT INTO movies.casts(aid, movieid) VALUES (4, 2);
INSERT INTO movies.casts(aid, movieid) VALUES (6, 2);
INSERT INTO movies.casts(aid, movieid) VALUES (6, 3);
INSERT INTO movies.casts(aid, movieid) VALUES (7, 3);
INSERT INTO movies.casts(aid, movieid) VALUES (8, 3);

INSERT INTO movies.queue(userid, movieid, position, times) VALUES (4, 1, 1, '2011-01-20 12:05:00');
INSERT INTO movies.queue(userid, movieid, position, times) VALUES (3, 2, 1, '2011-01-08 11:45:00');
INSERT INTO movies.queue(userid, movieid, position, times) VALUES (3, 1, 2, '2011-01-08 13:56:00');
INSERT INTO movies.queue(userid, movieid, position, times) VALUES (1, 2, 1, '2011-01-02 20:45:00');
INSERT INTO movies.queue(userid, movieid, position, times) VALUES (5, 3, 1, '2011-01-13 14:05:00');

INSERT INTO movies.ratings(userid, movieid, rate, review) VALUES (6, 1, 5, 'excellent!');
INSERT INTO movies.ratings(userid, movieid, rate, review) VALUES (1, 1, 1, 'boring');
INSERT INTO movies.ratings(userid, movieid, rate, review) VALUES (2, 1, 3, 'so so');
INSERT INTO movies.ratings(userid, movieid, rate, review) VALUES (6, 2, 5, 'i loved this movie!');
INSERT INTO movies.ratings(userid, movieid, rate, review) VALUES (5, 2, 4, 'almost made it to my fav list');
INSERT INTO movies.ratings(userid, movieid, rate, review) VALUES (4, 2, 3, '');
INSERT INTO movies.ratings(userid, movieid, rate, review) VALUES (1, 3, 1, 'soooo boring; movie is so old!');

CREATE UNIQUE INDEX movies.users_userid_email                      ON movies.users       (userid, email);
CREATE UNIQUE INDEX movies.creditcards_creditnum_userid_credittype ON movies.creditcards (creditnum, userid, credittype);
CREATE UNIQUE INDEX movies.movies_moiveid_title_yearofr            ON movies.movies      (movieid, title, yearofr);
CREATE UNIQUE INDEX movies.actors_afirstname_alastname_dateofb     ON movies.actors      (afirstname, alastname, dateofb);
CREATE UNIQUE INDEX movies.queue_times                             ON movies.queue       (times);
