DROP TABLE IF EXISTS users       CASCADE;
DROP TABLE IF EXISTS creditcards CASCADE;
DROP TABLE IF EXISTS movies      CASCADE;
DROP TABLE IF EXISTS genres      CASCADE;
DROP TABLE IF EXISTS actors      CASCADE;
DROP TABLE IF EXISTS casts       CASCADE;
DROP TABLE IF EXISTS queue       CASCADE;
DROP TABLE IF EXISTS ratings     CASCADE;

CREATE TABLE users (
    userid      SERIAL,
    email       VARCHAR(30),
    password    VARCHAR(20),
    ufirstname  VARCHAR(20),
    ulastname   VARCHAR(20),
    UNIQUE      (userid, email),
    PRIMARY KEY (userid)
);

CREATE TABLE creditcards (
    userid      INTEGER,
    creditnum   BIGINT,
    credittype  VARCHAR(16),
    expdate     DATE,
    UNIQUE      (creditnum, userid, credittype),
    PRIMARY KEY (userid),
    FOREIGN KEY (userid) REFERENCES users
);

CREATE TABLE movies (
    movieid     SERIAL, 
    title       VARCHAR(50),
    yearofr     DATE,
    summary     VARCHAR(1024),
    url         VARCHAR(50) DEFAULT 'unknown',
    UNIQUE      (movieid, title, yearofr),
    PRIMARY KEY (movieid)
);

CREATE TABLE genres (
    mgenre      VARCHAR(20),
    movieid     INTEGER,
    PRIMARY KEY (mgenre, movieid),
    FOREIGN KEY (movieid) REFERENCES movies
);

CREATE TABLE actors (
    aid         SERIAL,
    afirstname  VARCHAR(20),
    alastname   VARCHAR(20),
    dateofb     DATE,
    UNIQUE      (afirstname, alastname, dateofb),
    PRIMARY KEY (aid)
);

CREATE TABLE casts (
    aid         INTEGER,
    movieid     INTEGER,
    PRIMARY KEY (aid, movieid),
    FOREIGN KEY (aid) REFERENCES actors,
    FOREIGN KEY (movieid) REFERENCES movies
);

CREATE TABLE queue (
    userid      INTEGER,
    movieid     INTEGER,
    position    INTEGER, 
    times       timestamp,
    UNIQUE      (times),
    PRIMARY KEY (times, movieid, userid),
    FOREIGN KEY (movieid) REFERENCES movies,
    FOREIGN KEY (userid) REFERENCES users
);

CREATE TABLE ratings (
    userid      INTEGER,
    movieid     INTEGER,
    rate        INTEGER,
    review      VARCHAR(256) NOT NULL,
    PRIMARY KEY (userid, movieid),
    FOREIGN KEY (userid) REFERENCES users, 
    FOREIGN KEY (movieid) REFERENCES movies
);

INSERT INTO users(userid, email, password, ufirstname, ulastname) VALUES (1,'john.doe@unknown.com', '123456','john','doe');
INSERT INTO users(userid, email, password, ufirstname, ulastname) VALUES (2, 'joe.bloggs@unknown.com', '234567', 'joe', 'bloggs');
INSERT INTO users(userid, email, password, ufirstname, ulastname) VALUES (3, 'jane.doe@unknown.com', '345678', 'jane', 'doe');
INSERT INTO users(userid, email, password, ufirstname, ulastname) VALUES (4, 'mary.mayor@unknown.com', '456789', 'mary', 'mayor');
INSERT INTO users(userid, email, password, ufirstname, ulastname) VALUES (5, 'richard.miles@unknown.com', '567890', 'richard', 'miles');
INSERT INTO users(userid, email, password, ufirstname, ulastname) VALUES (6, 'lucy.roe@unknown.com', '678901', 'lucy', 'roe');

INSERT INTO creditcards(userid, creditnum, credittype, expdate) VALUES (1, 6781234567812345, 'mastercard', '2014-10-31');
INSERT INTO creditcards(userid, creditnum, credittype, expdate) VALUES (2, 5678123456781234, 'visa', '2013-08-31');
INSERT INTO creditcards(userid, creditnum, credittype, expdate) VALUES (3, 4567812345678123, 'visa', '2014-06-30');
INSERT INTO creditcards(userid, creditnum, credittype, expdate) VALUES (4, 3456781234567812, 'american express', '2015-01-31');
INSERT INTO creditcards(userid, creditnum, credittype, expdate) VALUES (5, 1234567812345678, 'visa', '2013-01-31');
INSERT INTO creditcards(userid, creditnum, credittype, expdate) VALUES (6, 2345678123456781, 'mastercard', '2014-01-31');

INSERT INTO movies(movieid, title, yearofr, summary) VALUES (1, 'the matrix', '1999-01-01', 'the film depicts a future in which reality as perceived by most humans is actually a simulated reality created by sentient machines to pacify and subdue the human population, while their bodies'' heat and electrical activity are used as an energy source. upon learning this, computer programmer "neo" is drawn into a rebellion against the machines, involving other people who have been freed from the "dream world" and into reality. the film contains many REFERENCES to the cyberpunk and hacker subcultures; philosophical and religious ideas such as rené descartes''s evil genius, the allegory of the cave, the brain in a vat thought experiment; and homages to alice''s adventures in wonderland, hong kong action cinema, spaghetti westerns, dystopian fiction, and japanese animation.');
INSERT INTO movies(movieid, title, yearofr, summary) VALUES (2, 'v for vendetta', '2006-01-01', 'v for vendetta is a 2006 dystopian thriller film directed by james mcteigue and produced by joel silver and the wachowski brothers, who also wrote the screenplay. it is an adaptation of the comic book series of the same name by alan moore and david lloyd. set in london in a near-future dystopian society, natalie portman stars as evey, a working-class girl who must determine if her hero has become the very menace she is fighting against. hugo weaving plays va bold, charismatic freedom fighter driven to exact revenge on those who disfigured him. stephen rea portrays the detective leading a desperate quest to capture v before he ignites a revolution.');
INSERT INTO movies(movieid, title, yearofr, summary) VALUES (3, 'nineteen eighty-four', '1984-01-01', 'nineteen eighty-four is a 1984 british science fiction film, based upon george orwell''s novel of the same name, following the life of winston smith in oceania, a country run by a totalitarian government. the film was written and directed by michael radford and stars john hurt, richard burton (in his last film role), and suzanna hamilton');

INSERT INTO genres(mgenre, movieid) VALUES ('science fiction', 1);
INSERT INTO genres(mgenre, movieid) VALUES ('action', 1);
INSERT INTO genres(mgenre, movieid) VALUES ('science fiction', 2);
INSERT INTO genres(mgenre, movieid) VALUES ('action', 2);
INSERT INTO genres(mgenre, movieid) VALUES ('science fiction', 3);

INSERT INTO actors(aid, afirstname, alastname, dateofb) VALUES (1, 'keanu', 'reeves', '1964-09-02');
INSERT INTO actors(aid, afirstname, alastname, dateofb) VALUES (2, 'laurence', 'fishburne', '1961-07-30');
INSERT INTO actors(aid, afirstname, alastname, dateofb) VALUES (3, 'carrie-ann', 'moss', '1967-08-21');
INSERT INTO actors(aid, afirstname, alastname, dateofb) VALUES (4, 'hugo', 'weaving', '1960-04-04');
INSERT INTO actors(aid, afirstname, alastname, dateofb) VALUES (5, 'natalie', 'portman', '1981-06-09');
INSERT INTO actors(aid, afirstname, alastname, dateofb) VALUES (6, 'john-vincent', 'hurt', '1940-01-22');
INSERT INTO actors(aid, afirstname, alastname, dateofb) VALUES (7, 'richard', 'burton', '1925-11-10');
INSERT INTO actors(aid, afirstname, alastname, dateofb) VALUES (8, 'suzanna', 'hamilton', '1960-01-01');

INSERT INTO casts(aid, movieid) VALUES (1, 1);
INSERT INTO casts(aid, movieid) VALUES (2, 1);
INSERT INTO casts(aid, movieid) VALUES (3, 1);
INSERT INTO casts(aid, movieid) VALUES (4, 1);
INSERT INTO casts(aid, movieid) VALUES (5, 2);
INSERT INTO casts(aid, movieid) VALUES (4, 2);
INSERT INTO casts(aid, movieid) VALUES (6, 2);
INSERT INTO casts(aid, movieid) VALUES (6, 3);
INSERT INTO casts(aid, movieid) VALUES (7, 3);
INSERT INTO casts(aid, movieid) VALUES (8, 3);

INSERT INTO queue(userid, movieid, position, times) VALUES (4, 1, 1, '2011-01-20 12:05 pm');
INSERT INTO queue(userid, movieid, position, times) VALUES (3, 2, 1, '2011-01-08 1:45 am');
INSERT INTO queue(userid, movieid, position, times) VALUES (3, 1, 2, '2011-01-08 1:56 am');
INSERT INTO queue(userid, movieid, position, times) VALUES (1, 2, 1, '2011-01-02 10:45 am');
INSERT INTO queue(userid, movieid, position, times) VALUES (5, 3, 1, '2011-01-13 3:05 pm');

INSERT INTO ratings(userid, movieid, rate, review) VALUES (6, 1, 5, 'excellent!');
INSERT INTO ratings(userid, movieid, rate, review) VALUES (1, 1, 1, 'boring');
INSERT INTO ratings(userid, movieid, rate, review) VALUES (2, 1, 3, 'so so');
INSERT INTO ratings(userid, movieid, rate, review) VALUES (6, 2, 5, 'i loved this movie!');
INSERT INTO ratings(userid, movieid, rate, review) VALUES (5, 2, 4, 'almost made it to my fav list');
INSERT INTO ratings(userid, movieid, rate, review) VALUES (4, 2, 3, '');
INSERT INTO ratings(userid, movieid, rate, review) VALUES (1, 3, 1, 'soooo boring; movie is so old!');

ANALYZE users;
ANALYZE creditcards;
ANALYZE movies;
ANALYZE genres;
ANALYZE actors;
ANALYZE casts;
ANALYZE queue;
ANALYZE ratings;
