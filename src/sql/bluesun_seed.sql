select @@session.sql_mode;

set @@session.sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';

drop table if exists star_wars_films;

CREATE TABLE star_wars_films (
  id int unsigned not null auto_increment primary key,
  `name` varchar(100) not null,
  gross decimal(15,2) not null,
  budget decimal(15,2) null,
  rotten_tomatoes float not null,
  release_date date not null default '0000-00-00',
  created timestamp not null default current_timestamp,
  deleted timestamp null default '0000-00-00 00:00:00'
);

insert into star_wars_films (`name`, release_date, gross, budget, rotten_tomatoes, deleted) values
('A New Hope', '1977-05-22', 775512064, 11000000, .93, null),
('Empire Strikes Back', '1980-05-21', 547975067, 18000000, .94, null),
('Return of the Jedi', '1983-05-25', 475306177, 32500000, .82, null),
('The Phantom Menace', '1999-05-19', 1027044677, 115000000, .53, now()),
('Attack of the Clones', '2002-05-16', 649436358, 115000000, .65, null),
('Revenge of the Sith', '2005-05-19', 850035635, 113000000, .8, null),
('The Force Awakens', '2015-12-18', 2068223624, 245000000, .93, null),
('The Last Jedi', '2017-12-17', 1333539889, 200000000, .91, now()),
('The Rise of Skywalker', '2019-12-20', 1000707423, 275000000, .53, now()),
('Untitled 10', '0000-00-00', 0, null, .00, '0000-00-00 00:00:00');
