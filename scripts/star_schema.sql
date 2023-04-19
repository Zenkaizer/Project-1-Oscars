DROP TABLE IF EXISTS oscars CASCADE;
CREATE TABLE oscars (
  id                int(10) NOT NULL AUTO_INCREMENT,
  film_title        varchar(255) NOT NULL UNIQUE,
  year              int(10) NOT NULL,
  film_nominations  int(10) NOT NULL,
  film_awards       int(10) NOT NULL,
  PRIMARY KEY (id));