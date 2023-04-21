DROP TABLE IF EXISTS years CASCADE;
DROP TABLE IF EXISTS countries CASCADE;
DROP TABLE IF EXISTS protagonists CASCADE;
DROP TABLE IF EXISTS director CASCADE;
DROP TABLE IF EXISTS films CASCADE;
CREATE TABLE years (
  id                int(10) NOT NULL,
  year              int(10) NOT NULL,
  PRIMARY KEY (id));
DROP TABLE IF EXISTS oscars CASCADE;
CREATE TABLE countries (
  id                int(10) NOT NULL AUTO_INCREMENT,
  film_title        varchar(255) NOT NULL UNIQUE,
  year              int(10) NOT NULL,
  film_nominations  int(10) NOT NULL,
  film_awards       int(10) NOT NULL,
  PRIMARY KEY (id));
DROP TABLE IF EXISTS oscars CASCADE;
CREATE TABLE protagonists (
  id                int(10) NOT NULL AUTO_INCREMENT,
  film_title        varchar(255) NOT NULL UNIQUE,
  year              int(10) NOT NULL,
  film_nominations  int(10) NOT NULL,
  film_awards       int(10) NOT NULL,
  PRIMARY KEY (id));