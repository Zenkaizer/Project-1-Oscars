DROP TABLE IF EXISTS film_protagonist CASCADE;
DROP TABLE IF EXISTS film_director CASCADE;
DROP TABLE IF EXISTS film_country CASCADE;
DROP TABLE IF EXISTS film CASCADE;
DROP TABLE IF EXISTS director CASCADE;
DROP TABLE IF EXISTS protagonist CASCADE;
DROP TABLE IF EXISTS country CASCADE;
DROP TABLE IF EXISTS category CASCADE;
CREATE TABLE film (
    id int(10) NOT NULL,
    title varchar(255) NOT NULL,
    year int(10) NOT NULL,
    awards int(10) NOT NULL,
    nominations int(10) NOT NULL,
    duration int(10) NOT NULL,
    PRIMARY KEY (id));
CREATE TABLE director (
    id int(10) NOT NULL,
    director varchar(255) NOT NULL,
    PRIMARY KEY (id));
CREATE TABLE film_director (
    id_film int(10) NOT NULL,
    id_director int(10) NOT NULL,
    PRIMARY KEY (id_film, id_director));
CREATE TABLE protagonist (
    id int(10) NOT NULL,
    protagonist varchar(255) NOT NULL,
    PRIMARY KEY (id));
CREATE TABLE film_protagonist (
    id_film int(10) NOT NULL,
    id_protagonist int(10) NOT NULL,
    PRIMARY KEY (id_film, id_protagonist));
CREATE TABLE country (
    id int(10) NOT NULL,
    country varchar(255) NOT NULL,
    country_es varchar(255) NOT NULL,
    PRIMARY KEY (id));
CREATE TABLE film_country (
    id_film int(10) NOT NULL,
    id_country int(10) NOT NULL,
    PRIMARY KEY (id_film, id_country));
CREATE TABLE category (
    id int(10) NOT NULL,
    category varchar(255) NOT NULL,
    category_es varchar(255) NOT NULL,
    PRIMARY KEY (id));
ALTER TABLE film_director ADD CONSTRAINT FKfilm_direc202422 FOREIGN KEY (id_film) REFERENCES film (id);
ALTER TABLE film_director ADD CONSTRAINT FKfilm_direc550567 FOREIGN KEY (id_director) REFERENCES director (id);
ALTER TABLE film_protagonist ADD CONSTRAINT FKfilm_prota141142 FOREIGN KEY (id_film) REFERENCES film (id);
ALTER TABLE film_protagonist ADD CONSTRAINT FKfilm_prota664523 FOREIGN KEY (id_protagonist) REFERENCES protagonist (id);
ALTER TABLE film_country ADD CONSTRAINT FKfilm_count176306 FOREIGN KEY (id_film) REFERENCES film (id);
ALTER TABLE film_country ADD CONSTRAINT FKfilm_count651429 FOREIGN KEY (id_country) REFERENCES country (id);