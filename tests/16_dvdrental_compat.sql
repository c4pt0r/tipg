SET statement_timeout = 0;
SET client_encoding = 'UTF8';

DROP TABLE IF EXISTS actor;
CREATE TABLE actor (
    actor_id integer NOT NULL,
    first_name character varying(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);

DROP TABLE IF EXISTS category;
CREATE TABLE category (
    category_id integer NOT NULL,
    name character varying(25) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);

DROP TABLE IF EXISTS country;
CREATE TABLE country (
    country_id integer NOT NULL,
    country character varying(50) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);

DROP TABLE IF EXISTS city;
CREATE TABLE city (
    city_id integer NOT NULL,
    city character varying(50) NOT NULL,
    country_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);

DROP TABLE IF EXISTS address;
CREATE TABLE address (
    address_id integer NOT NULL,
    address character varying(50) NOT NULL,
    address2 character varying(50),
    district character varying(20) NOT NULL,
    city_id smallint NOT NULL,
    postal_code character varying(10),
    phone character varying(20) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);

DROP TABLE IF EXISTS language;
CREATE TABLE language (
    language_id integer NOT NULL,
    name character(20) NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);

DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
    customer_id integer NOT NULL,
    store_id smallint NOT NULL,
    first_name character varying(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    email character varying(50),
    address_id smallint NOT NULL,
    activebool boolean DEFAULT true NOT NULL,
    create_date date DEFAULT now() NOT NULL,
    last_update timestamp without time zone DEFAULT now(),
    active integer
);

DROP TABLE IF EXISTS staff;
CREATE TABLE staff (
    staff_id integer NOT NULL,
    first_name character varying(45) NOT NULL,
    last_name character varying(45) NOT NULL,
    address_id smallint NOT NULL,
    email character varying(50),
    store_id smallint NOT NULL,
    active boolean DEFAULT true NOT NULL,
    username character varying(16) NOT NULL,
    password character varying(40),
    last_update timestamp without time zone DEFAULT now() NOT NULL,
    picture bytea
);

DROP TABLE IF EXISTS store;
CREATE TABLE store (
    store_id integer NOT NULL,
    manager_staff_id smallint NOT NULL,
    address_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);

DROP TABLE IF EXISTS inventory;
CREATE TABLE inventory (
    inventory_id integer NOT NULL,
    film_id smallint NOT NULL,
    store_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);

DROP TABLE IF EXISTS rental;
CREATE TABLE rental (
    rental_id integer NOT NULL,
    rental_date timestamp without time zone NOT NULL,
    inventory_id integer NOT NULL,
    customer_id smallint NOT NULL,
    return_date timestamp without time zone,
    staff_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);

DROP TABLE IF EXISTS payment;
CREATE TABLE payment (
    payment_id integer NOT NULL,
    customer_id smallint NOT NULL,
    staff_id smallint NOT NULL,
    rental_id integer NOT NULL,
    amount numeric(5,2) NOT NULL,
    payment_date timestamp without time zone NOT NULL
);

DROP TABLE IF EXISTS film_actor;
CREATE TABLE film_actor (
    actor_id smallint NOT NULL,
    film_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);

DROP TABLE IF EXISTS film_category;
CREATE TABLE film_category (
    film_id smallint NOT NULL,
    category_id smallint NOT NULL,
    last_update timestamp without time zone DEFAULT now() NOT NULL
);

SHOW TABLES;

INSERT INTO actor (actor_id, first_name, last_name) VALUES (1, 'PENELOPE', 'GUINESS');
INSERT INTO actor (actor_id, first_name, last_name) VALUES (2, 'NICK', 'WAHLBERG');

SELECT * FROM actor;

INSERT INTO category (category_id, name) VALUES (1, 'Action');
INSERT INTO category (category_id, name) VALUES (2, 'Animation');

SELECT * FROM category;

INSERT INTO country (country_id, country) VALUES (1, 'Afghanistan');
INSERT INTO country (country_id, country) VALUES (2, 'Algeria');

SELECT * FROM country;

DROP TABLE film_category;
DROP TABLE film_actor;
DROP TABLE payment;
DROP TABLE rental;
DROP TABLE inventory;
DROP TABLE store;
DROP TABLE staff;
DROP TABLE customer;
DROP TABLE language;
DROP TABLE address;
DROP TABLE city;
DROP TABLE country;
DROP TABLE category;
DROP TABLE actor;
