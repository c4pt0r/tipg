DROP TABLE IF EXISTS film_category CASCADE;
DROP TABLE IF EXISTS film_actor CASCADE;
DROP TABLE IF EXISTS inventory CASCADE;
DROP TABLE IF EXISTS payment CASCADE;
DROP TABLE IF EXISTS rental CASCADE;
DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS film CASCADE;
DROP TABLE IF EXISTS actor CASCADE;
DROP TABLE IF EXISTS category CASCADE;

CREATE TABLE actor (
    actor_id INT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    last_update TIMESTAMP
);

CREATE TABLE category (
    category_id INT PRIMARY KEY,
    name TEXT NOT NULL,
    last_update TIMESTAMP
);

CREATE TABLE film (
    film_id INT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    release_year INT,
    language_id INT,
    rental_duration INT,
    rental_rate DOUBLE PRECISION,
    length INT,
    replacement_cost DOUBLE PRECISION,
    rating TEXT,
    last_update TIMESTAMP,
    special_features TEXT,
    fulltext TEXT
);

CREATE TABLE customer (
    customer_id INT PRIMARY KEY,
    store_id INT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT,
    address_id INT,
    activebool BOOLEAN DEFAULT TRUE,
    create_date TIMESTAMP,
    last_update TIMESTAMP,
    active INT
);

CREATE TABLE rental (
    rental_id INT PRIMARY KEY,
    rental_date TIMESTAMP,
    inventory_id INT,
    customer_id INT,
    return_date TIMESTAMP,
    staff_id INT,
    last_update TIMESTAMP
);

CREATE TABLE payment (
    payment_id INT PRIMARY KEY,
    customer_id INT,
    staff_id INT,
    rental_id INT,
    amount DOUBLE PRECISION,
    payment_date TIMESTAMP
);

CREATE TABLE inventory (
    inventory_id INT PRIMARY KEY,
    film_id INT,
    store_id INT,
    last_update TIMESTAMP
);

CREATE TABLE film_actor (
    actor_id INT,
    film_id INT,
    last_update TIMESTAMP,
    PRIMARY KEY (actor_id, film_id)
);

CREATE TABLE film_category (
    film_id INT,
    category_id INT,
    last_update TIMESTAMP,
    PRIMARY KEY (film_id, category_id)
);

INSERT INTO actor VALUES (1, 'Penelope', 'Guiness', '2013-05-26 14:47:57');
INSERT INTO actor VALUES (2, 'Nick', 'Wahlberg', '2013-05-26 14:47:57');
INSERT INTO actor VALUES (3, 'Ed', 'Chase', '2013-05-26 14:47:57');

INSERT INTO category VALUES (1, 'Action', '2006-02-15 09:46:27');
INSERT INTO category VALUES (11, 'Horror', '2006-02-15 09:46:27');
INSERT INTO category VALUES (15, 'Sports', '2006-02-15 09:46:27');

INSERT INTO film VALUES (1, 'Academy Dinosaur', 'A Epic Drama', 2006, 1, 6, 0.99, 86, 20.99, 'PG', '2013-05-26 14:50:58', NULL, NULL);
INSERT INTO film VALUES (2, 'Ace Goldfinger', 'A Astounding Story', 2006, 1, 3, 4.99, 48, 12.99, 'G', '2013-05-26 14:50:58', NULL, NULL);
INSERT INTO film VALUES (3, 'Adaptation Holes', 'A Astounding Drama', 2006, 1, 7, 2.99, 50, 18.99, 'NC-17', '2013-05-26 14:50:58', NULL, NULL);
INSERT INTO film VALUES (4, 'Affair Prejudice', 'A Fanciful Documentary', 2006, 1, 5, 2.99, 117, 26.99, 'G', '2013-05-26 14:50:58', NULL, NULL);
INSERT INTO film VALUES (5, 'African Egg', 'A Fast-Paced Documentary', 2006, 1, 6, 2.99, 130, 22.99, 'G', '2013-05-26 14:50:58', NULL, NULL);
INSERT INTO film VALUES (6, 'Agent Truman', 'A Intrepid Panorama', 2006, 1, 3, 2.99, 169, 17.99, 'PG', '2013-05-26 14:50:58', NULL, NULL);
INSERT INTO film VALUES (7, 'Airplane Sierra', 'A Touching Saga', 2006, 1, 6, 4.99, 62, 28.99, 'PG-13', '2013-05-26 14:50:58', NULL, NULL);
INSERT INTO film VALUES (8, 'Airport Pollock', 'A Epic Tale', 2006, 1, 6, 4.99, 54, 15.99, 'R', '2013-05-26 14:50:58', NULL, NULL);
INSERT INTO film VALUES (9, 'Alabama Devil', 'A Thoughtful Panorama', 2006, 1, 3, 2.99, 114, 21.99, 'PG-13', '2013-05-26 14:50:58', NULL, NULL);
INSERT INTO film VALUES (10, 'Aladdin Calendar', 'A Action-Packed Tale', 2006, 1, 6, 4.99, 63, 24.99, 'NC-17', '2013-05-26 14:50:58', NULL, NULL);

INSERT INTO customer VALUES (1, 1, 'Mary', 'Smith', 'mary.smith@example.com', 5, TRUE, '2006-02-14', '2013-05-26 14:49:45', 1);
INSERT INTO customer VALUES (2, 1, 'Patricia', 'Johnson', 'patricia.johnson@example.com', 6, TRUE, '2006-02-14', '2013-05-26 14:49:45', 1);
INSERT INTO customer VALUES (3, 1, 'Linda', 'Williams', 'linda.williams@example.com', 7, TRUE, '2006-02-14', '2013-05-26 14:49:45', 0);

INSERT INTO rental VALUES (1, '2005-05-24 22:53:30', 367, 130, '2005-05-26 22:04:30', 1, '2006-02-15 21:30:53');
INSERT INTO rental VALUES (2, '2005-05-24 22:54:33', 1525, 459, '2005-05-28 19:40:33', 1, '2006-02-15 21:30:53');
INSERT INTO rental VALUES (3, '2005-05-24 23:03:39', 1711, 408, '2005-06-01 22:12:39', 1, '2006-02-15 21:30:53');

INSERT INTO payment VALUES (1, 1, 1, 76, 2.99, '2007-01-24 21:40:19');
INSERT INTO payment VALUES (2, 1, 1, 573, 0.99, '2007-01-25 15:16:50');
INSERT INTO payment VALUES (3, 1, 1, 1185, 5.99, '2007-01-28 21:44:14');
INSERT INTO payment VALUES (4, 2, 1, 1422, 0.99, '2007-01-29 21:41:03');
INSERT INTO payment VALUES (5, 2, 2, 1476, 9.99, '2007-01-30 01:05:05');

INSERT INTO inventory VALUES (1, 1, 1, '2006-02-15 05:09:17');
INSERT INTO inventory VALUES (2, 1, 1, '2006-02-15 05:09:17');
INSERT INTO inventory VALUES (3, 1, 2, '2006-02-15 05:09:17');
INSERT INTO inventory VALUES (4, 2, 1, '2006-02-15 05:09:17');
INSERT INTO inventory VALUES (5, 2, 2, '2006-02-15 05:09:17');

INSERT INTO film_actor VALUES (1, 1, '2006-02-15 05:05:03');
INSERT INTO film_actor VALUES (1, 2, '2006-02-15 05:05:03');
INSERT INTO film_actor VALUES (2, 3, '2006-02-15 05:05:03');
INSERT INTO film_actor VALUES (2, 4, '2006-02-15 05:05:03');

INSERT INTO film_category VALUES (1, 1, '2006-02-15 05:07:09');
INSERT INTO film_category VALUES (2, 11, '2006-02-15 05:07:09');
INSERT INTO film_category VALUES (3, 15, '2006-02-15 05:07:09');
INSERT INTO film_category VALUES (4, 1, '2006-02-15 05:07:09');
INSERT INTO film_category VALUES (5, 15, '2006-02-15 05:07:09');
