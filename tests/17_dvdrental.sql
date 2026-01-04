SELECT COUNT(*) AS actor_count FROM actor;
SELECT COUNT(*) AS film_count FROM film;
SELECT COUNT(*) AS customer_count FROM customer;
SELECT COUNT(*) AS rental_count FROM rental;
SELECT COUNT(*) AS payment_count FROM payment;
SELECT COUNT(*) AS film_actor_count FROM film_actor;
SELECT COUNT(*) AS inventory_count FROM inventory;
SELECT COUNT(*) AS category_count FROM category;

SELECT COUNT(*) AS g_films FROM film WHERE rating = 'G';
SELECT COUNT(*) AS pg_films FROM film WHERE rating = 'PG';
SELECT COUNT(*) AS pg13_films FROM film WHERE rating = 'PG-13';
SELECT COUNT(*) AS r_films FROM film WHERE rating = 'R';
SELECT COUNT(*) AS nc17_films FROM film WHERE rating = 'NC-17';

SELECT MIN(length) AS min_len, MAX(length) AS max_len FROM film;

SELECT SUM(amount) AS total_revenue FROM payment;

SELECT COUNT(*) AS short_films FROM film WHERE length < 60;
SELECT COUNT(*) AS long_films FROM film WHERE length > 120;
SELECT COUNT(*) AS active_customers FROM customer WHERE active = 1;

SELECT COUNT(*) AS sports_films FROM film f 
INNER JOIN film_category fc ON f.film_id = fc.film_id 
INNER JOIN category c ON fc.category_id = c.category_id 
WHERE c.name = 'Sports';

SELECT COUNT(*) AS action_films FROM film f 
INNER JOIN film_category fc ON f.film_id = fc.film_id 
INNER JOIN category c ON fc.category_id = c.category_id 
WHERE c.name = 'Action';

SELECT COUNT(*) AS horror_films FROM film f 
INNER JOIN film_category fc ON f.film_id = fc.film_id 
INNER JOIN category c ON fc.category_id = c.category_id 
WHERE c.name = 'Horror';
