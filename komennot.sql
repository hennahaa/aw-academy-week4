/*Harkkoja 6*/

-- 1 Luo uusi taulu

CREATE TABLE attributes (
Id SERIAL PRIMARY KEY,
name varchar(255) NOT NULL,
person_id int,
CONSTRAINT fk_person
    FOREIGN KEY(person_id)
        REFERENCES person(id)
);

-- 2 Lisää perosn tauluun uusi rivi

INSERT INTO person(name, age, student) VALUES ('Hemuli', 31, TRUE);

--SELECT * FROM person;

--3 Lisää certificates tauluun uusi rivi siten, että liität itsellesi Scrum nimisen sertifikaatin

INSERT INTO attributes(name, person_id) VALUES ('Scrum', 4);

--4 Lisätään Aku Ankalle GCP sertifikaatit ja Roope Sedälle Scrum ja GCP sertit

INSERT INTO attributes(name, person_id) VALUES ('GCP', 1);
INSERT INTO attributes(name, person_id) VALUES ('Scrum', 2);
INSERT INTO attributes(name, person_id) VALUES ('GCP', 2);

-- 5 Hae person taulusta kaikki Scrum sertin haltijat

SELECT person.name AS Scrumin_haltija, attributes.name AS sertti
FROM person
LEFT JOIN attributes ON person.id = attributes.person_id
WHERE attributes.name = 'Scrum'; 

-- 6 Hae person taulusta kaikki GCP sertin haltijat

SELECT person.name AS GCPN_haltija, attributes.name AS sertti
FROM person
LEFT JOIN attributes ON person.id = attributes.person_id
WHERE attributes.name = 'GCP'; 

/*Harkkoja 7*/
-- WORLD juttuja

SELECT * FROM country;

/*Harkkoja 8*/

-- Hae city taulusta kaikki rivit joiden country_code on FOREIGN

SELECT * FROM city WHERE country_code = 'FIN';

-- Laske kuinka monta kaupunkia yhdysvalloista löytyy

SELECT COUNT(*) AS kaupunkien_lkm_usa FROM city WHERE country_code = 'USA';

-- Laske yhteen yhdysvaltojen kaupunkien populaatio

SELECT SUM(population) AS kaupunkien_populaatio_usa FROM city WHERE country_code = 'USA';

-- Listaa kaupungit, joiden väkiluku on 1 ja 2 miljoonan välillä (vain 15 riviä)

SELECT * FROM city WHERE population >= 1000000 AND population <= 2000000 LIMIT 15;

-- Laske yhdysvaltojen osavaltioiden kaupunkien yhteenlaskettu väkiluku ryhmitettynä osavaltioittain

SELECT SUM(population) AS osavaltio_pop, district AS osavaltio FROM city WHERE country_code = 'USA' GROUP BY district;

-- Millä maalla korkein lifeexpectancy, vain yksi tulos, poista null

SELECT lifeexpectancy, name
FROM country
WHERE lifeexpectancy IS NOT NULL
ORDER BY lifeexpectancy DESC
LIMIT 1;

-- Laske maakohtaisesti, kuinka paljon tietyn maan kaikissa kaupungeissa on yhteensä asukkaita, ota oleelliset sarakkeet mukaan hakutulokseen

SELECT SUM(population) AS city_pop, country_code
FROM city
GROUP BY country_code
ORDER BY country_code;

/*SELECT population, country_code
FROM city
WHERE country_code = 'ABW';

SELECT population
FROM country
WHERE code = 'ABW'; */

-- lisää mukaan country pop

SELECT SUM(city.population) AS city_pop, city.country_code, country.population
FROM city, country
WHERE city.country_code = country.code
GROUP BY country_code, country.population
ORDER BY country_code;

/*Harkka 9 JOIN*/

-- Listaa maat pääkaupungeittain, käytä Joinia

SELECT country.name AS maa, city.name AS paakaupunki
FROM city
INNER JOIN country ON country.capital = city.id;

-- Listaa vain Espanja

SELECT country.name AS maa, city.name AS paakaupunki
FROM country
LEFT JOIN city ON country.capital = city.id
WHERE country.name = 'Spain';

-- Listaa kaikki euroopan maat

SELECT country.name AS maa, city.name AS paakaupunki
FROM country
LEFT JOIN city ON country.capital = city.id
WHERE country.continent = 'Europe';

-- Listaa kaikki kielet joita puhutaan Southeast Asian alueella

SELECT country_language.language AS kieli, country.name AS maa
FROM country_language
INNER JOIN country ON country_language.country_code = country.code
WHERE country.region = 'Southeast Asia';

/*Harkka 10 Alikyselyt*/

-- Käytä alikyselyä ja hae kaikki kaupungit joiden populaatio on suurepi kuin koko suomen populaatio

SELECT population AS vakiluku, name AS kaupunki FROM city
WHERE population >
(SELECT population
FROM country
WHERE code = 'FIN');

-- Hae kaikki kaupungit, jossa on yli miljoona asukasta, ja jotka ovat maassa jossa puhutaan englantia

SELECT name AS kaupunki FROM city
WHERE population >= 1000000 AND country_code IN
(SELECT country_code
FROM country_language
WHERE language = 'English');
