### Setup a sample db for testing

```
docker-compose up -d

#Setup Postgres
./scripts/download_and_insert.sh
```

### Larger dataset
Create database large_film
```
CREATE DATABASE large_film;
```
Generate using the script `generate_fake_data.py`
```
cd docker;
python scripts/generate_fake_data.py
```

Run in Postgres
```
COPY public.film (film_id, title, description, release_year, language_id, original_language_id, rental_duration, rental_rate, length, replacement_cost, rating, last_update, special_features, fulltext)
FROM
  '/var/lib/postgresql/data/film.csv' WITH (
    FORMAT csv);

```

### Resources
A bigger dataset can be downloaded from here
https://github.com/toddwschneider/nyc-taxi-data