#!/bin/sh

last_id=`docker exec dozer-tests-films-db psql -U postgres -d films -t -c "SELECT MAX(film_id) FROM \"film\"" | sed 's/ *$//g'`
last_id=`echo $last_id | sed -e 's/^[[:space:]]*//'`
echo "Last id: $last_id"

url="http://localhost:8080/films/${last_id##*( )}"
echo $url

curl $url

docker exec dozer-tests-films-db psql -U postgres -d films -t -c "BEGIN; UPDATE \"film\" SET description = 'dozer test' WHERE film_id = $last_id; COMMIT;"

curl $url