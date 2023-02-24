# New York Taxi Data

Download `fhvhv_tripdata_2022-01.parquet` from <https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2022-01.parquet> and put it under `trips/` directory.

Download `taxi+_zone_lookup.csv` from <https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv> and put it under `zones/` directory.

Run `dozer` from this directory.

## Run Taxi Data Sample with Dozer

```bash
docker run -it \
  -v "$PWD":/usr/dozer \
  -p 8080:8080 \
  -p 50051:50051 \
  --platform linux/amd64 \
  public.ecr.aws/getdozer/dozer:dev \
  dozer
```

## Expected Output

```bash
____   ___ __________ ____
|  _ \ / _ \__  / ____|  _ \
| | | | | | |/ /|  _| | |_) |
| |_| | |_| / /_| |___|  _ <
|____/ \___/____|_____|_| \_\


Dozer Version: 0.1.7

INFO Initiating app:     
INFO Home dir: ./.dozer    
INFO [API] Configuration    
+------+---------+-------+
| Type | IP      | Port  |
+------+---------+-------+
| REST | 0.0.0.0 | 8080  |
+------+---------+-------+
| GRPC | 0.0.0.0 | 50051 |
+------+---------+-------+
INFO [API] Endpoints    
+--------+-------------+-----+
| Path   | Name        | Sql |
+--------+-------------+-----+
| /trips | trips_cache |     |
+--------+-------------+-----+
INFO [ny_taxi] Connection parameters    
+------+----+
| path | ./ |
+------+----+
INFO [ny_taxi] ✓ Connection validation completed  
```
