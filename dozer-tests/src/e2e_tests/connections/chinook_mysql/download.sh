#!/bin/sh

curl --create-dirs -o ./data/init.sql https://raw.githubusercontent.com/lerocha/chinook-database/e7e6d5f008e35d3f89d8b8a4f8d38e3bfa7e34bd/ChinookDatabase/DataSources/Chinook_MySql.sql

cat >>./data/init.sql <<'EOSQL'

CREATE TABLE `ready` (`ready` INT);
INSERT INTO `ready` (`ready`) VALUES (1);

EOSQL
