#!/bin/sh
set -e
r=`mysql -u root --password=$MYSQL_ROOT_PASSWORD --batch --silent --database=Chinook -e 'SELECT COUNT(*) > 0 FROM ready'`
[ "$r" = 1 ]
