FROM mongodb/mongodb-community-server:6.0.8-ubi8


ADD --chown=mongod https://atlas-education.s3.amazonaws.com/sampledata.archive /data/sampledata.archive

COPY --chown=mongod ./init.sh /docker-entrypoint-initdb.d/00-init.sh

COPY --chown=mongod --chmod=0700 ./entrypoint.sh /data/entrypoint.sh

ENTRYPOINT ["/data/entrypoint.sh"]
CMD ["mongod", "--replSet", "rs0", "--bind_ip", "localhost,mflix"]
