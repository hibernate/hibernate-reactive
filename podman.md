# Podman

Our tests needs several databases up and running.
This is a list of commands to start the required databases and command line interfaces using [podman].

These are the credentials used to connect to the dbs:
* username: hreactive
* password: hreactive
* database: hreactive

[podman]:https://podman.io/

## [PostgreSQL]

[PostgreSQL]:https://www.postgresql.org/

* Server
```
  sudo podman run --ulimit memlock=-1:-1 -it --rm=true --memory-swappiness=0 --name HibernateTestingPGSQL -e POSTGRES_USER=hreactive -e POSTGRES_PASSWORD=hreactive -e POSTGRES_DB=hreactive -p 5432:5432 postgres
```

* CLI
```
  sudo podman exec -it HibernateTestingPGSQL psql  -U hreactive -W -d hreactive
```

## [MySQL]

[MySQL]:https://www.mysql.com/

* Server
```
  sudo podman run --rm -it --name HibernateTestingMariaDB -e MYSQL_ROOT_PASSWORD=hreactive -e MYSQL_DATABASE=hreactive -e MYSQL_USER=hreactive -e MYSQL_PASSWORD=hreactive -p 3306:3306 mysql
```

* CLI
```
  sudo podman exec -it HibernateTestingMariaDB mysql -U hreactive -p
```

## [DB2]

[DB2]:https://www.ibm.com/analytics/db2

* Server
```
sudo podman run --rm -it -e LICENSE=accept --privileged=true --name HibernateTestingDB2 -e DBNAME=hib-rx -e DB2INSTANCE=hib_rx -e DB2INST1_PASSWORD=hibernate-rx -p 50000:50000 ibmcom/db2
```

* CLI
```
  sudo podman exec -ti HibernateTestingDB2 bash -c "su - ${DB2INSTANCE}"
```
