# Podman

Our tests needs several databases up and running.
This is a list of commands to start the required databases and command line interfaces using [podman].

These are the credentials used to connect to the dbs:
* username: hreact
* password: hreact
* database: hreact

[podman]:https://podman.io/

## [PostgreSQL]

[PostgreSQL]:https://www.postgresql.org/

* Server
```
  sudo podman run --ulimit memlock=-1:-1 -it --rm=true --memory-swappiness=0 --name HibernateTestingPGSQL -e POSTGRES_USER=hreact -e POSTGRES_PASSWORD=hreact -e POSTGRES_DB=hreact -p 5432:5432 postgres
```

* CLI
```
  sudo podman exec -it HibernateTestingPGSQL psql  -U hreact -W -d hreact
```

## [MySQL]

[MySQL]:https://www.mysql.com/

* Server
```
  sudo podman run --rm -it --name HibernateTestingMariaDB -e MYSQL_ROOT_PASSWORD=hreact -e MYSQL_DATABASE=hreact -e MYSQL_USER=hreact -e MYSQL_PASSWORD=hreact -p 3306:3306 mysql
```

* CLI
```
  sudo podman exec -it HibernateTestingMariaDB mysql -U hreact -p
```

## [DB2]

[DB2]:https://www.ibm.com/analytics/db2

* Server
```
sudo podman run --rm -it -e LICENSE=accept --privileged=true --name HibernateTestingDB2 -e DBNAME=hreact -e DB2INSTANCE=hreact -e DB2INST1_PASSWORD=hreact -p 50000:50000 ibmcom/db2
```

* CLI
```
  sudo podman exec -ti HibernateTestingDB2 bash -c "su - ${DB2INSTANCE}"
```
