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
