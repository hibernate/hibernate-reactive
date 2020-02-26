# Podman

Our tests needs several databases up and running.
This is a list of commands to start the required databases and command line interfaces using [podman].

These are the credentials used to connect to the dbs:
* username: hibernate-rx
* password: hibernate-rx
* database: hibernate-rx

[podman]:https://podman.io/

## [PostgreSQL]

[PostgreSQL]:https://www.postgresql.org/

* Server
```
  sudo podman run --ulimit memlock=-1:-1 -it --rm=true --memory-swappiness=0 --name HibernateTestingPGSQL -e POSTGRES_USER=hibernate-rx -e POSTGRES_PASSWORD=hibernate-rx -e POSTGRES_DB=hibernate-rx -p 5432:5432 postgres
```

* CLI
```
  sudo podman exec -it HibernateTestingPGSQL psql  -U hibernate-rx -W -d hibernate-rx
```

## [MySQL]

[MySQL]:https://www.mysql.com/

* Server
```
  sudo podman run --rm -it --name HibernateTestingMariaDB -e MYSQL_ROOT_PASSWORD=hibernate-rx -e MYSQL_DATABASE=hibernate-rx -e MYSQL_USER=hibernate-rx -e MYSQL_PASSWORD=hibernate-rx -p 3306:3306 mysql
```

* CLI
```
  sudo podman exec -it HibernateTestingMariaDB mysql -U hibernate-rx -p
```
