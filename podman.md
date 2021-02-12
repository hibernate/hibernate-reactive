# How to start the test databases using Podman

The following sections will use [Podman][podman] to start the database of your choice,
create the schema and create the credentials required to run the tests during the build.

Podman is a daemonless container engine for developing, managing, and running OCI
Containers on your Linux System.
Containers can either be run as root or in rootless mode.

---
**TIP**

If you replace `podman` with `sudo docker`, they will also work with [Docker][docker].
Example:

```
podman run --rm --name HibernateTestingPGSQL postgres:13.0
```

becomes for Docker:

```
sudo docker run --rm --name HibernateTestingPGSQL postgres:13.0
```
---

[podman]:https://podman.io/
[docker]:https://www.docker.com/

## PostgreSQL

Use the following command to start a [PostgreSQL][postgresql] database with the
required credentials and schema to run the tests:

[postgresql]:https://www.postgresql.org

```
podman run --rm --name HibernateTestingPGSQL \
    -e POSTGRES_USER=hreact -e POSTGRES_PASSWORD=hreact -e POSTGRES_DB=hreact \
    -p 5432:5432 postgres:13.1
```

When the database has started, you can run the tests on PostgreSQL with:

```
./gradlew test
```

Optionally, you can connect to the database with the [PostgreSQL interactive terminal][psql](`psql`)
using:

```
podman exec -it HibernateTestingPGSQL psql -U hreact -W -d hreact
```
When asked for the password, type: `hreact`

[psql]:https://www.postgresql.org/docs/9.6/app-psql.html

## MySQL

Use the following command to start a [MySQL][mysql] database with the required credentials
and schema to run the tests:

[mysql]:https://www.mysql.com/

```
podman run --rm --name HibernateTestingMySQL \
    -e MYSQL_ROOT_PASSWORD=hreact -e MYSQL_DATABASE=hreact -e MYSQL_USER=hreact -e MYSQL_PASSWORD=hreact \
    -p 3306:3306 mysql:8.0.23
```

When the database has started, you can run the tests on MySQL with:

```
./gradlew test -Pdb=mysql
```

Optionally, you can connect to the database with the [MySQL Command-Line Client][mysql-cli](`mysql`) using:

[mysql-cli]:https://www.mysql.com/

```
podman exec -it HibernateTestingMySQL mysql -U hreact -phreact
```

## Db2

Use the following command to start a [Db2][db2] database with the required credentials
and schema to run the tests:

[db2]:https://www.ibm.com/analytics/db2

```
podman run --rm -e LICENSE=accept --privileged=true --name HibernateTestingDB2 \
    -e DBNAME=hreact -e DB2INSTANCE=hreact -e DB2INST1_PASSWORD=hreact \
    -e PERSISTENT_HOME=false -e ARCHIVE_LOGS=false -e AUTOCONFIG=false \
    -p 50000:50000 ibmcom/db2:11.5.4.0
```

When the database has started, you can run the tests on Db2 with:

```
./gradlew test -Pdb=db2
```

Optionally, you can connect to the database with the [Db2 command line interface][db2-cli] using:

```
podman exec -ti HibernateTestingDB2 bash -c "su - hreact -c db2 connect"
```
[db2-cli]:https://www.ibm.com/support/knowledgecenter/en/SSEPEK_11.0.0/comref/src/tpc/db2z_commandlineprocessor.html
