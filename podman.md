# How to start the test databases using Podman

The following sections will use [Podman][podman] to start the database of your choice,
create the schema and create the credentials required to run the tests during the build.

Podman is a daemonless container engine for developing, managing, and running OCI
Containers on your Linux System.
Containers can either be run as root or in rootless mode.

---
**TIP**

If you replace `podman` with `sudo docker`, they will also work with [Docker][docker].

---

Each database has a Dockerfile in `tooling/docker/` that contains the base image,
environment variables, exposed ports, and startup commands.
You can use these Dockerfiles directly with `podman build` to start a database for testing
without Testcontainers.

---
**TIP**

Use the `-t` option with `podman build` to tag the image with a specific version,
e.g. `-t hreact-postgresql:18.3`. Without it, the image defaults to `:latest`.

---

[podman]:https://podman.io/
[docker]:https://www.docker.com/

## PostgreSQL

Build and start a [PostgreSQL][postgresql] database:

[postgresql]:https://www.postgresql.org

```
podman build -f tooling/docker/postgresql.Dockerfile -t hreact-postgresql .
podman run --rm --name HibernateTestingPGSQL -p 5432:5432 hreact-postgresql
```

When the database has started, you can run the tests on PostgreSQL with:

```
./gradlew testDbPostgreSQL -PskipTestcontainers
```

Optionally, you can connect to the database with the [PostgreSQL interactive terminal][psql](`psql`)
using this one-liner:

```
podman exec -e PGPASSWORD=hreact -it HibernateTestingPGSQL psql -U hreact -d hreact
```

[psql]:https://www.postgresql.org/docs/14/app-psql.html

## MariaDB

Build and start a [MariaDB][mariadb] database:

[mariadb]:https://mariadb.org/

```
podman build -f tooling/docker/maria.Dockerfile -t hreact-maria .
podman run --rm --name HibernateTestingMariaDB -p 3306:3306 hreact-maria
```

When the database has started, you can run the tests on MariaDB with:

```
./gradlew testDbMariaDB -PskipTestcontainers
```

Optionally, you can connect to the database with the [MySQL Command-Line Client][mysql-cli](`mysql`) using:

[mysql-cli]:https://www.mysql.com/

```
podman exec -it HibernateTestingMariaDB mysql -U hreact -phreact
```

## MySQL

Build and start a [MySQL][mysql] database:

[mysql]:https://www.mysql.com/

```
podman build -f tooling/docker/mysql.Dockerfile -t hreact-mysql .
podman run --rm --name HibernateTestingMySQL -p 3306:3306 hreact-mysql
```

When the database has started, you can run the tests on MySQL with:

```
./gradlew testDbMySQL -PskipTestcontainers
```

Optionally, you can connect to the database with the [MySQL Command-Line Client][mysql-cli](`mysql`) using:

```
podman exec -it HibernateTestingMySQL mysql -U hreact -phreact
```

## CockroachDB

Build and start a [CockroachDB][cockroachdb] database:

[cockroachdb]:https://www.cockroachlabs.com/get-cockroachdb/

```
podman build -f tooling/docker/cockroachdb.Dockerfile -t hreact-cockroachdb .
podman run --rm --name HibernateTestingCockroachDB -p 26257:26257 -p 8080:8080 hreact-cockroachdb
```

Some of tests needs temporary tables and because this is an experimental feature in
CockroachDB, it needs to be enabled after the database has started:

```
podman exec -it HibernateTestingCockroachDB ./cockroach sql --insecure \
    -e "SET CLUSTER SETTING sql.defaults.experimental_temporary_tables.enabled = 'true';"
```

When the database has started, you can run the tests on CockroachDB with:

```
./gradlew testDbCockroachDB -PskipTestcontainers
```

Optionally, you can connect to the database with the [Cockroach commands][cockroach-cli](`cockroach`)
using:

```
podman exec -it HibernateTestingCockroachDB ./cockroach sql --insecure
```

[cockroach-cli]:https://www.cockroachlabs.com/docs/stable/cockroach-commands.html

## Db2

Build and start a [Db2][db2] database:

[db2]:https://www.ibm.com/analytics/db2

```
podman build -f tooling/docker/db2.Dockerfile -t hreact-db2 .
podman run --rm --privileged --name HibernateTestingDB2 -p 50000:50000 hreact-db2
```

**NOTE:** Db2 requires `--privileged` mode. On Fedora with rootless Podman, this may not work.
See the [Db2 with rootless Podman](#db2-with-rootless-podman) section for workarounds.

When the database has started, you can run the tests on Db2 with:

```
./gradlew testDbDB2 -PskipTestcontainers
```

Optionally, you can connect to the database with the [Db2 command line interface][db2-cli] using:

```
podman exec -ti HibernateTestingDB2 bash -c "su - hreact -c db2 connect"
```
[db2-cli]:https://www.ibm.com/support/knowledgecenter/en/SSEPEK_11.0.0/comref/src/tpc/db2z_commandlineprocessor.html

### Db2 with rootless Podman

Db2 requires `--privileged` mode for shared memory management, which doesn't work with
rootless Podman. You can use rootful Podman instead:

```
sudo systemctl enable --now podman.socket
sudo podman build -f tooling/docker/db2.Dockerfile -t hreact-db2 .
sudo podman run --rm --privileged --name HibernateTestingDB2 -p 50000:50000 hreact-db2
```

## Microsoft SQL Server

Build and start a [Microsoft SQL Server][mssql] database:

[mssql]:https://www.microsoft.com/en-gb/sql-server/

```
podman build -f tooling/docker/sqlserver.Dockerfile -t hreact-sqlserver .
podman run --rm --name HibernateTestingMSSQL -p 1433:1433 hreact-sqlserver
```

When the database has started, you can run the tests on MS SQL Server with:

```
./gradlew testDbMSSQLServer -PskipTestcontainers
```

Optionally, you can connect to the database with the [sqlcmd utility][sqlcmd-cli] using:

```
podman exec -it HibernateTestingMSSQL /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P '~!HReact!~'
```

[sqlcmd-cli]:https://docs.microsoft.com/en-us/sql/tools/sqlcmd-utility?view=sql-server-ver15

## Oracle Database

Build and start an [Oracle Database Free][oracle]:

[oracle]:https://www.oracle.com/database/free/

```
podman build -f tooling/docker/oracle.Dockerfile -t hreact-oracle .
podman run --rm --name HibernateTestingOracle -p 1521:1521 hreact-oracle
```

**NOTE:** Oracle Database takes 30-60 seconds to fully initialize after the container starts.
Wait for the message "DATABASE IS READY TO USE!" in the logs before running tests:

```
podman logs -f HibernateTestingOracle
```

When the database has started, you can run the tests on Oracle with:

```
./gradlew testDbOracle -PskipTestcontainers
```
