[![Hibernate team logo](http://static.jboss.org/hibernate/images/hibernate_logo_whitebkg_200px.png)](https://hibernate.org/reactive)

[![Main branch build status](https://github.com/hibernate/hibernate-reactive/workflows/Hibernate%20Reactive%20CI/badge.svg?style=flat)](https://github.com/hibernate/hibernate-reactive/actions?query=workflow%3A%22Hibernate+Reactive+CI%22)
[![Apache 2.0 license](https://img.shields.io/badge/License-APACHE%202.0-green.svg?logo=APACHE&style=flat)](https://opensource.org/licenses/Apache-2.0)
[![Latest version on Maven Central](https://img.shields.io/maven-central/v/org.hibernate.reactive/hibernate-reactive-core.svg?label=Maven%20Central&logo=apache-maven&style=flat)](https://search.maven.org/search?q=g:%22org.hibernate.reactive%22)
[![Developers stream on Zulip](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg?logo=zulip&style=flat)](https://hibernate.zulipchat.com/#narrow/stream/205413-hibernate-reactive-dev)
[![Hibernate Reactive documentation](https://img.shields.io/badge/Hibernate-Documentation-orange.svg?logo=Hibernate&style=flat)](https://hibernate.org/reactive/documentation/)

# Hibernate Reactive

A reactive API for [Hibernate ORM][], supporting non-blocking database
drivers and a reactive style of interaction with the database.

Hibernate Reactive may be used in any plain Java program, but is 
especially targeted toward usage in reactive environments like 
[Quarkus][] and [Vert.x][].

Currently [PostgreSQL][], [MySQL][], [MariaDB][], [Db2][], 
[CockroachDB][], [MS SQL Server][MSSQL] and [Oracle][] are supported.

Learn more at <http://hibernate.org/reactive>.

[Hibernate ORM]: https://hibernate.org/orm/
[Quarkus]: https://quarkus.io
[Quarkus quickstarts]: https://github.com/quarkusio/quarkus-quickstarts
[Vert.x]: https://vertx.io

## Compatibility

Hibernate Reactive has been tested with:

- Java 11, 17, 18
- PostgreSQL 15
- MySQL 8
- MariaDB 10
- Db2 11.5
- CockroachDB 21.2
- MS SQL Server 2019
- Oracle 21.3
- [Hibernate ORM][] 5.6.12.Final
- [Vert.x Reactive PostgreSQL Client](https://vertx.io/docs/vertx-pg-client/java/) 4.3.4
- [Vert.x Reactive MySQL Client](https://vertx.io/docs/vertx-mysql-client/java/) 4.3.4
- [Vert.x Reactive Db2 Client](https://vertx.io/docs/vertx-db2-client/java/) 4.3.4
- [Vert.x Reactive MS SQL Server Client](https://vertx.io/docs/vertx-mssql-client/java/) 4.3.4
- [Vert.x Reactive Oracle Client](https://vertx.io/docs/vertx-oracle-client/java/) 4.3.4
- [Quarkus][Quarkus] via the Hibernate Reactive extension

[PostgreSQL]: https://www.postgresql.org
[MySQL]: https://www.mysql.com
[MariaDB]: https://mariadb.com
[DB2]: https://www.ibm.com/analytics/db2
[CockroachDB]: https://www.cockroachlabs.com/
[MSSQL]: https://www.microsoft.com/en-gb/sql-server
[Oracle]: https://www.oracle.com/database/

## Documentation

The [Introduction to Hibernate Reactive][introduction] covers 
everything you need to know to get started, including:

- [setting up a project][build] that uses Hibernate Reactive and the 
  Vert.x reactive SQL client for your database,
- [configuring][config] Hibernate Reactive to access your database,
- writing Java code to [define the entities][model] of your data model, 
- writing reactive data access code [using a reactive session][session], 
  and
- [tuning the performance][performance] of your program.

We recommend you start there!

The [Vert.x and Hibernate Reactive How-to][vertx-hr] explains how to use
Hibernate Reactive in Vert.x.

The [Hibernate Reactive with Panache Guide][reactive-panache] introduces
Panache Reactive, an active record-style API based on Hibernate Reactive.

[introduction]: https://github.com/hibernate/hibernate-reactive/blob/main/documentation/src/main/asciidoc/reference/introduction.adoc

[build]: https://github.com/hibernate/hibernate-reactive/blob/main/documentation/src/main/asciidoc/reference/introduction.adoc#including-hibernate-reactive-in-your-project-build
[config]: https://github.com/hibernate/hibernate-reactive/blob/main/documentation/src/main/asciidoc/reference/introduction.adoc#basic-configuration
[model]: https://github.com/hibernate/hibernate-reactive/blob/main/documentation/src/main/asciidoc/reference/introduction.adoc#mapping-entity-classes
[session]: https://github.com/hibernate/hibernate-reactive/blob/main/documentation/src/main/asciidoc/reference/introduction.adoc#using-the-reactive-session
[performance]: https://github.com/hibernate/hibernate-reactive/blob/main/documentation/src/main/asciidoc/reference/introduction.adoc#tuning-and-performance

[vertx-hr]: https://how-to.vertx.io/hibernate-reactive-howto/
[reactive-panache]: https://quarkus.io/guides/hibernate-reactive-panache

## Examples

The directory [`examples`][examples] contains several small projects showing
different features of Hibernate Reactive:

  - [CRUD operations using the session](https://github.com/hibernate/hibernate-reactive/tree/main/examples/session-example)
  - [Native queries and stateless session](https://github.com/hibernate/hibernate-reactive/tree/main/examples/native-sql-example)

[examples]: https://github.com/hibernate/hibernate-reactive/tree/main/examples

## Quarkus quickstarts

A collection of [quickstarts][Quarkus quickstarts] for Quarkus is available on GitHub:

  - [Hibernate Reactive with RESTEasy Reactive](https://github.com/quarkusio/quarkus-quickstarts/tree/main/hibernate-reactive-quickstart)
  - [Hibernate Reactive with Panache](https://github.com/quarkusio/quarkus-quickstarts/tree/main/hibernate-reactive-panache-quickstart)
  - [Hibernate Reactive with Vert.x Web Routes](https://github.com/quarkusio/quarkus-quickstarts/tree/main/hibernate-reactive-routes-quickstart)

Or you can [generate a new Quarkus project](https://code.quarkus.io/?g=org.acme&a=code-with-quarkus&v=1.0.0-SNAPSHOT&b=MAVEN&s=r1s&cn=code.quarkus.io)
that uses the Hibernate Reactive extension and start coding right away.

## Examples using JBang

With [JBang](https://www.jbang.dev/) you can run one of the examples available in the catalog
without having to clone the repository or setup the project in the IDE.
Once you have downloaded JBang, the list of examples is available via: 
```
jbang alias list hibernate/hibernate-reactive
```

If you want to run one of the example (in this case the one called `example`), you can do it with:
```
jbang example@hibernate/hibernate-reactive
```

or you can [open it in your editor](https://github.com/jbangdev/jbang#editing) (IntelliJ IDEA in this case) with:
```
jbang edit --open=idea testcase@hibernate/hibernate-reactive
```

You can also generate and run a db-specific test. See available templates using: `jbang template list`

```
cockroachdb-reproducer = Template for a test with CockroachDB using Junit 4, Vert.x Unit and Testcontainers
db2-reproducer = Template for a test with Db2 using Junit 4, Vert.x Unit and Testcontainers
mariadb-reproducer = Template for a test with MariaDB using Junit 4, Vert.x Unit and Testcontainers
mysql-reproducer = Template for a test with MySQL using Junit 4, Vert.x Unit and Testcontainers
pg-reproducer = Template for a test with PostgreSQL using Junit 4, Vert.x Unit and Testcontainers
```

Example for PostgreSQL:
  - Generate java test from template: `jbang init --template=pg-reproducer pgTest.java`
  - Run the test: `jbang pgTest.java` 

## Gradle build

The project is built with Gradle, but you do _not_ need to have Gradle
installed on your machine.

### Building

To compile this project, navigate to the `hibernate-reactive` directory, 
and type:

    ./gradlew compileJava

To publish Hibernate Reactive to your local Maven repository, run:

    ./gradlew publishToMavenLocal

### Building documentation

To build the API and Reference documentation type:

    ./gradlew assembleDocumentation

You'll find the generated documentation in the subdirectory
`release/build/documentation`.

    open release/build/documentation/reference/html_single/index.html
    open release/build/documentation/javadocs/index.html

### Running tests

To run the tests, you'll need to decide which RDBMS you want to test 
with, and then get an instance of the test database running on your 
machine.

By default, the tests will be run against PostgreSQL. To test against 
MySQL, MariaDB, or Db2, you must explicitly specify `-Pdb=mysql`,
`-Pdb=maria`, or `-Pdb=db2`, for example:

    ./gradlew test -Pdb=db2
    
It's also possible to run all tests or only selected tests on
all available databases:

    ./gradlew testAll -PincludeTests=DefaultPortTest

the property `includeTests` represents the name of the test to run
and can contain the wildcard '*'. The property is optional but
running all tests on all databases might take a lot of time.

You can also enable/disable logging standard output streams for your
tests by adding the following property:

    -PshowStandardOutput

There are three ways to start the test database.
    
#### If you have Docker installed

If you have Docker installed, running the tests is really easy. You
don't need to create the test databases manually. Just type:

    ./gradlew test -Pdocker

The above command will start a PostgreSQL instance, which is the default target.

Or:

    ./gradlew test -Pdocker -Pdb=mysql

Or:

    ./gradlew test -Pdocker -Pdb=maria

Or:

    ./gradlew test -Pdocker -Pdb=db2

Or:

    ./gradlew test -Pdocker -Pdb=oracle

The tests will run faster if you reuse the same containers across 
multiple test runs. To do this, edit the testcontainers configuration 
file `.testcontainers.properties` in your home directory, adding the 
line `testcontainers.reuse.enable=true`. (Just create the file if it 
doesn't already exist.)

#### If you already have PostgreSQL installed

If you already have PostgreSQL installed on your machine, you'll just 
need to create the test database. From the command line, type the 
following commands:

    psql
    create database hreact;
    create user hreact with password 'hreact';
    grant all privileges on database hreact to hreact;
    alter user hreact createdb;

Then run `./gradlew test` from the `hibernate-reactive` directory.

#### If you already have MySQL installed

If you have MySQL installed, you can create the test database using 
the following commands:

    mysql -uroot
    create database hreact;
    create user hreact identified by 'hreact';
    grant all on hreact.* to hreact;

Then run `./gradlew test -Pdb=mysql` from the `hibernate-reactive` 
directory.

#### If you have Podman

If you have [Podman][podman] installed, you can start the test
database by following the instructions in [podman.md](podman.md).

[podman]: https://podman.io

## Limitations

We're working hard to support the full feature set of Hibernate ORM. 
At present several minor limitations remain.

- The annotation `@org.hibernate.annotations.Source` for 
  database-generated `@Version` properties is not yet supported.
- The annotation `@org.hibernate.annotations.CollectionId` is not yet 
  supported.
- With Db2:
  * [Automatic schema](http://hibernate.org/reactive/documentation/1.1/reference/html_single/#_automatic_schema_export) update and validation is not supported.
  * `@Lob` annotation is not supported - See [this issue on the vertx-db2-client](https://github.com/eclipse-vertx/vertx-sql-client/issues/496)
