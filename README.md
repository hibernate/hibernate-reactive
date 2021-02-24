
![Hibernate logo][]

[![CI Status](https://github.com/hibernate/hibernate-reactive/workflows/Hibernate%20Reactive%20CI/badge.svg)](https://github.com/hibernate/hibernate-reactive/actions?query=workflow%3A%22Hibernate+Reactive+CI%22)
[![License](https://img.shields.io/badge/License-LGPL%202.1-green.svg)](https://opensource.org/licenses/LGPL-2.1)
[![Download](https://api.bintray.com/packages/hibernate/artifacts/hibernate-reactive/images/download.svg)](https://bintray.com/hibernate/artifacts/hibernate-reactive/_latestVersion)

[Hibernate logo]: http://static.jboss.org/hibernate/images/hibernate_logo_whitebkg_200px.png

# Hibernate Reactive

A reactive API for [Hibernate ORM][], supporting non-blocking database
drivers and a reactive style of interaction with the database.

Hibernate Reactive may be used in any plain Java program, but is 
especially targeted toward usage in reactive environments like 
[Quarkus][] and [Vert.x][].

Currently [PostgreSQL][], [MySQL][], [MariaDB][], and [Db2][] are 
supported.

Learn more at <http://hibernate.org/reactive>.

[Hibernate ORM]: https://hibernate.org/orm/
[Quarkus]: https://quarkus.io
[Quarkus quickstarts]: https://github.com/quarkusio/quarkus-quickstarts
[Vert.x]: https://vertx.io

## Compatibility

Hibernate Reactive has been tested with:

- Java 8
- PostgreSQL 13
- MySQL 8
- MariaDB 10
- Db2 11.5
- [Hibernate ORM][] 5.4.28.Final
- [Vert.x Reactive PostgreSQL Client](https://vertx.io/docs/vertx-pg-client/java/) 4.0.2
- [Vert.x Reactive MySQL Client](https://vertx.io/docs/vertx-mysql-client/java/) 4.0.2
- [Vert.x Reactive Db2 Client](https://vertx.io/docs/vertx-db2-client/java/) 4.0.2
- [Quarkus][Quarkus] via the Hibernate Reactive extension

Support for SQL Server is coming soon.

[PostgreSQL]: https://www.postgresql.org
[MySQL]: https://www.mysql.com
[MariaDB]: https://mariadb.com
[DB2]: https://www.ibm.com/analytics/db2

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

[introduction]: https://github.com/hibernate/hibernate-reactive/blob/master/documentation/src/main/asciidoc/reference/introduction.adoc

[build]: https://github.com/hibernate/hibernate-reactive/blob/master/documentation/src/main/asciidoc/reference/introduction.adoc#including-hibernate-reactive-in-your-project-build
[config]: https://github.com/hibernate/hibernate-reactive/blob/master/documentation/src/main/asciidoc/reference/introduction.adoc#basic-configuration
[model]: https://github.com/hibernate/hibernate-reactive/blob/master/documentation/src/main/asciidoc/reference/introduction.adoc#mapping-entity-classes
[session]: https://github.com/hibernate/hibernate-reactive/blob/master/documentation/src/main/asciidoc/reference/introduction.adoc#using-the-reactive-session
[performance]: https://github.com/hibernate/hibernate-reactive/blob/master/documentation/src/main/asciidoc/reference/introduction.adoc#tuning-and-performance

## Example program

There is a very simple example program in the [`example`][example] 
directory.

[example]: https://github.com/hibernate/hibernate-reactive/tree/master/example

A list of [quickstarts][Quarkus quickstarts] for Quarkus is available on GitHub:
  - [Hibernate Reactive with RESTEasy Reactive](https://github.com/quarkusio/quarkus-quickstarts/tree/master/hibernate-reactive-quickstart) quickstart
  - [Hibernate Reactive with Vert.x Web Routes](https://github.com/quarkusio/quarkus-quickstarts/tree/master/hibernate-reactive-routes-quickstart) quickstart

Or you can [generate a new Quarks project](https://code.quarkus.io/?g=org.acme&a=code-with-quarkus&v=1.0.0-SNAPSHOT&b=MAVEN&s=r1s&cn=code.quarkus.io)
that uses the Hibernate Reactive extension and start coding right away.

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
    
There are three ways to start the test database.
    
#### If you have Docker installed

If you have Docker installed, running the tests is really easy. You
don't need to create the test databases manually. Just type:

    ./gradlew test -Pdocker

Or:

    ./gradlew test -Pdocker -Pdb=mysql

Or:

    ./gradlew test -Pdocker -Pdb=maria

Or:

    ./gradlew test -Pdocker -Pdb=db2

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

- Automatic update or validation of an existing database schema require 
  use of JDBC.
- The annotation `@org.hibernate.annotations.Source` for 
  database-generated `@Version` properties is not yet supported.
- The annotation `@org.hibernate.annotations.CollectionId` is not yet 
  supported.
- There's no equivalent of `Session.byNaturalId()` for lookups based
  on fields annotated `@NaturalId`.
