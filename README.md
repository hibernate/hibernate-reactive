![Hibernate logo][]

[![CI Status](https://github.com/hibernate/hibernate-reactive/workflows/Hibernate%20Reactive%20CI/badge.svg)](https://github.com/hibernate/hibernate-reactive/actions?query=workflow%3A%22Hibernate+Reactive+CI%22)
[![License](https://img.shields.io/badge/License-LGPL%202.1-green.svg)](https://opensource.org/licenses/LGPL-2.1)
[![Download](https://api.bintray.com/packages/hibernate/artifacts/hibernate-reactive/images/download.svg)](https://bintray.com/hibernate/artifacts/hibernate-reactive/_latestVersion)

[Hibernate logo]: http://static.jboss.org/hibernate/images/hibernate_logo_whitebkg_200px.png

# Hibernate Reactive

A reactive API for Hibernate ORM, supporting non-blocking database
drivers and a reactive style of interaction with the database.

Hibernate Reactive may be used in any plain Java program, but is 
especially targeted toward usage in reactive environments like 
[Quarkus][] and [Vert.x][].

Currently [PostgreSQL][], [MySQL][], and [DB2][] are supported.

[Quarkus]: https://quarkus.io
[Vert.x]: https://vertx.io

## Compatibility

Hibernate Reactive has been tested with:

- Java 8
- PostgreSQL 12
- MySQL 8
- DB2 11.5
- [Hibernate ORM](https://hibernate.org/orm/) 5.4.21.Final
- [Vert.x Reactive PostgreSQL Client](https://vertx.io/docs/vertx-pg-client/java/) 3.9.2
- [Vert.x Reactive MySQL Client](https://vertx.io/docs/vertx-mysql-client/java/) 3.9.2
- [Vert.x Reactive DB2 Client](https://vertx.io/docs/vertx-db2-client/java/) 3.9.2

Support for SQL Server is coming soon.

Integration with Quarkus has been developed and tested but has not yet 
been released.

[PostgreSQL]: https://www.postgresql.org
[MySQL]: https://www.mysql.com
[DB2]: https://www.ibm.com/analytics/db2

## Documentation

The [Introduction to Hibernate Reactive][introduction] covers everything 
you need to know to get started, including:

- [setting up a project][build] that uses Hibernate Reactive and the Vert.x 
  reactive SQL client for your database,
- [configuring][config] Hibernate Reactive to access your database,
- writing Java code to [define the entities][model] of your data model, and 
- writing reactive data access code [using a reactive session][session].

We recommend you start there!

[introduction]: https://github.com/hibernate/hibernate-reactive/blob/master/documentation/src/main/asciidoc/reference/introduction.adoc

[build]: https://github.com/hibernate/hibernate-reactive/blob/master/documentation/src/main/asciidoc/reference/introduction.adoc#including-hibernate-reactive-in-your-project-build
[config]: https://github.com/hibernate/hibernate-reactive/blob/master/documentation/src/main/asciidoc/reference/introduction.adoc#configuration
[model]: https://github.com/hibernate/hibernate-reactive/blob/master/documentation/src/main/asciidoc/reference/introduction.adoc#mapping-entity-classes
[session]: https://github.com/hibernate/hibernate-reactive/blob/master/documentation/src/main/asciidoc/reference/introduction.adoc#using-the-reactive-session

## Example program

There is a very simple example program in the [`example`][example] 
directory.

[example]: https://github.com/hibernate/hibernate-reactive/tree/master/example

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
MySQL or DB2, you must explicitly specify `-Pdb=mysql` or `-Pdb=db2`, 
for example:

    ./gradlew test -Pdb=db2
    
There are three ways to start the test database.
    
#### If you have Docker installed

If you have Docker installed, running the tests is really easy. You
don't need to create the test databases manually. Just type:

    ./gradlew test -Pdocker

Or:

    ./gradlew test -Pdocker -Pdb=mysql

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

If you use [Podman][], you can start the test database by following 
the instructions in [podman.md](podman.md).

[Podman]: https://podman.io

To run the tests, type `./gradlew test` from the `hibernate-reactive` 
directory.

## Limitations

We're working hard to support the full feature set of Hibernate ORM. At
present a few limitations remain.

#### Association mappings

At this time, Hibernate Reactive does not support the following mapping
features:

- `@ElementCollection`s,
- `@ManyToMany` associations, and
- one-sided `@OneToMany` associations without `mappedBy`.

Instead, use `@OneToMany(mappedBy=...)` together with `@ManyToOne` for 
all associations.

#### Query language

HQL `update` and `delete` queries which affect multiple tables (due to the
use of `TABLE_PER_CLASS` or `JOINED` inheritance mapping) are not working.

#### Caching

The query cache is not yet supported.

Note that you should not use Hibernate Reactive with a second-level cache 
implementation which performs blocking IO, for example passivation to the
filesystem or distributed replication.

#### Driver-specific limitations

You might run into some [limitations of the Vert.x DB2 client][] when using 
Hibernate Reactive with DB2.

[limitations of the Vert.x DB2 client]: https://github.com/eclipse-vertx/vertx-sql-client/blob/master/vertx-db2-client/src/main/asciidoc/index.adoc#reactive-db2-client

