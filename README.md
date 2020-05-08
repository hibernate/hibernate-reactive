![Hibernate logo][]

[![GitHub Actions Status](<https://img.shields.io/github/workflow/status/hibernate/hibernate-rx/Gradle%20Build%20and%20Test?logo=GitHub>)](https://github.com/hibernate/hibernate-rx/actions?query=workflow%3A%22Gradle+Build+and+Test%22)
[![License](https://img.shields.io/badge/License-LGPL%202.1-green.svg)](https://www.gnu.org/licenses/old-licenses/lgpl-2.1.txt)

# Hibernate RX

A reactive API for Hibernate ORM, supporting non-blocking database
drivers and a reactive style of interaction with the database.

Hibernate RX may be used in any plain Java program, but is especially
targeted toward usage in reactive environments like [Quarkus][] and 
[Vert.x][].

Currently [PostgreSQL][] and [MySQL][] are supported.

[Quarkus]: https://quarkus.io
[Vert.x]: https://vertx.io
[PostgreSQL]: https://www.postgresql.org
[MySQL]: https://www.mysql.com

[Hibernate logo]: http://static.jboss.org/hibernate/images/hibernate_logo_whitebkg_200px.png

_This project is still at an experimental stage of development._

## Example program

There is a very simple example program in the [`example`][example] 
directory.

[example]: https://github.com/hibernate/hibernate-rx/tree/master/example 

## Gradle build

The project is built with Gradle, but you do _not_ need to have Gradle
installed on your machine.

### Building

To compile this project, navigate to the `hibernate-rx` directory, and 
type:

	./gradlew compileJava

To publish Hibernate RX to your local Maven repository, run:

	./gradlew publishToMavenLocal

### Running tests

To run the tests, you'll need to get the test databases running on your 
machine. There are three ways to start the test databases. 

#### If you have Docker installed

If you have Docker installed, running the tests is really easy. You
don't need to create the test databases manually. Just type:

    ./gradlew test -Pdocker
    
The tests will run faster if you reuse the same containers across 
multiple test runs. To do this, set `testcontainers.reuse.enable=true` in 
the file `$HOME/.testcontainers.properties`. (Just create the file if it 
doesn't already exist.)

#### If you already have PostgreSQL installed

If you already have PostgreSQL installed on your machine, you'll just 
need to create the test database. From the command line, type the 
following commands:

	psql
	create database "hibernate-rx";
	create user "hibernate-rx" with password 'hibernate-rx';
	grant all privileges on database "hibernate-rx" to "hibernate-rx";

There are also tests for MySQL, so if you also have MySQL installed, 
you can run these tests as well. Create the test database using the 
following commands:

    mysql -uroot
    create database `hibernate-rx`;
    create user `hibernate-rx` identified by 'hibernate-rx';
    grant all on `hibernate-rx`.* to `hibernate-rx`;
    
Finally, run `./gradlew test` from the `hibernate-rx` directory.

#### If you have Podman

If you use [Podman][], you can start the test database by following 
the instructions in [podman.md](podman.md).

[Podman]: https://podman.io

To run the tests, type `./gradlew test` from the `hibernate-rx` directory.

## Compatibility

The project has been tested with:

- Java 8
- PostgreSQL 12
- MySQL 8
- [Hibernate ORM](https://hibernate.org/orm/) 5.4.11-SNAPSHOT
- [Vert.x Reactive PostgreSQL Client](https://vertx.io/docs/vertx-pg-client/java/) 3.9.0
- [Vert.x Reactive MySQL Client](https://vertx.io/docs/vertx-mysql-client/java/) 3.9.0

## Usage

Usage is very straightforward for anyone with any prior experience with
Hibernate or JPA. 

### Including Hibernate RX in your project

Add the following dependency to your project:

	org.hibernate.rx:hibernate-rx-core:1.0.0-SNAPSHOT

You'll also need to add your Hibernate 5.4 snapshot, the Vert.x 
reactive database driver, and a regular JDBC driver (which is 
used for schema export).

There is an example Gradle build included in the example program.

### Mapping entity classes

Use the regular JPA mapping annotations defined in the package 
`javax.persistence`, and/or the Hibernate mapping annotations in
`org.hibernate.annotations`.

Most mapping annotations are already supported in Hibernate RX. The
annotations which are not yet supported are listed in _Limitations_,
below.

### Configuration

Configuration is completely transparent; configure Hibernate 
exactly as you normally would, for example by providing a
`META-INF/persistence.xml` file.

An example [`persistence.xml`][xml] file is included in the example 
program.

[xml]: https://github.com/hibernate/hibernate-rx/blob/master/example/src/main/resources/META-INF/persistence.xml

### Obtaining a reactive session factory

Obtain a Hibernate `SessionFactory` or JPA `EntityManagerFactory` 
just as you normally would, for example, by calling:

	EntityManagerFactory emf = createEntityManagerFactory("example");

 Now, `unwrap()` the `RxSessionFactory`:
 
	RxSessionFactory sessionFactory = emf.unwrap(RxSessionFactory.class);

### Obtaining a reactive session

To obtain an `RxSession` from the `RxSessionFactory`, use `openRxSession()`:

	RxSession session = sessionFactory.openRxSession();

### Using the reactive session

The `RxSession` interface has methods with the same names as methods of the
JPA `EntityManager`. However, each of these methods returns its result via
a `CompletionStage`, for example:

	session1.persist(book)
		.thenCompose( $ -> session1.flush() )
		.thenAccept( $ -> session1.close() )

Note that `find()` also wraps its result in `Optional`:

	session2.find(Book.class, book.id)
		.thenApply(Optional::get)
		.thenAccept( bOOk -> System.out.println(bOOk.title + " is a great book!") )
		.thenAccept( $ -> session2.close() )

## Limitations

At this time, Hibernate RX does _not_ support the following features:

- transactions
- pessimistic locking via `LockMode`
- `@ElementCollection` and `@ManyToMany`
- `@OneToMany` without `mappedBy` 
- transparent lazy loading
- JPA's `@NamedEntityGraph`
- eager select fetching, for example `@ManyToOne(fetch=EAGER) @Fetch(SELECT)`
  (eager join fetching *is* supported)
- optimizers for `SEQUENCE` and `TABLE` id generation
- criteria queries

Instead, use the following supported features:

- optimistic locking with `@Version`
- `@OneToMany(mappedBy=...)` together with `@ManyToOne`
- explicit lazy loading via `RxSession.fetch(entity.association)`, which 
  returns a `CompletionStage`
- `@FetchProfile`

Note that you should not use Hibernate RX with a second-level cache 
implementation which performs blocking IO, for example passivation to the
filesystem or distributed replication.
