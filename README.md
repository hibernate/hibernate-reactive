![Hibernate logo][]

[![GitHub Actions Status](<https://img.shields.io/github/workflow/status/hibernate/hibernate-reactive/Gradle%20Build%20and%20Test?logo=GitHub>)](https://github.com/hibernate/hibernate-reactive/actions?query=workflow%3A%22Gradle+Build+and+Test%22)
[![License](https://img.shields.io/badge/License-LGPL%202.1-green.svg)](https://www.gnu.org/licenses/old-licenses/lgpl-2.1.txt)

# Hibernate Reactive

A reactive API for Hibernate ORM, supporting non-blocking database
drivers and a reactive style of interaction with the database.

Hibernate Reactive may be used in any plain Java program, but is 
especially targeted toward usage in reactive environments like 
[Quarkus][] and [Vert.x][].

[Quarkus]: https://quarkus.io
[Vert.x]: https://vertx.io
[PostgreSQL]: https://www.postgresql.org
[MySQL]: https://www.mysql.com

[Hibernate logo]: http://static.jboss.org/hibernate/images/hibernate_logo_whitebkg_200px.png

## Compatibility

Currently [PostgreSQL][] and [MySQL][] are supported.

Hibernate Reactive has been tested with:

- Java 8
- PostgreSQL 12
- MySQL 8
- [Hibernate ORM](https://hibernate.org/orm/) 5.4.16.Final
- [Vert.x Reactive PostgreSQL Client](https://vertx.io/docs/vertx-pg-client/java/) 3.9.0
- [Vert.x Reactive MySQL Client](https://vertx.io/docs/vertx-mysql-client/java/) 3.9.0

Support for DB2 and SQL Server is coming soon.

Integration with Quarkus has been developed and tested but has not yet 
been released.

## Usage

Usage is very straightforward for anyone with any prior experience with
Hibernate or JPA. 

### Including Hibernate Reactive in your project

Add the following dependency to your project:

    org.hibernate.reactive:hibernate-reactive-core:1.0.0-SNAPSHOT

You'll also need to add dependencies to:

- Hibernate ORM, `org.hibernate:hibernate-core`, and
- the Vert.x reactive database driver for your database, for example,
  `io.vertx:vertx-pg-client` or `io.vertx:vertx-mysql-client`.

There's an example Gradle [build][] included in the example program.

[build]: https://github.com/hibernate/hibernate-reactive/blob/master/example/build.gradle

### Mapping entity classes

Use the regular JPA mapping annotations defined in the package 
`javax.persistence`, and/or the Hibernate mapping annotations in
`org.hibernate.annotations`.

Most mapping annotations are already supported in Hibernate Reactive. 
The annotations which are not yet supported are listed in _Limitations_,
below.

### Configuration

Configuration is completely transparent; configure Hibernate 
exactly as you normally would, for example by providing a
`META-INF/persistence.xml` file.

An example [`persistence.xml`][xml] file is included in the example 
program.

[xml]: https://github.com/hibernate/hibernate-reactive/blob/master/example/src/main/resources/META-INF/persistence.xml

### Obtaining a reactive session factory

Obtain a Hibernate `SessionFactory` or JPA `EntityManagerFactory` 
just as you normally would, for example, by calling:

    EntityManagerFactory emf = createEntityManagerFactory("example");

 Now, `unwrap()` the reactive `SessionFactory`:
 
    Stage.SessionFactory sessionFactory = emf.unwrap(Stage.SessionFactory.class);

### Obtaining a reactive session

To obtain a reactive `Session` from the `SessionFactory`, use `openReactiveSession()`:

    Stage.Session session = sessionFactory.openReactiveSession();

### Using the reactive session

The `Session` interface has methods with the same names as methods of the
JPA `EntityManager`. However, each of these methods returns its result via
a `CompletionStage`, for example:

    session1.find(Book.class, book.id)
            .thenAccept( bOOk -> System.out.println(bOOk.title + " is a great book!") )
            .thenAccept( $ -> session1.close() )

Methods with no meaningful return value return a reference to the `Session`:
    
    session2.persist(book)
            .thenCompose( $ -> session2.flush() )
            .thenAccept( $ -> session2.close() )

That `createQuery()` method produces a reactive `Query`, allowing HQL / JPQL 
queries to be executed asynchronously, always returning their results via a 
`CompletionStage`:

    session3.createQuery("select title from Book order by title desc")
            .getResultList()
            .thenAccept(System.out::println)
            .thenAccept( $ -> session3.close() )

If you already know Hibernate, and if you already have some experience with 
reactive programming, there's not much new to learn here: you should 
immediately feel right at home. 

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
    create database "hreactive";
    create user "hreactive" with password 'hreactive';
    grant all privileges on database "hreactive" to "hreactive";

There are also tests for MySQL, so if you also have MySQL installed, 
you can run these tests as well. Create the test database using the 
following commands:

    mysql -uroot
    create database `hreactive`;
    create user `hreactive` identified by 'hreactive';
    grant all on `hreactive`.* to `hreactive`;
    
Finally, run `./gradlew test` from the `hibernate-reactive` directory.

#### If you have Podman

If you use [Podman][], you can start the test database by following 
the instructions in [podman.md](podman.md).

[Podman]: https://podman.io

To run the tests, type `./gradlew test` from the `hibernate-reactive` 
directory.

## Limitations

At this time, Hibernate Reactive does _not_ support the following features:

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
- native SQL queries

Instead, use the following supported features:

- optimistic locking with `@Version`
- `@OneToMany(mappedBy=...)` together with `@ManyToOne`
- explicit lazy loading via `Session.fetch(entity.association)`, which 
  returns a `CompletionStage`
- `@FetchProfile`
- HQL queries

Note that you should not use Hibernate Reactive with a second-level cache 
implementation which performs blocking IO, for example passivation to the
filesystem or distributed replication.
