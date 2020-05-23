![Hibernate logo][]

[![CI Status](https://github.com/hibernate/hibernate-reactive/workflows/Hibernate%20Reactive%20CI/badge.svg)](https://github.com/hibernate/hibernate-reactive/actions?query=workflow%3A%22Hibernate+Reactive+CI%22)
[![License](https://img.shields.io/badge/License-LGPL%202.1-green.svg)](https://www.gnu.org/licenses/old-licenses/lgpl-2.1.txt)

# Hibernate Reactive

A reactive API for Hibernate ORM, supporting non-blocking database
drivers and a reactive style of interaction with the database.

Hibernate Reactive may be used in any plain Java program, but is 
especially targeted toward usage in reactive environments like 
[Quarkus][] and [Vert.x][].

[Quarkus]: https://quarkus.io
[Vert.x]: https://vertx.io

[Hibernate logo]: http://static.jboss.org/hibernate/images/hibernate_logo_whitebkg_200px.png

## Compatibility

Currently [PostgreSQL][], [MySQL][], and [DB2][] are supported.

Hibernate Reactive has been tested with:

- Java 8
- PostgreSQL 12
- MySQL 8
- DB2 11.5
- [Hibernate ORM](https://hibernate.org/orm/) 5.4.16.Final
- [Vert.x Reactive PostgreSQL Client](https://vertx.io/docs/vertx-pg-client/java/) 3.9.1
- [Vert.x Reactive MySQL Client](https://vertx.io/docs/vertx-mysql-client/java/) 3.9.1
- Vert.x Reactive DB2 Client 3.9.1

Support for SQL Server is coming soon.

Integration with Quarkus has been developed and tested but has not yet 
been released.

[PostgreSQL]: https://www.postgresql.org
[MySQL]: https://www.mysql.com
[DB2]: https://www.ibm.com/analytics/db2

## Usage

Usage is very straightforward for anyone with any prior experience with
Hibernate or JPA. 

### Including Hibernate Reactive in your project

Add the following dependency to your project:

    org.hibernate.reactive:hibernate-reactive-core:1.0.0-SNAPSHOT

You'll also need to add a dependency for the Vert.x reactive database 
driver for your database, for example:

- `io.vertx:vertx-pg-client` for Postgres,
- `io.vertx:vertx-mysql-client` for MySQL, or
- `io.vertx:vertx-db2-client` for DB2.

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

Configuration properties of particular interest include:

- `javax.persistence.jdbc.url`, the JDBC URL of your database,
- `javax.persistence.jdbc.user` and `javax.persistence.jdbc.password`,
  the database credentials, and
- `hibernate.connection.pool_size`, the size of the Vert.x reactive
  connection pool.

An example [`persistence.xml`][xml] file is included in the example 
program.

[xml]: https://github.com/hibernate/hibernate-reactive/blob/master/example/src/main/resources/META-INF/persistence.xml

### Obtaining a reactive session factory

Obtain a Hibernate `SessionFactory` or JPA `EntityManagerFactory` 
just as you normally would, for example, by calling:

    EntityManagerFactory emf = Persistence.createEntityManagerFactory("example");

Now, `unwrap()` the reactive `SessionFactory`:
 
    Stage.SessionFactory sessionFactory = emf.unwrap(Stage.SessionFactory.class);

The type `Stage.SessionFactory` gives access to reactive APIs based on 
Java's `CompletionStage`.

If you prefer to use the [Mutiny][]-based API, `unwrap()` the type 
`Mutiny.SessionFactory`:

    Mutiny.SessionFactory sessionFactory = emf.unwrap(Mutiny.SessionFactory.class);

[Mutiny]: https://smallrye.io/smallrye-mutiny/

### Obtaining a reactive session

To obtain a reactive `Session` from the `SessionFactory`, use `withSession()`:

    sessionFactory.withSession(
            session -> ... //do some work
    );

Alternatively, you can use `openSession()`, but you must remember to `close()` 
the session when you're done.

    sessionFactory.openSession()
            .thenCompose(
                session -> ... //do some work
                        .thenAccept( $ -> session.close() ) 
            );

### Using the reactive session

The `Session` interface has methods with the same names as methods of the
JPA `EntityManager`. However, each of these methods returns its result via
a `CompletionStage` (or Mutiny `Uni`), for example:

    session1.find(Book.class, book.id)
            .thenAccept( book -> System.out.println(book.title + " is a great book!") )

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

If you already know Hibernate, and if you already have some experience with 
reactive programming, there's not much new to learn here: you should 
immediately feel right at home. 

### Fetching lazy associations

In Hibernate ORM, lazy associations are fetched transparently when the
association is fist accessed within a session. In Hibernate Reactive, 
association fetching is an asynchronous process that produces a result
via a `CompletionStage` (or Mutiny `Uni`).

Therefore, lazy fetching is an explicit operation named `fetch()`,
a static method of `Stage` and `Mutiny`:

    session4.find(Author.class, author.id)
            .thenCompose( author -> Stage.fetch(author.books) )
            .thenAccept( books -> ... )

Of course, this isn't necessary if you fetch the association eagerly.

### Transactions

The `withTransaction()` method performs work within the scope of a database 
transaction. 

    session.withTransaction( tx -> session.persist(book) )

The session is automatically flushed at the end of the transaction.

### Identifier generation

Sequence, table, and `UUID` id generation is built in, along with support
for assigned ids. 

Custom id generators may be defined by implementing `ReactiveIdentifierGenerator` 
and declaring the custom implementation using `@GenericGenerator`.

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

By default (almost) all of the tests will run with PostgreSQL. To use a different database
such as DB2, you can specify `-Pdb=<DBType>`, for example:

    ./gradlew test -Pdb=db2
    
Possible values are (case insensitive): `db2`, `mysql`, `pg` (or `postgresql`)
    
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
    create database hreact;
    create user hreact with password 'hreact';
    grant all privileges on database hreact to hreact;

There are also tests for MySQL, so if you also have MySQL installed, 
you can run these tests as well. Create the test database using the 
following commands:

    mysql -uroot
    create database hreact;
    create user hreact identified by 'hreact';
    grant all on hreact.* to hreact;

Finally, run `./gradlew test` from the `hibernate-reactive` directory.

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

Instead, use `@OneToMany(mappedBy=...)` together with `@ManyToOne` for all
associations.

#### Eager association fetching

Hibernate Reactive does not support eager association fetching via subsequent 
SQL select, for example, `@ManyToOne(fetch=EAGER) @Fetch(SELECT)`
or `@ManyToOne(fetch=EAGER) @Fetch(SUBSELECT)`. So you must choose between:

- lazy fetching, for example, `@ManyToOne @Fetch(SELECT)` or
  `@ManyToOne @Fetch(SUBSELECT)`, or
- eager fetching via outer join, `@ManyToOne(fetch=EAGER)`.

As usual, we recommend that all association mappings be declared lazy.

#### Criteria queries

Currently there is no support for criteria queries. Use HQL or native SQL 
instead. If you're concerned about type safety, check out the Hibernate 
[Query Validator][].

[Query Validator]: https://github.com/hibernate/query-validator/

#### Fetch profiles

JPA `@NamedEntityGraph`s are not supported, but Hibernate `@FetchProfile`s
_are_.

#### Identifier generation

There is no block optimization for the `SEQUENCE` and `TABLE` id generators.

#### Caching

Note that you should not use Hibernate Reactive with a second-level cache 
implementation which performs blocking IO, for example passivation to the
filesystem or distributed replication.
