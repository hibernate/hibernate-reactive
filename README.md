# Hibernate RX

A non-blocking API for Hibernate ORM, supporting a reactive style of
interaction with the database.

_This project is still at an experimental stage._

## Building

To compile run `./gradlew build` from this directory.

## Running tests

To run the tests, ensure that PostgreSQL is installed on your machine.
From the command line, type the following commands

    createdb hibernate-rx
    createuser hibernate-rx
    psql
    grant all privileges on database "hibernate-rx" to "hibernate-rx";

Finally, run `./gradlew test` from this directory.

## Dependencies

The project has been tested with

- Java 8
- PostgreSQL
- [Hibernate ORM](https://hibernate.org/orm/) 5.4.10.Final
- [SmallRye PostgreSQL Client](https://github.com/smallrye/smallrye-reactive-utils) 0.0.12

## Usage

Add the following dependency to your project:

    org.hibernate.rx:hibernate-rx-core:1.0.0-SNAPSHOT

To obtain an `RxSession` from a Hibernate `SessionFactory`, use:

    RxSession session = sessionFactory.unwrap(RxHibernateSessionFactory.class).openRxSession();

## Limitations

At this time, Hibernate RX does not support the following functionality:

- transactions
- collections, except for `@ManyToOne` associations
- transparent lazy loading: lazy fetching may be requested explicitly 
   using `session.fetch(entity.association)`, which returns a
   `CompletionStage`.
- JPA's `@NamedEntityGraph`s: use `@FetchProfile` instead
- criteria queries

Currently only PostgreSQL is supported. Support for MySQL is coming soon!
