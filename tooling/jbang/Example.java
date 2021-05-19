/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */

///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.hibernate:hibernate-core:5.4.31.Final
//DEPS junit:junit:4.12
//DEPS javax.persistence:javax.persistence-api:2.2
//DEPS org.hibernate.reactive:hibernate-reactive-core:1.0.0.CR4
//DEPS org.hibernate.validator:hibernate-validator:6.1.5.Final
//DEPS org.assertj:assertj-core:3.13.2
//DEPS io.vertx:vertx-pg-client:4.0.3
//DEPS io.vertx:vertx-db2-client:4.0.3
//DEPS io.vertx:vertx-mysql-client:4.0.3
//DEPS io.vertx:vertx-sql-client:4.0.3
//DEPS io.vertx:vertx-unit:4.0.3
//
//

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.Basic;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Past;
import javax.validation.constraints.Size;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.mutiny.Mutiny;
import org.hibernate.reactive.provider.ReactiveServiceRegistryBuilder;
import org.hibernate.reactive.provider.Settings;

import static java.lang.System.out;
import static java.time.Month.JANUARY;
import static java.time.Month.JUNE;
import static java.time.Month.MAY;
import static javax.persistence.CascadeType.PERSIST;
import static javax.persistence.FetchType.LAZY;

public class Example {

	/**
	 * Define the configuration parameter values for your use-case
	 */
	private static Configuration createConfiguration() {
		Configuration configuration = new Configuration();

		configuration.setProperty( Settings.URL, "jdbc:postgresql://localhost:5432/hreact" );

		// =====  OTHER Test DBs =======================================================================
		// MYSQL: jdbc:mysql://localhost:3306/hreact?user=hreact&password=hreact&serverTimezone=UTC
		// DB2:   jdbc:db2://localhost:50000/hreact:user=hreact;password=hreact;
		// CockroachDB  jdbc:cockroachdb://localhost:26257/postgres?sslmode=disable&user=root
		//  NOTE:  CockroachDB does not need password and requires //DEPS io.vertx:vertx-pg-client:4.0.3
		// ==============================================================================================

		configuration.setProperty( Settings.USER, "hreact" );
		configuration.setProperty( Settings.PASS, "hreact" );

		configuration.setProperty( Settings.HBM2DDL_AUTO, "create" );

		configuration.addAnnotatedClass( Author.class );
		configuration.addAnnotatedClass( Book.class );

		configuration.setProperty( Settings.SHOW_SQL, "true" );
		configuration.setProperty( Settings.HIGHLIGHT_SQL, "true" );
		configuration.setProperty( Settings.FORMAT_SQL, "true" );
		return configuration;
	}

	public static Mutiny.SessionFactory createSessionFactory() {
		Configuration configuration = createConfiguration();
		StandardServiceRegistryBuilder builder = new ReactiveServiceRegistryBuilder()
				.applySettings( configuration.getProperties() );
		StandardServiceRegistry registry = builder.build();

		return configuration.buildSessionFactory( registry )
				.unwrap( Mutiny.SessionFactory.class );
	}

	public static void main(String[] args) {
		out.println( "== Mutiny API Example ==" );

		// define some test data
		Author author1 = new Author( "Iain M. Banks" );
		Author author2 = new Author( "Neal Stephenson" );
		Book book1 = new Book( "1-85723-235-6", "Feersum Endjinn", author1, LocalDate.of( 1994, JANUARY, 1 ) );
		Book book2 = new Book( "0-380-97346-4", "Cryptonomicon", author2, LocalDate.of( 1999, MAY, 1 ) );
		Book book3 = new Book( "0-553-08853-X", "Snow Crash", author2, LocalDate.of( 1992, JUNE, 1 ) );
		author1.getBooks().add( book1 );
		author2.getBooks().add( book2 );
		author2.getBooks().add( book3 );

		try (Mutiny.SessionFactory factory = createSessionFactory()) {
			// obtain a reactive session
			factory.withTransaction(
					// persist the Authors with their Books in a transaction
					(session, tx) -> session.persistAll( author1, author2 )
			)
					// wait for it to finish
					.await().indefinitely();

			factory.withSession(
					// retrieve a Book
					session -> session.find( Book.class, book1.getId() )
							// print its title
							.invoke( book -> out.println( book.getTitle() + " is a great book!" ) )
			)
					.await().indefinitely();
		}
	}

	@Entity
	@Table(name = "authors")
	static class Author {
		@Id
		@GeneratedValue
		private Integer id;

		@NotNull
		@Size(max = 100)
		private String name;

		@OneToMany(mappedBy = "author", cascade = PERSIST)
		private List<Book> books = new ArrayList<>();

		Author(String name) {
			this.name = name;
		}

		Author() {
		}

		Integer getId() {
			return id;
		}

		String getName() {
			return name;
		}

		List<Book> getBooks() {
			return books;
		}
	}

	@Entity
	@Table(name = "books")
	static class Book {
		@Id
		@GeneratedValue
		private Integer id;

		@Size(min = 13, max = 13)
		private String isbn;

		@NotNull
		@Size(max = 100)
		private String title;

		@Basic(fetch = LAZY)
		@NotNull
		@Past
		private LocalDate published;

		@NotNull
		@ManyToOne(fetch = LAZY)
		private Author author;

		Book(String isbn, String title, Author author, LocalDate published) {
			this.title = title;
			this.isbn = isbn;
			this.author = author;
			this.published = published;
		}

		Book() {
		}

		Integer getId() {
			return id;
		}

		String getIsbn() {
			return isbn;
		}

		String getTitle() {
			return title;
		}

		Author getAuthor() {
			return author;
		}

		LocalDate getPublished() {
			return published;
		}
	}
}
