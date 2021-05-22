/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */

///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS junit:junit:4.12
//DEPS javax.persistence:javax.persistence-api:2.2
//DEPS org.hibernate.reactive:hibernate-reactive-core:1.0.0.CR4
//DEPS org.hibernate.validator:hibernate-validator:6.1.5.Final
//DEPS org.assertj:assertj-core:3.13.2
//DEPS io.vertx:vertx-pg-client:4.0.3
//DEPS io.vertx:vertx-mysql-client:4.0.3
//DEPS io.vertx:vertx-db2-client:4.0.3
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

/**
 * JBang compatible example for Hibernate Reactive.
 * <p>
 * We expect the selected database (the default is PostgreSQL)
 * to be up and running. Instructions on how to start the databases
 * using Podman or Docker are in the
 * <a hreaf="https://github.com/hibernate/hibernate-reactive/blob/main/podman.md">podman.md</a>
 * file in the Hibernate Reactive project on GitHub.
 * </p>
 * <p>
 * Usage example:
 *     <dl>
 *         <dt>1. Download JBang</dt>
 *         <dd>See <a hreaf="https://www.jbang.dev/download">https://www.jbang.dev/download</a></dd>
 *         <dt>2. Start the database with the right credentials</dt>
 *         <dd>
 *             Using Podman or Docker (you can replace {@code podman} with {@code docker}):
 *             <pre>
 * podman run --rm --name HibernateTestingPGSQL \
 *     -e POSTGRES_USER=hreact -e POSTGRES_PASSWORD=hreact -e POSTGRES_DB=hreact \
 *     -p 5432:5432 postgres:13.2
 *             </pre>
 *         </dd>
 *         <dt>3. Run the test with JBang</dt>
 *         <dd>
 *             <pre>jbang Example.java</pre>
 *         </dd>
 *         <dt>4. (Optional) Edit the file (with IntelliJ IDEA for example):</dt>
 *         <dd>
 *             <pre>jbang edit --open=idea Example.java</pre>
 *         </dd>
 *     </dl>
 * <p/>
 *
 * @see <a href="https://www.jbang.dev/">JBang</a>
 */
public class Example {

	/**
	 * The default URLs for the supported databases
	 */
	enum Database {
		POSTGRESQL( "jdbc:postgresql://localhost:5432/hreact?user=hreact&password=hreact" ),
		MYSQL( "jdbc:mysql://localhost:3306/hreact?user=hreact&password=hreact&serverTimezone=UTC" ),
		MARIADB( "jdbc:mariadb://localhost:3306/hreact?user=hreact&password=hreact&serverTimezone=UTC" ),
		DB2( "jdbc:db2://localhost:50000/hreact:user=hreact;password=hreact;" ),
		COCKROACHDB( "jdbc:cockroachdb://localhost:26257/postgres?sslmode=disable&user=root" );

		private String url;

		Database(String url) {
			this.url = url;
		}

		public String getUrl() {
			return url;
		}
	}

	/**
	 * Create the {@link Configuration} for the {@link Mutiny.SessionFactory}.
	 */
	private static Configuration createConfiguration() {
		Configuration configuration = new Configuration();

		configuration.setProperty( Settings.URL, Database.MARIADB.getUrl() );

		// (Optional) Override default credentials
		// configuration.setProperty( Settings.USER, "hreact" );
		// configuration.setProperty( Settings.PASS, "hreact" );

		// Supported values are: none, create, create-drop
		configuration.setProperty( Settings.HBM2DDL_AUTO, "create" );

		// Register new entities here
		configuration.addAnnotatedClass( Author.class );
		configuration.addAnnotatedClass( Book.class );

		// Query logging
		configuration.setProperty( Settings.SHOW_SQL, "false" );
		configuration.setProperty( Settings.HIGHLIGHT_SQL, "false" );
		configuration.setProperty( Settings.FORMAT_SQL, "false" );
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
					// wait for it to finish
					.await().indefinitely();
		}
	}

	/**
	 * Example of a class representing an entity.
	 * <p>
	 * If you create new entities, be sure to add them in {@link #createConfiguration()}.
	 * For example:
	 * <pre>
	 * configuration.addAnnotatedClass( MyOtherEntity.class );
	 * </pre>
	 */
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
