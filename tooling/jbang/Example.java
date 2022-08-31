///usr/bin/env jbang "$0" "$@" ; exit $?
/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */

//DEPS io.vertx:vertx-pg-client:${vertx.version:4.3.3}
//DEPS io.vertx:vertx-mysql-client:${vertx.version:4.3.3}
//DEPS io.vertx:vertx-db2-client:${vertx.version:4.3.3}
//DEPS org.hibernate.reactive:hibernate-reactive-core-jakarta:${hibernate-reactive.version:1.1.6.Final}
//DEPS org.slf4j:slf4j-simple:1.7.30

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import jakarta.persistence.Basic;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;

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
import static jakarta.persistence.CascadeType.PERSIST;
import static jakarta.persistence.FetchType.LAZY;

/**
 * <a hreaf="https://www.jbang.dev/">JBang</a> compatible example for Hibernate Reactive.
 * <p>
 * The selected database must be available before running the example.
 * </p>
 * <p>
 * Instructions on how to start the containers using Podman or Docker are available
 * in the <a hreaf="https://github.com/hibernate/hibernate-reactive/blob/main/podman.md">podman.md</a>
 * file on GitHub.
 * </p>
 * <p>
 * Usage example:
 *     <dl>
 *         <dt>1. Download JBang</dt>
 *         <dd>See <a hreaf="https://www.jbang.dev/download">https://www.jbang.dev/download</a></dd>
 *         <dt>2. Start a database (default is PostgreSQL)</dt>
 *         <dd>
 *             <pre>
 *                 podman run --rm --name HibernateTestingPGSQL \
 *                      -e POSTGRES_USER=hreact -e POSTGRES_PASSWORD=hreact -e POSTGRES_DB=hreact \
 *                      -p 5432:5432 postgres:14
 *              </pre>
 *         </dd>
 *         <dt>3. Run the example with JBang</dt>
 *         <dd>
 *             <pre>jbang Example.java</pre>
 *         </dd>
 *         <dt>4. (Optional) Edit the file (with IntelliJ IDEA for example):</dt>
 *         <dd>
 *             <pre>jbang edit --open=idea Example.java</pre>
 *         </dd>
 *     </dl>
 * <p/>
 */
public class Example {

	/**
	 * You can change this value to select a different database
	 */
	private static final Database DATABASE = Database.POSTGRESQL;

	/**
	 * Create the {@link Configuration} for the {@link Mutiny.SessionFactory}.
	 */
	private static Configuration createConfiguration() {
		Configuration configuration = new Configuration();

		// The URL to connect to
		configuration.setProperty( Settings.URL, DATABASE.getUrl() );

		// (Optional) Override default credentials
		// configuration.setProperty( Settings.USER, "hreact" );
		// configuration.setProperty( Settings.PASS, "hreact" );

		// Schema generation. Supported values are create, drop, create-drop, drop-create, none
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
		out.println();
		out.println( "Expected database: " + DATABASE );
		out.println( "Connection URL: " + DATABASE.getUrl() );
		out.println();

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

			out.println();
			out.println( "Looking for the book \"" + book1.getTitle() + "\"..." );
			factory.withSession(
					// retrieve a Book
					session -> session.find( Book.class, book1.getId() )
							// author is a lazy association, we need to fetch it first if we want to use it
							.chain( book -> session.fetch( book.getAuthor() )
									// print its title and author
									.invoke( author -> out.println( "FOUND: \"" + book.getTitle() + "\"" + " by " + author.getName() + " is a great book!" ) )
							)
			)
					// wait for it to finish
					.await().indefinitely();
		}
	}

	/**
	 * Example of classes representing entities.
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

		private String isbn;

		private String title;

		@Basic(fetch = LAZY)
		private LocalDate published;

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

	/**
	 * Show an example of different URL strings for each supported database.
	 * <p>
	 * One way to start the selected container is by following the instructions
	 * in the <a hreaf="https://github.com/hibernate/hibernate-reactive/blob/main/podman.md">podman.md</a>
	 * file on the Hibernate Reactive repository on GitHub.
	 * </p>
	 */
	enum Database {
		POSTGRESQL( "postgresql://localhost:5432/hreact?user=hreact&password=hreact" ),
		MYSQL( "mysql://localhost:3306/hreact?user=hreact&password=hreact&serverTimezone=UTC" ),
		MARIADB( "mariadb://localhost:3306/hreact?user=hreact&password=hreact&serverTimezone=UTC" ),
		DB2( "db2://localhost:50000/hreact:user=hreact;password=hreact;" ),
		COCKROACHDB( "cockroachdb://localhost:26257/postgres?sslmode=disable&user=root" );

		private String url;

		Database(String url) {
			this.url = url;
		}

		/**
		 * The URL string on the default port.
		 *
		 * @return a URL string value for the property {@link Settings#URL}
		 */
		public String getUrl() {
			return url;
		}
	}
}
