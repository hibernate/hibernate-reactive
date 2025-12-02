/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.List;

import org.hibernate.annotations.Struct;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.reactive.logging.impl.Log;
import org.hibernate.reactive.logging.impl.LoggerFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.COCKROACHDB;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Timeout(value = 10, timeUnit = MINUTES)
@DisabledFor(value = ORACLE, reason = "see issue https://github.com/hibernate/hibernate-reactive/issues/1855")
@DisabledFor(value = {SQLSERVER, MYSQL,	MARIA, COCKROACHDB}, reason = "ORM does not support @Struct for these databases")
public class StructComponentTest extends BaseReactiveTest {

	private static final Log LOG = LoggerFactory.make( Log.class, MethodHandles.lookup() );

	static Book book = createBook();
	static Publisher ePublisher;
	static Publisher pPublisher;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class );
	}

	private static Book createBook() {
		ePublisher = new Publisher();
		ePublisher.setName( "ebooks" );
		ePublisher.setPubId( 5 );

		pPublisher = new Publisher();
		pPublisher.setName( "paperbooks" );
		pPublisher.setPubId( 25 );

		Book book = new Book();
		book.title = "Hibernate";
		book.author = "Steve";
		book.ebookPublisher = ePublisher;
		book.paperBackPublisher = pPublisher;
		return book;
	}

	@BeforeEach
	public void populateDB(VertxTestContext context) {
		test( context, getSessionFactory()
				.withTransaction( session -> session.persist( book )
						.thenCompose( v -> session.flush() ) )
		);
	}

	@Test
	public void testStructComponent(VertxTestContext context) {
		test( context, openSession()
				.thenCompose( s2 -> s2.find( Book.class, book.id ) )
				.thenAccept( resultBook -> {
					assertNotNull( resultBook );
					assertEquals( book.title, resultBook.title );
					assertEquals( book.ebookPublisher.pubId, resultBook.ebookPublisher.pubId );
					assertEquals( book.paperBackPublisher.pubId, resultBook.paperBackPublisher.pubId );
				} )
		);
	}

	@Entity(name = "Book")
	public static class Book {

		@Id
		@GeneratedValue
		private Long id;

		private String title;

		private String author;

		@Column(name = "ebook_publisher")
		private Publisher ebookPublisher;
		private Publisher paperBackPublisher;
	}

	@Embeddable
	@Struct( name = "publisher_type")
	public static class Publisher {

		private String name;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		private Integer pubId;

		public Integer getPubId() {
			return pubId;
		}

		public void setPubId(Integer pubId) {
			this.pubId = pubId;
		}
	}
}
