/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;
import org.hibernate.id.enhanced.SequenceStyleGenerator;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test that {@link SequenceStyleGenerator} declared with {@link GenericGenerator} uses the reactive sequence generation.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class GenericSequenceGeneratorTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class );
	}

	@Test
	public void testPersist(VertxTestContext context) {
		Book book = new Book( "Snow Crash" );
		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( book ) )
				.thenAccept( v -> assertThat( book.id ).isEqualTo( 100L ) )
				.thenCompose( v -> getSessionFactory().withSession( s -> s.find( Book.class, book.id ) ) )
				.thenAccept( found -> assertThat( found.title ).isEqualTo( book.title ) )
		);
	}

	@Test
	public void testStatelessInsert(VertxTestContext context) {
		Book book = new Book( "Cryptonomicon" );
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.insert( book ) )
				.thenAccept( v -> assertThat( book.id ).isNotNull().isGreaterThanOrEqualTo( 100L ) )
				.thenCompose( v -> getSessionFactory().withStatelessSession( s -> s.get( Book.class, book.id ) ) )
				.thenAccept( found -> assertThat( found.title ).isEqualTo( book.title ) )
		);
	}

	@Entity(name = "Book")
	@Table(name = "GenericSequenceBook")
	public static class Book {
		@Id
		@GeneratedValue(generator = "sequence")
		@GenericGenerator(
				type = SequenceStyleGenerator.class,
				parameters = {
						@Parameter(name = SequenceStyleGenerator.SEQUENCE_PARAM, value = "generic_book_seq"),
						@Parameter(name = SequenceStyleGenerator.INITIAL_PARAM, value = "100")
				}
		)
		Long id;

		String title;

		public Book() {
		}

		public Book(String title) {
			this.title = title;
		}
	}
}
