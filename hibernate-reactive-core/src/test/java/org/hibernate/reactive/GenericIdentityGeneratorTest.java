/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.annotations.GenericGenerator;
import org.hibernate.id.IdentityGenerator;

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
 * Test that {@link IdentityGenerator} declared with {@link GenericGenerator} uses the reactive identity generation.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class GenericIdentityGeneratorTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class );
	}

	@Test
	public void testPersist(VertxTestContext context) {
		Book book = new Book( "Snow Crash" );
		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( book ) )
				.thenAccept( v -> assertThat( book.id ).isNotNull() )
				.thenCompose( v -> getSessionFactory().withSession( s -> s.find( Book.class, book.id ) ) )
				.thenAccept( found -> assertThat( found.title ).isEqualTo( book.title ) )
		);
	}

	@Test
	public void testStatelessInsert(VertxTestContext context) {
		Book book = new Book( "Cryptonomicon" );
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.insert( book ) )
				.thenAccept( v -> assertThat( book.id ).isNotNull() )
				.thenCompose( v -> getSessionFactory().withStatelessSession( s -> s.get( Book.class, book.id ) ) )
				.thenAccept( found -> assertThat( found.title ).isEqualTo( book.title ) )
		);
	}

	@Entity(name = "Book")
	@Table(name = "GenericIdentityBook")
	public static class Book {
		@Id
		@GeneratedValue(generator = "identity")
		@GenericGenerator(type = IdentityGenerator.class)
		Long id;

		String title;

		public Book() {
		}

		public Book(String title) {
			this.title = title;
		}
	}
}
