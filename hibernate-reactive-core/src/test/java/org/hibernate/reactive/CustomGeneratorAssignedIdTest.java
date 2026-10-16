/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import org.hibernate.annotations.GenericGenerator;
import org.hibernate.generator.EventType;
import org.hibernate.reactive.id.ReactiveIdentifierGenerator;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.util.internal.CompletionStages.completedFuture;

/**
 * Test a {@link ReactiveIdentifierGenerator} that allows assigned identifiers
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class CustomGeneratorAssignedIdTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Book.class );
	}

	@Test
	public void testStatelessInsertWithAssignedId(VertxTestContext context) {
		Book book = new Book( 5L, "Anathem" );
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.insert( book ) )
				.thenAccept( v -> assertThat( book.id ).isEqualTo( 5L ) )
				.thenCompose( v -> getSessionFactory().withStatelessSession( s -> s.get( Book.class, 5L ) ) )
				.thenAccept( found -> assertThat( found.title ).isEqualTo( book.title ) )
		);
	}

	@Test
	public void testStatelessInsertWithGeneratedId(VertxTestContext context) {
		Book book = new Book( null, "Seveneves" );
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.insert( book ) )
				.thenAccept( v -> assertThat( book.id ).isGreaterThanOrEqualTo( 1000L ) )
				.thenCompose( v -> getSessionFactory().withStatelessSession( s -> s.get( Book.class, book.id ) ) )
				.thenAccept( found -> assertThat( found.title ).isEqualTo( book.title ) )
		);
	}

	public static class AssignedOrGenerated implements ReactiveIdentifierGenerator<Long> {
		private final AtomicLong current = new AtomicLong( 999L );

		@Override
		public CompletionStage<Long> generate(ReactiveConnectionSupplier session, Object entity) {
			return completedFuture( current.incrementAndGet() );
		}

		@Override
		public CompletionStage<Long> generate(ReactiveConnectionSupplier session, Object owner, Object currentValue, EventType eventType) {
			return currentValue == null
					? generate( session, owner )
					: completedFuture( (Long) currentValue );
		}

		@Override
		public boolean allowAssignedIdentifiers() {
			return true;
		}

		@Override
		public boolean generatedOnExecution() {
			return false;
		}

		@Override
		public EnumSet<EventType> getEventTypes() {
			return EnumSet.of( EventType.INSERT );
		}
	}

	@Entity(name = "Book")
	@Table(name = "AssignedIdBook")
	public static class Book {
		@Id
		@GeneratedValue(generator = "assigned")
		@GenericGenerator(type = AssignedOrGenerated.class)
		Long id;

		String title;

		public Book() {
		}

		public Book(Long id, String title) {
			this.id = id;
			this.title = title;
		}
	}
}
