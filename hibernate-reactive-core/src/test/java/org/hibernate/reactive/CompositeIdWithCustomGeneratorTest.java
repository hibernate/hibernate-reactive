/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
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
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.util.internal.CompletionStages.completedFuture;

/**
 * Test a {@link ReactiveIdentifierGenerator} declared with {@link GenericGenerator}
 * on an attribute of a composite id.
 */
@Timeout(value = 10, timeUnit = MINUTES)
public class CompositeIdWithCustomGeneratorTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Product.class );
	}

	@Test
	public void testPersist(VertxTestContext context) {
		final Product product = new Product( "Stateful", 1L );
		test( context, getSessionFactory()
				.withTransaction( s -> s.persist( product ) )
				.thenAccept( v -> assertThat( product.id ).isNotNull().isPositive() )
				.thenCompose( v -> getSessionFactory().withSession( s -> s
						.find( Product.class, new ProductId( product.version, product.id ) ) ) )
				.thenAccept( found -> assertThat( found ).isEqualTo( product ) )
		);
	}

	@Test
	public void testStatelessInsert(VertxTestContext context) {
		final Product product = new Product( "Stateless", 1L );
		test( context, getSessionFactory()
				.withStatelessTransaction( s -> s.insert( product ) )
				.thenAccept( v -> assertThat( product.id ).isNotNull().isPositive() )
				.thenCompose( v -> getSessionFactory().withStatelessSession( s -> s
						.get( Product.class, new ProductId( product.version, product.id ) ) ) )
				.thenAccept( found -> assertThat( found ).isEqualTo( product ) )
		);
	}

	public static class Counter implements ReactiveIdentifierGenerator<Long> {
		private final AtomicLong current = new AtomicLong();

		@Override
		public CompletionStage<Long> generate(ReactiveConnectionSupplier session, Object entity) {
			return completedFuture( current.incrementAndGet() );
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

	@Entity(name = "Product")
	@Table(name = "CustomGeneratorProduct")
	@IdClass(ProductId.class)
	public static class Product {
		@Id
		Long version;

		@Id
		@GeneratedValue
		@GenericGenerator(type = Counter.class)
		Long id;

		String name;

		public Product() {
		}

		public Product(String name, Long version) {
			this.name = name;
			this.version = version;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			return Objects.equals( name, ( (Product) o ).name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}
	}

	public static class ProductId {
		Long version;
		Long id;

		public ProductId() {
		}

		public ProductId(Long version, Long id) {
			this.version = version;
			this.id = id;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			ProductId productId = (ProductId) o;
			return Objects.equals( version, productId.version ) && Objects.equals( id, productId.id );
		}

		@Override
		public int hashCode() {
			return Objects.hash( version, id );
		}
	}
}
