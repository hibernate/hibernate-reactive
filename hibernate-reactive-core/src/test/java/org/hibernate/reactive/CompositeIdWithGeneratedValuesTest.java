/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.SequenceGenerator;

import java.util.Collection;
import java.util.List;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)

public class CompositeIdWithGeneratedValuesTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Product.class );
	}

	@Test
	public void testCompositeIdWithGeneratedValues(VertxTestContext context) {
		final Product product = new Product( "name", 1150L );
		test(
				context,
				getMutinySessionFactory().withTransaction( session -> session.persist( product ) )
						.invoke( () -> assertThat( product.id ).isNotNull() )
						.chain( () -> getMutinySessionFactory().withTransaction( session -> session.find(
								Product.class,
								new ProductId( product.version, product.id )
						) ) )
						.invoke( found -> {
							assertThat( found ).hasFieldOrPropertyWithValue( "id", product.id );
							assertThat( found ).hasFieldOrPropertyWithValue( "version", product.version );
							assertThat( found ).hasFieldOrPropertyWithValue( "name", product.name );
						} )
		);
	}

	@Entity
	@IdClass(ProductId.class)
	public static class Product {
		@Id
		private Long version;

		@Id
		@GeneratedValue
		@SequenceGenerator(name = "product_seq", sequenceName = "product_seq")
		private Long id;

		private String name;

		public Product() {
		}

		public Product(String name, Long version) {
			this.name = name;
			this.version = version;
		}

		public Long getVersion() {
			return version;
		}

		public void setVersion(Long version) {
			this.version = version;
		}

		public Long getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		@Override
		public String toString() {
			return "(" + id + "," + version + "):" + name;
		}
	}

	public static class ProductId {
		private Long version;
		private Long id;

		private ProductId() {
		}

		public ProductId(Long version, Long id) {
			this.version = version;
			this.id = id;
		}
	}
}
