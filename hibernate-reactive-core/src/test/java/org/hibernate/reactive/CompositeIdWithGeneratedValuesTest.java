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

@Timeout(value = 10, timeUnit = MINUTES)

public class CompositeIdWithGeneratedValuesTest extends BaseReactiveTest{

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Product.class);
	}

	@Test
	public void testCompositeIdWithGeneratedValues(VertxTestContext context) {
		Product product = new Product("name", 1L);
		test( context, openSession()
				.thenCompose( session -> session
						.persist( product )
						.thenCompose( v -> session.flush() ))
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
