/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.Test;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;

public class ReactiveConstraintViolationTest extends BaseReactiveTest {

	@Override
	protected Set<Class<?>> annotatedEntities() {
		return Set.of( GuineaPig.class );
	}

	private CompletionStage<Void> populateDB() {
		return getSessionFactory()
				.withTransaction( s -> s.persist( new GuineaPig( 5, "Aloi" ) ) );
	}

	@Test
	public void reactiveConstraintViolation(TestContext context) {
		test(
				context,
				populateDB()
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.persist( new GuineaPig( 5, "Aloi" ) )
								.thenCompose( i -> s.flush() ) )
						.handle( (r,ce) -> context.assertTrue( ce != null && ce.getCause() instanceof ConstraintViolationException ) )

		);
	}

	@Entity(name = "GuineaPig")
	@Table(name = "bad_pig")
	public static class GuineaPig {
		@Id
		private Integer id;
		private String name;
		@Version
		private int version;

		public GuineaPig() {
		}

		public GuineaPig(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return id + ": " + name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			GuineaPig guineaPig = (GuineaPig) o;
			return Objects.equals( name, guineaPig.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}
}
