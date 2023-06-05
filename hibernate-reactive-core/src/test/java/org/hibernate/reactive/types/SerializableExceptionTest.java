/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import io.vertx.junit5.Timeout;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import org.hibernate.reactive.BaseReactiveTest;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test an issue with Vert.x SQL client 3.9.2:
 * it happens when running the same query twice. The first time it will guess the type of the parameters
 * and then cache it (if the cache is enabled). The problem is that if the parameter is null in the first
 * query, it won't guess the right type and won't try again. It's a problem for us when dealing with Buffers and
 * Serializable.
 */
@Timeout(value = 10, timeUnit = MINUTES)

public class SerializableExceptionTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Basic.class );
	}

	@Test
	public void testSerialization(VertxTestContext context) {
		Serializable expected =  new String[] { "Hello", "World!" };
		Basic nullField = new Basic( "Fergus" );
		Basic notNullField = new Basic( "Merida", expected );

		test(
				context,
				// For this test to fail the entity with the null field needs to be persisted before
				// the entity with the notNullField
				getSessionFactory().withTransaction( (s, t) -> s.persist( nullField, notNullField ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( Basic.class, notNullField.getId() ) )
						.thenAccept( found -> {
							assertNotNull( found );
							assertTrue( Objects.deepEquals( expected, found.thing ) );
						} )

		);
	}

	@Entity(name = "Basic")
	@Table(name = "Basic")
	public static class Basic {

		@Id
		@GeneratedValue
		Integer id;

		String string;

		@jakarta.persistence.Basic
		Serializable thing;

		public Basic(String string) {
			this( string, null );
		}

		public Basic(String string, Serializable thing) {
			this.string = string;
			this.thing = thing;
		}

		public Basic(Integer id, String string) {
			this.id = id;
			this.string = string;
		}

		Basic() {
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		public String getString() {
			return string;
		}

		public void setString(String string) {
			this.string = string;
		}

		@Override
		public String toString() {
			return id + ": " + string;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Basic basic = (Basic) o;
			return Objects.equals( string, basic.string );
		}

		@Override
		public int hashCode() {
			return Objects.hash( string );
		}
	}
}
