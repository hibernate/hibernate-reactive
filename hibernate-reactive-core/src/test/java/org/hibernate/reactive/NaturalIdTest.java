/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.hibernate.annotations.NaturalId;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.common.Identifier.composite;
import static org.hibernate.reactive.common.Identifier.id;

public class NaturalIdTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( SimpleThing.class, CompoundThing.class );
	}

	@Test
	public void testSimpleNaturalIdSingleLoad(TestContext context) {
		SimpleThing thing1 = new SimpleThing();
		thing1.naturalKey = "abc123";
		SimpleThing thing2 = new SimpleThing();
		thing2.naturalKey = "def456";
		test(
				context,
				getSessionFactory()
						.withSession( session -> session.persist( thing1, thing2 ).thenCompose( v -> session.flush() ) )
						.thenCompose( v -> getSessionFactory().withSession(
								session -> session.find( SimpleThing.class, id( "naturalKey", "abc123" ) )
						) )
						.thenAccept( t -> assertThat( t ).isEqualTo( thing1 ) )
						.thenCompose( v -> getSessionFactory().withSession(
								session -> session.find( SimpleThing.class, id( SimpleThing.class, "naturalKey", "not an id" ) )
						) )
						.thenAccept( context::assertNull )
		);
	}

	@Test
	public void testCompoundNaturalIdSingleLoad(TestContext context) {
		CompoundThing thing1 = new CompoundThing();
		thing1.naturalKey = "xyz666";
		thing1.version = 1;
		CompoundThing thing2 = new CompoundThing();
		thing2.naturalKey = "xyz666";
		thing2.version = 2;
		test(
				context,
				getSessionFactory()
						.withSession( session -> session.persist( thing1, thing2 ).thenCompose( v -> session.flush() ) )
						.thenCompose( v -> getSessionFactory().withSession(
								session -> session.find( CompoundThing.class, composite(
										id( "naturalKey", "xyz666" ),
										id( "version", 1 )
								) )
						) )

						.thenAccept( t -> assertThat( t ).isEqualTo( thing1 ) )
						.thenCompose( v -> getSessionFactory().withSession(
								session -> session.find( CompoundThing.class, composite(
										id( CompoundThing.class, "naturalKey", "xyz666" ),
										id( CompoundThing.class, "version", 3 )
								) )
						) )
						.thenAccept( context::assertNull )
		);
	}

	@Entity(name = "SimpleThing")
	static class SimpleThing {
		@Id
		@GeneratedValue
		long id;
		@NaturalId
		String naturalKey;

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			SimpleThing that = (SimpleThing) o;
			return Objects.equals( naturalKey, that.naturalKey );
		}

		@Override
		public int hashCode() {
			return Objects.hash( naturalKey );
		}
	}

	@Entity(name = "CompoundThing")
	static class CompoundThing {
		@Id
		@GeneratedValue
		long id;
		@NaturalId
		String naturalKey;
		@NaturalId
		int version;

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			CompoundThing that = (CompoundThing) o;
			return version == that.version && Objects.equals( naturalKey, that.naturalKey );
		}

		@Override
		public int hashCode() {
			return Objects.hash( naturalKey, version );
		}
	}
}
