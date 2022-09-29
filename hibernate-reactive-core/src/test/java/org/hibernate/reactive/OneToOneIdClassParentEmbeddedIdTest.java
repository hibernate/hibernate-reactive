/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.io.Serializable;
import java.util.Objects;

import org.hibernate.cfg.Configuration;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.OneToOne;


public class OneToOneIdClassParentEmbeddedIdTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( AnEntity.class );
		configuration.addAnnotatedClass( OtherEntity.class );
		return configuration;
	}

	@Test
	public void testLoad(TestContext context) {
		final OtherEntity otherEntity = new OtherEntity( 1, "Other Entity" );
		final AnEntity anEntity = new AnEntity( otherEntity, "An Entity" );

		test(
				context,
				openSession()
						.thenCompose( s -> s.persist( otherEntity )
								.thenCompose( v -> s.persist( anEntity ) )
								.thenCompose( v -> s.flush() )
						)
						.thenCompose( v -> openSession() )
						.thenCompose( s -> s.find( AnEntity.class, new OtherEntityId( 1 ) )
								.thenAccept( optionalAnEntity -> {
									context.assertNotNull( optionalAnEntity );
									context.assertEquals( anEntity, optionalAnEntity );
									context.assertEquals( otherEntity, optionalAnEntity.otherEntity );
								} )
						)
		);
	}

	@Entity(name = "AnEntity")
	@IdClass(OtherEntityId.class)
	public static class AnEntity implements Serializable {
		@Id
		@OneToOne
		private OtherEntity otherEntity;
		private String name;

		AnEntity() {
		}

		AnEntity(OtherEntity otherEntity, String name) {
			this.otherEntity = otherEntity;
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			AnEntity anEntity = (AnEntity) o;
			return otherEntity.equals( anEntity.otherEntity ) &&
					Objects.equals( name, anEntity.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( otherEntity, name );
		}
	}

	@Embeddable
	public static class OtherEntityId implements Serializable {
		private int id;

		OtherEntityId() {
		}

		OtherEntityId(int id) {
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
			OtherEntityId that = (OtherEntityId) o;
			return id == that.id;
		}

		@Override
		public int hashCode() {
			return Objects.hash( id );
		}
	}

	@Entity(name = "OtherEntity")
	public static class OtherEntity implements Serializable {

		@EmbeddedId
		private OtherEntityId id;
		private String name;

		OtherEntity() {
		}

		OtherEntity(int id, String name) {
			this.id = new OtherEntityId( id );
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			OtherEntity that = (OtherEntity) o;
			return id.equals( that.id ) &&
					Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id, name );
		}
	}
}
