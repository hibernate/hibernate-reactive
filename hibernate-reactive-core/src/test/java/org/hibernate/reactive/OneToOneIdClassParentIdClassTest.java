/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.ext.unit.TestContext;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.OneToOne;
import java.io.Serializable;
import java.util.Objects;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;

public class OneToOneIdClassParentIdClassTest extends BaseReactiveTest {
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
				completedFuture( openSession() )
						.thenCompose( s -> s.persist( otherEntity ) )
						.thenCompose( s -> s.persist( anEntity ) )
						.thenCompose( s -> s.flush() )
						.thenApply( v -> openSession() )
						.thenCompose( s -> s.find( AnEntity.class, new OtherEntityId( 1 ) )
								.thenAccept( optionalAnEntity -> {
									context.assertNotNull( optionalAnEntity );
									context.assertEquals( anEntity, optionalAnEntity );
									context.assertEquals( otherEntity, optionalAnEntity.otherEntity );
								})
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
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			AnEntity anEntity = (AnEntity) o;
			return otherEntity.equals(anEntity.otherEntity) &&
					Objects.equals(name, anEntity.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(otherEntity, name);
		}
	}

	public static class OtherEntityId implements Serializable {
		private int id;

		OtherEntityId() {
		}

		OtherEntityId(int id) {
			this.id = id;
		}
	}

	@Entity(name = "OtherEntity")
	@IdClass(OtherEntityId.class)
	public static class OtherEntity implements Serializable {

		@Id
		private int id;
		private String name;

		OtherEntity() {
		}

		OtherEntity(int id, String name) {
			this.id = id;
			this.name = name;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			OtherEntity that = (OtherEntity) o;
			return id == that.id &&
					Objects.equals(name, that.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name);
		}
	}
}
