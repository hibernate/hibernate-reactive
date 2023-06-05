/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;

import static org.assertj.core.api.Assertions.assertThat;

public class UUIDAsBinaryTypeTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( UUIDEntity.class );
	}

	@Test
	public void uuidIdentifierWithMutinyAPI(VertxTestContext context) {
		UUIDEntity entityA = new UUIDEntity( "UUID A" );
		UUIDEntity entityB = new UUIDEntity( "UUID B" );

		test( context, openMutinySession().chain( session -> session
						.persistAll( entityA, entityB )
						.call( session::flush ) )
				.invoke( () -> assertThat( entityA.id ).isNotEqualTo( entityB.id ) )
				.chain( this::openMutinySession )
				.chain( session -> session.find( UUIDEntity.class, entityA.id, entityB.id ) )
				.invoke( list ->  assertThat( list ).containsExactlyInAnyOrder( entityA, entityB ) )
		);
	}

	@Entity(name = "UUIDEntity")
	private static class UUIDEntity {

		@Id
		@GeneratedValue
		UUID id;

		@Column(unique = true)
		String name;

		public UUIDEntity() {
		}

		public UUIDEntity(String name) {
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
			UUIDEntity that = (UUIDEntity) o;
			return Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}

		@Override
		public String toString() {
			return id + ":" + name;
		}
	}
}
