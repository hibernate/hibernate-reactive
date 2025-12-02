/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

public class EmbeddedIdWithOneToOneTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( FooEntity.class, BarEntity.class );
	}

	@Test
	public void test(VertxTestContext context) {
		BarEntity barEntity = new BarEntity( "1" );
		FooId fooId = new FooId( barEntity );
		FooEntity entity = new FooEntity( fooId );

		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.persistAll( barEntity, entity ) )
						.chain( () -> getMutinySessionFactory()
								.withTransaction( s -> s.find( FooEntity.class, fooId ) )
						)
						.invoke( result -> {
							assertThat( result.getId() ).isEqualTo( fooId );
							assertThat( result.getId().getIdEntity() ).isEqualTo( fooId.getIdEntity() );
							assertThat( result.getId().getIdEntity().getId() ).isEqualTo( fooId.getIdEntity().getId() );
						} )
		);
	}

	@Entity(name = "bar")
	public static class BarEntity {

		@Id
		private String id;

		public BarEntity() {
		}

		public BarEntity(String id) {
			this.id = id;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}
	}

	@Entity(name = "foo")
	public static class FooEntity {

		@EmbeddedId
		private FooId id;

		public FooEntity() {
		}

		public FooEntity(FooId id) {
			this.id = id;
		}

		public FooId getId() {
			return id;
		}

		public void setId(FooId id) {
			this.id = id;
		}
	}

	@Embeddable
	public static class FooId {

		@OneToOne(fetch = FetchType.EAGER)
		@JoinColumn(name = "id", nullable = false)
		private BarEntity barEntity;

		public FooId() {
		}

		public FooId(BarEntity barEntity) {
			this.barEntity = barEntity;
		}

		public BarEntity getIdEntity() {
			return barEntity;
		}

		public void setIdEntity(BarEntity barEntity) {
			this.barEntity = barEntity;
		}

		@Override
		public boolean equals(Object o) {
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			FooId fooId = (FooId) o;
			return Objects.equals( barEntity, fooId.barEntity );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( barEntity );
		}
	}
}
