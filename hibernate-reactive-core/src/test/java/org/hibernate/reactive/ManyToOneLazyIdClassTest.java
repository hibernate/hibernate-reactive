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

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

@Timeout(value = 10, timeUnit = MINUTES)
public class ManyToOneLazyIdClassTest extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( Relationship.class, EntityA.class, EntityB.class );
	}

	@Test
	public void test(VertxTestContext context) {
		final UUID aId = UUID.randomUUID();
		final EntityA entityA = new EntityA( aId, "testA" );
		final UUID bId = UUID.randomUUID();
		final EntityB entityB = new EntityB( bId, "testB" );
		final Relationship relationship = new Relationship( entityA, entityB, "testRelationship" );

		test( context, getMutinySessionFactory()
				.withTransaction( s -> s.persistAll( entityA, entityB, relationship ) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.find( EntityA.class, aId )
						.invoke( entity -> assertThat( entity ).isEqualTo( entityA ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s
						.find( EntityB.class, bId )
						.invoke( entity -> assertThat( entity ).isEqualTo( entityB ) )
				) )
				.chain( () -> getMutinySessionFactory().withTransaction( s -> s.find(
						Relationship.class,
						new RelationshipId( aId, bId )
				) ) )
		);
	}

	@Entity
	@Table(name = "entity_a")
	public static class EntityA {

		@Id
		@Column(name = "id")
		private UUID id;

		@Column(name = "name")
		private String name;

		public EntityA(UUID id, String name) {
			this.id = id;
			this.name = name;
		}

		public EntityA() {
		}

		public UUID getId() {
			return id;
		}

		public void setId(UUID id) {
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
			return id + ":" + name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			EntityA entityA = (EntityA) o;
			return Objects.equals( id, entityA.id ) && Objects.equals( name, entityA.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id, name );
		}
	}

	@Entity
	@Table(name = "entity_b")
	public static class EntityB {

		@Id
		@Column(name = "id")
		private UUID id;

		@Column(name = "name")
		private String name;

		public EntityB(UUID id, String name) {
			this.id = id;
			this.name = name;
		}

		public EntityB() {
		}

		public UUID getId() {
			return id;
		}

		public void setId(UUID id) {
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
			return id + ":" + name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			EntityB entityB = (EntityB) o;
			return Objects.equals( id, entityB.id ) && Objects.equals( name, entityB.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( id, name );
		}
	}

	@Entity( name = "Relationship")
	@Table(name = "relationship")
	@IdClass(RelationshipId.class)
	public static class Relationship {

		@Id
		@ManyToOne(fetch = FetchType.LAZY)
		@JoinColumn(name = "a_id", referencedColumnName = "id")
		private EntityA entityA;

		@Id
		@ManyToOne(fetch = FetchType.LAZY)
		@JoinColumn(name = "b_id", referencedColumnName = "id")
		private EntityB entityB;

		@Column(name = "dataField")
		private String dataField;

		public Relationship(EntityA entityA, EntityB entityB, String dataField) {
			this.entityA = entityA;
			this.entityB = entityB;
			this.dataField = dataField;
		}

		public Relationship() {
		}

		public EntityA getEntityA() {
			return entityA;
		}

		public void setEntityA(EntityA entityA) {
			this.entityA = entityA;
		}

		public EntityB getEntityB() {
			return entityB;
		}

		public void setEntityB(EntityB entityB) {
			this.entityB = entityB;
		}

		public String getDataField() {
			return dataField;
		}

		public void setDataField(String dataField) {
			this.dataField = dataField;
		}

		@Override
		public String toString() {
			return entityA + "-" + entityB + ":" + dataField;
		}

		@Override
		public boolean equals(Object obj) {
			if ( this == obj ) {
				return true;
			}
			if ( !( obj instanceof Relationship ) ) {
				return false;
			}
			Relationship other = (Relationship) obj;
			if ( entityA == null
					|| other.getEntityA() == null
					|| entityB == null
					|| other.getEntityB() == null
			) {
				return false;
			}
			return entityA.equals( other.getEntityA() )
					&& entityB.equals( other.getEntityB() );
		}

		@Override
		public int hashCode() {
			return Objects.hash( entityA, entityB );
		}
	}

	public static class RelationshipId {

		private UUID entityA;
		private UUID entityB;

		public RelationshipId() {
		}

		public RelationshipId(UUID entityA, UUID entityB) {
			this.entityA = entityA;
			this.entityB = entityB;
		}

		public UUID getEntityA() {
			return entityA;
		}

		public void setEntityA(UUID entityA) {
			this.entityA = entityA;
		}

		public UUID getEntityB() {
			return entityB;
		}

		public void setEntityB(UUID entityB) {
			this.entityB = entityB;
		}

		@Override
		public String toString() {
			return entityA + ":" + entityB;
		}

		@Override
		public boolean equals(Object obj) {
			if ( this == obj ) {
				return true;
			}
			if ( !( obj instanceof RelationshipId ) ) {
				return false;
			}
			RelationshipId other = (RelationshipId) obj;
			if ( entityA == null
					|| other.getEntityA() == null
					|| entityB == null
					|| other.getEntityB() == null
			) {
				return false;
			}
			return entityA.equals( other.getEntityA() )
					&& entityB.equals( other.getEntityB() );
		}

		@Override
		public int hashCode() {
			return Objects.hash( entityA, entityB );
		}
	}

}
