/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.hibernate.ObjectNotFoundException;
import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.SoftDelete;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Tuple;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.common.Identifier.id;
import static org.hibernate.reactive.testing.ReactiveAssertions.assertThrown;
import static org.hibernate.reactive.util.impl.CompletionStages.voidFuture;

@Timeout(value = 10, timeUnit = MINUTES)
public class ImplicitSoftDeleteTests extends BaseReactiveTest {

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( ImplicitEntity.class );
	}

	@Override
	protected CompletionStage<Void> cleanDb() {
		return voidFuture();
	}

	@BeforeEach
	void createTestData(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.createNativeQuery( "delete from implicit_entities" ).executeUpdate() )
						.call( () -> getMutinySessionFactory().withTransaction( s -> s
								.persistAll( new ImplicitEntity( 1, "first" ), new ImplicitEntity( 2, "second" ), new ImplicitEntity( 3, "third" ) )
						) )
						.chain( () -> getMutinySessionFactory().withTransaction( s -> {
							final ImplicitEntity first = s.getReference( ImplicitEntity.class, 1 );
							return s.remove( first ).call( s::flush );
						} ) )
						.call( () -> getMutinySessionFactory()
								.withTransaction( s -> s.createNativeQuery( "select * from implicit_entities e order by id", Tuple.class ).getResultList() )
								.invoke( tuples -> assertThat( tuples ).hasSize( 3 ) )
						)
		);
	}

	@Test
	void testSelectionQuery(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.createQuery( "from ImplicitEntity", ImplicitEntity.class ).getResultList() )
						.invoke( list -> assertThat( list ).hasSize( 2 ) )
		);
	}

	@Test
	void testLoading(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						// Load
						.withTransaction( s -> s
								.find( ImplicitEntity.class, 1 ).invoke( entity -> assertThat( entity ).isNull() )
								.call( () -> s.find( ImplicitEntity.class, 2 ).invoke( entity -> assertThat( entity ).isNotNull() ) )
								.call( () -> s.find( ImplicitEntity.class, 3 ).invoke( entity -> assertThat( entity ).isNotNull() ) )
						)
						// Proxy
						// We deleted the entity, so we expect an ObjectNotFoundException
						.chain( () -> assertThrown( ObjectNotFoundException.class, getMutinySessionFactory().withTransaction( s -> {
							final ImplicitEntity reference = s.getReference( ImplicitEntity.class, 1 );
							return s.fetch( reference ).map( ImplicitEntity::getName );
						} ) ) )
						.chain( () -> getMutinySessionFactory().withTransaction( s -> {
							final ImplicitEntity reference = s.getReference( ImplicitEntity.class, 2 );
							return s.fetch( reference ).map( ImplicitEntity::getName );
						} ) )
						.invoke( name -> assertThat( name ).isEqualTo( "second" ) )
						.chain( () -> getMutinySessionFactory().withTransaction( s -> {
							final ImplicitEntity reference = s.getReference( ImplicitEntity.class, 3 );
							return s.fetch( reference ).map( ImplicitEntity::getName );
						} ) )
						.invoke( name -> assertThat( name ).isEqualTo( "third" ) )
		);
	}

	@Test
	void testMultiLoading(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.find( ImplicitEntity.class, 1, 2, 3 ) )
						.invoke( list -> assertThat( list )
								.containsExactly( null, new ImplicitEntity( null, "second" ), new ImplicitEntity( null, "third" ) ) )
		);
	}

	@Test
	void testNaturalIdLoading(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.find( ImplicitEntity.class, id( "name", "first" ) ) )
						.invoke( entity -> assertThat( entity ).isNull() )
						.chain( () -> getMutinySessionFactory().withTransaction( s -> s.find( ImplicitEntity.class, id( "name", "second" ) ) ) )
						.invoke( entity -> assertThat( entity ).extracting( ImplicitEntity::getId ).isEqualTo( 2 ) )
						.chain( () -> getMutinySessionFactory().withTransaction( s -> s.find( ImplicitEntity.class, id( "name", "third" ) ) ) )
						.invoke( entity -> assertThat( entity ).extracting( ImplicitEntity::getId ).isEqualTo( 3 ) )
		);
	}

	@Test
	void testDeletion(VertxTestContext context) {
		test(
				context, getMutinySessionFactory().withTransaction( s -> {
					final ImplicitEntity reference = s.getReference( ImplicitEntity.class, 2 );
					return s.remove( reference ).call( s::flush )
							.call( () -> s.createSelectionQuery( "from ImplicitEntity", ImplicitEntity.class ).getResultList()
									// #1 was "deleted" up front and we just "deleted" #2... only #3 should be active
									.invoke( list -> {
										assertThat( list ).extracting( ImplicitEntity::getId ).containsExactly( 3 );
										assertThat( list ).extracting( ImplicitEntity::getName ).containsExactly( "third" );
									} )
							);
				} )
		);
	}

	@Test
	void testFullUpdateMutationQuery(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.createMutationQuery( "update ImplicitEntity set name = null" ).executeUpdate() )
						.invoke( affected -> assertThat( affected ).isEqualTo( 2 ) )
		);
	}

	@Test
	void testRestrictedUpdateMutationQuery(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.createMutationQuery( "update ImplicitEntity set name = null where name = 'second'" ).executeUpdate() )
						.invoke( affected -> assertThat( affected ).isEqualTo( 1 ) )
		);
	}

	@Test
	void testFullDeleteMutationQuery(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.createMutationQuery( "delete ImplicitEntity" ).executeUpdate() )
						// only #2 and #3
						.invoke( affected -> assertThat( affected ).isEqualTo( 2 ) )
		);
	}

	@Test
	void testRestrictedDeleteMutationQuery(VertxTestContext context) {
		test(
				context, getMutinySessionFactory()
						.withTransaction( s -> s.createMutationQuery( "delete ImplicitEntity where name = 'second'" ).executeUpdate() )
						// only #2
						.invoke( affected -> assertThat( affected ).isEqualTo( 1 ) )
		);
	}


	@Entity(name = "ImplicitEntity")
	@Table(name = "implicit_entities")
	@SoftDelete
	public static class ImplicitEntity {
		@Id
		private Integer id;
		@NaturalId
		private String name;

		public ImplicitEntity() {
		}

		public ImplicitEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		public Integer getId() {
			return id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public boolean equals(Object object) {
			if ( object == null || getClass() != object.getClass() ) {
				return false;
			}
			ImplicitEntity that = (ImplicitEntity) object;
			return Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}
	}
}
