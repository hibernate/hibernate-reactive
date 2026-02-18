/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.hibernate.annotations.SoftDelete;
import org.hibernate.annotations.SoftDeleteType;
import org.hibernate.reactive.testing.SqlStatementTracker;
import org.hibernate.type.YesNoConverter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.criteria.CriteriaBuilder;
import jakarta.persistence.criteria.CriteriaDelete;
import jakarta.persistence.criteria.Root;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.hibernate.reactive.util.impl.CompletionStages.loop;

/**
 * Tests validity of @SoftDelete annotation value options
 * as well as verifying logged 'create table' and 'update' queries for each database.
 *
 * Tests single-table entity mappings (entities without inheritance).
 */
public class SoftDeleteSingleTableTest extends BaseReactiveTest {

	private static SqlStatementTracker sqlTracker;

	static final Deletable[] activeEntities = {
			new ActiveEntity( 1, "active first" ),
			new ActiveEntity( 2, "active second" ),
			new ActiveEntity( 3, "active third" ),
	};
	static final Deletable[] deletedEntities = {
			new DeletedEntity( 1, "deleted first" ),
			new DeletedEntity( 2, "deleted second" ),
			new DeletedEntity( 3, "deleted third" ),
	};
	static final Deletable[] implicitEntities = {
			new ImplicitEntity( 1, "implicit first" ),
			new ImplicitEntity( 2, "implicit second" ),
			new ImplicitEntity( 3, "implicit third" )
	};

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( ActiveEntity.class, DeletedEntity.class, ImplicitEntity.class, TimestampEntity.class );
	}

	@BeforeEach
	public void populateDB(VertxTestContext context) {
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( activeEntities ) )
				.call( () -> getMutinySessionFactory().withTransaction( session -> session.persistAll( deletedEntities ) ) )
				.call( () -> getMutinySessionFactory().withTransaction( session -> session.persistAll( implicitEntities ) ) )
		);
	}

	// Entities are annotated with @SoftDelete, we need to execute a native query to actually empty the table
	@Override
	protected CompletionStage<Void> cleanDb() {
		return loop( annotatedEntities(), aClass -> getSessionFactory()
				.withTransaction( s -> s.createNativeQuery( "delete from " + aClass.getSimpleName() ).executeUpdate() )
		);
	}

	@Test
	public void testTimestampSoftDelete(VertxTestContext context) {
		TimestampEntity entity1 = new TimestampEntity( 1, "timestamp first" );
		TimestampEntity entity2 = new TimestampEntity( 2, "timestamp second" );

		test( context, getMutinySessionFactory()
				.withTransaction( session -> session.persistAll( entity1, entity2 ) )
				// Initially neither row should have a deletion timestamp
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createNativeQuery( "select id, name, deleted from TimestampEntity order by id" )
						.getResultList()
						.invoke( rows -> {
							assertThat( rows ).hasSize( 2 );
							for ( Object row : rows ) {
								Object[] r = (Object[]) row;
								assertThat( r[2] ).as( "deleted timestamp should be null for active entity" ).isNull();
							}
						} )
				) )
				// Soft delete the first entity
				.call( () -> getMutinySessionFactory().withTransaction( s -> s
						.find( TimestampEntity.class, 1 )
						.chain( s::remove )
				) )
				// The deleted entity must not be visible via HQL
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.find( TimestampEntity.class, 1 )
						.invoke( found -> assertThat( found ).isNull() )
				) )
				// HQL query should return only the non-deleted entity
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createSelectionQuery( "from TimestampEntity order by id", TimestampEntity.class )
						.getResultList()
						.invoke( list -> {
							assertThat( list ).hasSize( 1 );
							assertThat( list.get( 0 ).getId() ).isEqualTo( 2 );
						} )
				) )
				// Native query must show both rows still in the table
				.call( () -> getMutinySessionFactory().withSession( s -> s
						.createNativeQuery( "select id, name, deleted from TimestampEntity order by id" )
						.getResultList()
						.invoke( rows -> {
							assertThat( rows ).hasSize( 2 );
							Object[] deletedRow = (Object[]) rows.get( 0 );
							Object[] activeRow = (Object[]) rows.get( 1 );
							// The first row should have a non-null timestamp (soft deleted)
							assertThat( deletedRow[2] ).as( "deleted timestamp should be set" ).isNotNull();
							// The second row should still have null (active)
							assertThat( activeRow[2] ).as( "active entity should have null deleted timestamp" ).isNull();
						} )
				) )
		);
	}

	@Test
	public void testActiveStrategyWithYesNoConverter(VertxTestContext context) {
		testSoftDelete( context, s -> s.equals( "N" ), "active", ActiveEntity.class, activeEntities,
				() -> getMutinySessionFactory().withTransaction( s -> s
						.remove( s.getReference( ActiveEntity.class, activeEntities[0].getId() ) )
				)
		);
	}

	@Test
	public void testDeletedStrategyWithYesNoConverter(VertxTestContext context) {
		testSoftDelete( context, s -> s.equals( "Y" ), "deleted", DeletedEntity.class, deletedEntities,
				() -> getMutinySessionFactory().withTransaction( s -> s
						.remove( s.getReference( DeletedEntity.class, deletedEntities[0].getId() ) )
				)
		);
	}

	@Test
	public void testDefaults(VertxTestContext context) {
		Predicate<Object> deleted = obj -> dbType() == DB2
				? ( (short) obj ) == 1
				: (boolean) obj;

		testSoftDelete( context, deleted, "deleted", ImplicitEntity.class, implicitEntities,
				() -> getMutinySessionFactory().withTransaction( s -> s
						.remove( s.getReference( ImplicitEntity.class, implicitEntities[0].getId() ) )
				)
		);
	}

	@Test
	public void testDeletionWithHQLQuery(VertxTestContext context) {
		Predicate<Object> deleted = obj -> requireNonNull( dbType() ) == DB2
				? ( (short) obj ) == 1
				: (boolean) obj;

		testSoftDelete( context, deleted, "deleted", ImplicitEntity.class, implicitEntities,
				() -> getMutinySessionFactory().withTransaction( s -> s
						.createMutationQuery( "delete from ImplicitEntity where name = :name" )
						.setParameter( "name", implicitEntities[0].getName() )
						.executeUpdate()
				)
		);
	}

	@Test
	public void testDeletionWithCriteria(VertxTestContext context) {
		Predicate<Object> deleted = obj -> dbType() == DB2
				? ( (short) obj ) == 1
				: (boolean) obj;

		testSoftDelete( context, deleted, "deleted", ImplicitEntity.class, implicitEntities,
				() -> getMutinySessionFactory().withTransaction( s -> {
					CriteriaBuilder cb = getSessionFactory().getCriteriaBuilder();
					CriteriaDelete<ImplicitEntity> delete = cb.createCriteriaDelete( ImplicitEntity.class );
					Root<ImplicitEntity> root = delete.from( ImplicitEntity.class );
					delete.where( cb.equal( root.get( "name" ), implicitEntities[0].getName() ) );
					return s.createQuery( delete ).executeUpdate();
				} )
		);
	}

	private void testSoftDelete(
			VertxTestContext context,
			Predicate<Object> deleted,
			String deletedColumn,
			Class<?> entityClass,
			Deletable[] entities, Supplier<Uni<?>> deleteEntity) {
		test( context, getMutinySessionFactory()
				// Check that the soft delete column exists and has the expected initial value
				.withSession( s -> s
						// This SQL query should be compatible with all databases
						.createNativeQuery( "select id, name, " + deletedColumn + " from " + entityClass.getSimpleName() + " order by id" )
						.getResultList()
						.invoke( rows -> {
							assertThat( rows ).hasSize( entities.length );
							for ( int i = 0; i < entities.length; i++ ) {
								Object[] row = (Object[]) rows.get( i );
								Integer actualId = ( (Number) row[0] ).intValue();
								assertThat( actualId ).isEqualTo( entities[i].getId() );
								assertThat( row[1] ).isEqualTo( entities[i].getName() );
								// Only the first element should be deleted
								assertThat( deleted.test( row[2] ) ).isFalse();
							}
						} )
				)
				// Delete an entity
				.call( deleteEntity )
				// Test select all
				.call( () -> getMutinySessionFactory().withTransaction( s -> s
						.createSelectionQuery( "from " + entityClass.getSimpleName() + " order by id", Object.class )
						.getResultList()
						.invoke( list -> assertThat( list ).containsExactly( entities[1], entities[2] ) )
				) )
				// Test find
				.call( () -> getMutinySessionFactory().withTransaction( s -> s
						.find( entityClass, entities[0].getId() )
						.invoke( entity -> assertThat( entity ).isNull() )
				) )
				// Test table content with a native query
				.call( () -> getMutinySessionFactory().withSession( s -> s
						// This SQL query should be compatible with all databases
						.createNativeQuery( "select id, name, " + deletedColumn + " from " + entityClass.getSimpleName() + " order by id" )
						.getResultList()
						.invoke( rows -> {
							assertThat( rows ).hasSize( entities.length );
							for ( int i = 0; i < entities.length; i++ ) {
								Object[] row = (Object[]) rows.get( i );
								Integer actualId = ( (Number) row[0] ).intValue();
								assertThat( actualId ).isEqualTo( entities[i].getId() );
								assertThat( row[1] ).isEqualTo( entities[i].getName() );
								// Only the first element should have been deleted
								System.out.println( Arrays.toString( row ) );
								System.out.println( "Index: " + i + ", Actual: " + deleted.test( row[2] ) + " Expected: " + ( i == 0 ) );
								assertThat( deleted.test( row[2] ) ).isEqualTo( i == 0 );
							}
						} )
				) )
		);
	}

	// The interface helps with simplifying the code for the test
	private interface Deletable {
		Integer getId();

		String getName();
	}

	@Entity(name = "ActiveEntity")
	@Table(name = "ActiveEntity")
	@SoftDelete(converter = YesNoConverter.class, strategy = SoftDeleteType.ACTIVE)
	public static class ActiveEntity implements Deletable {
		@Id
		private Integer id;
		private String name;

		public ActiveEntity() {
		}

		public ActiveEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		@Override
		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		@Override
		public String getName() {
			return name;
		}

		public void setName(String name) {
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
			ActiveEntity that = (ActiveEntity) o;
			return Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}

		@Override
		public String toString() {
			return this.getClass() + ":" + id + ":" + name;
		}
	}

	@Entity(name = "DeletedEntity")
	@Table(name = "DeletedEntity")
	@SoftDelete(converter = YesNoConverter.class, strategy = SoftDeleteType.DELETED)
	public static class DeletedEntity implements Deletable {
		@Id
		private Integer id;
		private String name;

		public DeletedEntity() {
		}

		public DeletedEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		@Override
		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		@Override
		public String getName() {
			return name;
		}

		public void setName(String name) {
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
			DeletedEntity that = (DeletedEntity) o;
			return Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}

		@Override
		public String toString() {
			return this.getClass() + ":" + id + ":" + name;
		}
	}

	@Entity(name = "TimestampEntity")
	@Table(name = "TimestampEntity")
	@SoftDelete(strategy = SoftDeleteType.TIMESTAMP)
	public static class TimestampEntity {
		@Id
		private Integer id;
		private String name;

		public TimestampEntity() {
		}

		public TimestampEntity(Integer id, String name) {
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
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			TimestampEntity that = (TimestampEntity) o;
			return Objects.equals( id, that.id );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( id );
		}

		@Override
		public String toString() {
			return getClass().getSimpleName() + ":" + id + ":" + name;
		}
	}

	@Entity(name = "ImplicitEntity")
	@Table(name = "ImplicitEntity")
	@SoftDelete
	public static class ImplicitEntity implements Deletable {
		@Id
		private Integer id;
		private String name;

		public ImplicitEntity() {
		}

		public ImplicitEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}

		@Override
		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		@Override
		public String getName() {
			return name;
		}

		public void setName(String name) {
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
			ImplicitEntity that = (ImplicitEntity) o;
			return Objects.equals( name, that.name );
		}

		@Override
		public int hashCode() {
			return Objects.hashCode( name );
		}

		@Override
		public String toString() {
			return this.getClass() + ":" + id + ":" + name;
		}
	}
}
