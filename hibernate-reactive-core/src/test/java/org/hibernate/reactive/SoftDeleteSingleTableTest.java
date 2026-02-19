/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.time.temporal.Temporal;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.hibernate.annotations.SoftDelete;
import org.hibernate.annotations.SoftDeleteType;
import org.hibernate.reactive.annotations.DisabledFor;
import org.hibernate.type.YesNoConverter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.Uni;
import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Tuple;
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
@DisabledFor(value = DB2, reason = "Needed to have 6 in buffer but only had 0")
public class SoftDeleteSingleTableTest extends BaseReactiveTest {

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
	static final Deletable[] timestampEntities = {
			new TimestampEntity( 1, "timestamp first" ),
			new TimestampEntity( 2, "timestamp second" ),
			new TimestampEntity( 3, "timestamp third" ),
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
				.call( () -> getMutinySessionFactory().withTransaction( session -> session.persistAll( timestampEntities ) ) )
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
		// I'm using Temporal because different databases may return different types
		testSoftDelete( context, timestamp -> timestamp instanceof Temporal, "deleted", TimestampEntity.class, timestampEntities,
						() -> getMutinySessionFactory().withTransaction( s -> s
								.remove( s.getReference( TimestampEntity.class, timestampEntities[0].getId() ) )
						)
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
		testSoftDelete( context, SoftDeleteSingleTableTest::deleted, "deleted", ImplicitEntity.class, implicitEntities,
				() -> getMutinySessionFactory().withTransaction( s -> s
						.remove( s.getReference( ImplicitEntity.class, implicitEntities[0].getId() ) )
				)
		);
	}

	@Test
	public void testDeletionWithHQLQuery(VertxTestContext context) {
		testSoftDelete( context, SoftDeleteSingleTableTest::deleted, "deleted", ImplicitEntity.class, implicitEntities,
						() -> getMutinySessionFactory().withTransaction( s -> s
						.createMutationQuery( "delete from ImplicitEntity where name = :name" )
						.setParameter( "name", implicitEntities[0].getName() )
						.executeUpdate()
				)
		);
	}

	@Test
	public void testDeletionWithCriteria(VertxTestContext context) {
		testSoftDelete( context, SoftDeleteSingleTableTest::deleted, "deleted", ImplicitEntity.class, implicitEntities,
				() -> getMutinySessionFactory().withTransaction( s -> {
					CriteriaBuilder cb = getSessionFactory().getCriteriaBuilder();
					CriteriaDelete<ImplicitEntity> delete = cb.createCriteriaDelete( ImplicitEntity.class );
					Root<ImplicitEntity> root = delete.from( ImplicitEntity.class );
					delete.where( cb.equal( root.get( "name" ), implicitEntities[0].getName() ) );
					return s.createQuery( delete ).executeUpdate();
				} )
		);
	}

	// Db2 saves a boolean as a number
	private static boolean deleted(Object obj) {
		return requireNonNull( dbType() ) == DB2
				? ( (short) obj ) == 1
				: (boolean) obj;
	}

	private void testSoftDelete(
			VertxTestContext context,
			Predicate<Object> deleted,
			String deletedColumn,
			Class<?> entityClass,
			Deletable[] entities, Supplier<Uni<?>> deleteFirstEntityFun) {
		test( context, getMutinySessionFactory()
				// Check that the soft delete column exists and has the expected initial value
				.withSession( s -> s
						// This SQL query should be compatible with all databases
						.createNativeQuery( "select id, name, " + deletedColumn + " from " + entityClass.getSimpleName() + " order by id", Tuple.class )
						.getResultList()
						.invoke( tuples -> {
							assertThat( tuples ).hasSize( entities.length );
							for ( int i = 0; i < tuples.size(); i++ ) {
								Tuple tuple = tuples.get( i );
								// The id from a native query is not always returned as an int (Oracle returns a BigDecimal)
								int tupleId = tuple.get( "id", Number.class ).intValue();
								assertThat( tupleId ).isEqualTo( entities[i].getId() );
								assertThat( tuple.get( "name", String.class ) ).isEqualTo( entities[i].getName() );
								boolean markedAsDeleted = deleted.test( tuple.get( deletedColumn ) );
								// No entity should be marked as deleted
								assertThat( markedAsDeleted ).isFalse();
							}
						} )
				)
				// Delete an entity
				.call( deleteFirstEntityFun )
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
						.createNativeQuery( "select id, name, " + deletedColumn + " from " + entityClass.getSimpleName() + " order by id", Tuple.class )
						.getResultList()
						.invoke( tuples -> {
							assertThat( tuples ).hasSize( entities.length );
							for ( int i = 0; i < tuples.size(); i++ ) {
								Tuple tuple = tuples.get( i );
								// The id from a native query is not always returned as an int (Oracle returns a BigDecimal)
								int tupleId = tuple.get( "id", Number.class ).intValue();
								assertThat( tupleId ).isEqualTo( entities[i].getId() );
								assertThat( tuple.get( "name", String.class ) ).isEqualTo( entities[i].getName() );
								boolean markedAsDeleted = deleted.test( tuple.get( deletedColumn ) );
								// Only the first element should have been marked as deleted
								assertThat( markedAsDeleted ).isEqualTo( i == 0 );
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
	public static class TimestampEntity implements Deletable {
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
