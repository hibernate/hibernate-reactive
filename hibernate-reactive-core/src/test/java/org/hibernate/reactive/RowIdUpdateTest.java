/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.annotations.RowId;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DBSelectionExtension;
import org.hibernate.reactive.testing.SqlStatementTracker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.MapsId;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.testing.DBSelectionExtension.skipTestsFor;

/**
 * Adapted from the test with the same name in Hibernate ORM: {@literal org.hibernate.orm.test.rowid.RowIdUpdateTest}
 */
public class RowIdUpdateTest extends BaseReactiveTest {

	// Db2: Exception: IllegalStateException: Needed to have 6 in buffer but only had 0
	// Oracle: Vert.x driver doesn't support RowId type parameters
	@RegisterExtension
	public final DBSelectionExtension skip = skipTestsFor( DB2, ORACLE );

	private static SqlStatementTracker sqlTracker;

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( ParentEntity.class, SimpleEntity.class );
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		sqlTracker = new SqlStatementTracker( RowIdUpdateTest::isUsingRowId, configuration.getProperties() );
		return configuration;
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	private static boolean isUsingRowId(String s) {
		return s.toLowerCase().startsWith( "update" );
	}

	@BeforeEach
	public void prepareDb(VertxTestContext context) {
		test( context, getMutinySessionFactory().withTransaction( session -> session.persistAll(
				new SimpleEntity( 1L, "initial_status" ),
				new ParentEntity( 2L, new SimpleEntity( 2L, "initial_status" ) )
		) ) );
	}

	@Test
	public void testSimpleUpdateSameTransaction(VertxTestContext context) {
		sqlTracker.clear();
		test( context, getMutinySessionFactory()
				.withTransaction( session -> {
					final SimpleEntity simpleEntity = new SimpleEntity( 3L, "initial_status" );
					return session.persist( simpleEntity )
							.call( session::flush )
							.invoke( () -> simpleEntity.setStatus( "new_status" ) );
				} )
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session.find( SimpleEntity.class, 3L ) ) )
				// the update should have used the primary key, as the row-id value is not available
				.invoke( RowIdUpdateTest::shouldUsePrimaryKey )
				.invoke( entity -> assertThat( entity ).hasFieldOrPropertyWithValue( "status", "new_status" ) )
		);
	}

	@Test
	public void testRelatedUpdateSameTransaction(VertxTestContext context) {
		sqlTracker.clear();
		test( context, getMutinySessionFactory()
				.withTransaction( session -> {
					final SimpleEntity simple = new SimpleEntity( 4L, "initial_status" );
					final ParentEntity parent = new ParentEntity( 4L, simple );
					return session.persist( parent )
							.call( session::flush )
							.invoke( () -> parent.getChild().setStatus( "new_status" ) );
				} )
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session.find( SimpleEntity.class, 4L ) ) )
				// the update should have used the primary key, as the row-id value is not available
				.invoke( RowIdUpdateTest::shouldUsePrimaryKey )
				.invoke( entity -> assertThat( entity ).hasFieldOrPropertyWithValue( "status", "new_status" ) )
		);
	}

	@Test
	public void testSimpleUpdateDifferentTransaction(VertxTestContext context) {
		sqlTracker.clear();
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session
						.find( SimpleEntity.class, 1L )
						.invoke( entity -> entity.setStatus( "new_status" ) )
				)
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session.find( SimpleEntity.class, 1L ) ) )
				.invoke( RowIdUpdateTest::shouldUseRowId )
				.invoke( entity -> assertThat( entity ).hasFieldOrPropertyWithValue( "status", "new_status" ) )
		);
	}

	@Test
	public void testRelatedUpdateRelatedDifferentTransaction(VertxTestContext context) {
		sqlTracker.clear();
		test( context, getMutinySessionFactory()
				.withTransaction( session -> session
						.find( ParentEntity.class, 2L )
						.invoke( entity -> entity.getChild().setStatus( "new_status" ) )
				)
				.invoke( RowIdUpdateTest::shouldUseRowId )
				.chain( () -> getMutinySessionFactory()
						.withSession( session -> session.find( SimpleEntity.class, 2L ) ) )
				.invoke( entity -> assertThat( entity )
						.hasFieldOrPropertyWithValue( "status", "new_status" )
				)
		);
	}

	private static void shouldUsePrimaryKey() {
		assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
		assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
				.matches( "update SimpleEntity set status=.+ where primary_key=.+" );
	}

	private static void shouldUseRowId() {
		// Not all databases have a rowId column
		String rowId = getDialect().rowId( "" );
		String column = rowId == null ? "primary_key" : rowId;
		assertThat( sqlTracker.getLoggedQueries() ).hasSize( 1 );
		assertThat( sqlTracker.getLoggedQueries().get( 0 ) )
				.matches( "update SimpleEntity set status=.+ where " + column + "=.+" );
	}

	@Entity(name = "SimpleEntity")
	@Table(name = "SimpleEntity")
	@RowId
	public static class SimpleEntity {
		@Id
		@Column(name = "primary_key")
		public Long primaryKey;

		public String status;

		public SimpleEntity() {
		}

		public SimpleEntity(Long primaryKey, String status) {
			this.primaryKey = primaryKey;
			this.status = status;
		}

		public String getStatus() {
			return status;
		}

		public void setStatus(String status) {
			this.status = status;
		}
	}

	@Entity(name = "ParentEntity")
	@Table(name = "ParentEntity")
	public static class ParentEntity {
		@Id
		public Long id;

		@OneToOne(cascade = CascadeType.ALL)
		@MapsId
		public SimpleEntity child;

		public ParentEntity() {
		}

		public ParentEntity(Long id, SimpleEntity child) {
			this.id = id;
			this.child = child;
		}

		public SimpleEntity getChild() {
			return child;
		}
	}
}
