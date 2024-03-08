/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Collection;
import java.util.List;

import org.hibernate.annotations.NaturalId;
import org.hibernate.annotations.SoftDelete;
import org.hibernate.annotations.SoftDeleteType;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.testing.SqlStatementTracker;
import org.hibernate.type.YesNoConverter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.junit5.VertxTestContext;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MARIA;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.MYSQL;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.ORACLE;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/*
 * Tests validity of @SoftDelete annotation value options
 * as well as verifying logged 'create table' and 'update' queries for each database
 *
 * @see org.hibernate.orm.test.softdelete.SimpleSoftDeleteTests
 */

public class SoftDeleteTest extends BaseReactiveTest {

	private static SqlStatementTracker sqlTracker;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		sqlTracker = new SqlStatementTracker( SoftDeleteTest::filter, configuration.getProperties() );
		return configuration;
	}

	@Override
	protected void addServices(StandardServiceRegistryBuilder builder) {
		sqlTracker.registerService( builder );
	}

	protected static boolean filter(String s) {
		String[] accepted = { "create table ", "update " };
		for ( String valid : accepted ) {
			if ( s.toLowerCase().startsWith( valid ) ) {
				return true;
			}
		}
		return false;
	}

	@Override
	protected Collection<Class<?>> annotatedEntities() {
		return List.of( ActiveEntity.class,DeletedEntity.class, ImplicitEntity.class );
	}

	@BeforeEach
	public void populateDB(VertxTestContext context) {

		Object[] entities = {
				new ActiveEntity( 1, "first" ),
				new ActiveEntity( 2, "second" ),
				new ActiveEntity( 3, "third" ),
				new DeletedEntity( 1, "first" ),
				new DeletedEntity( 2, "second" ),
				new DeletedEntity( 3, "third" ),
				new ImplicitEntity( 1, "first" ),
				new ImplicitEntity( 2, "second" ),
				new ImplicitEntity( 3, "third" )
		};

		test( context, getSessionFactory().withTransaction( session -> session.persist( entities ) ) );
	}

	@Test
	public void verifyActiveDeletedStrategies(VertxTestContext context) {

		String expectedQueryStartSimple = isDbType( MYSQL ) || isDbType( MARIA ) ?
				"create table SimpleActiveEntity (active char(1) not null comment 'Soft-delete indicator' check (active in ('Y','N'))" :
				"create table SimpleActiveEntity (active char(1) not null check (active in ('Y','N'))";
		String expectedQueryStartDeleted =  isDbType( MYSQL ) || isDbType( MARIA ) ?
				"create table SimpleDeletedEntity (deleted char(1) not null comment 'Soft-delete indicator' check (deleted in ('N','Y'))" :
				"create table SimpleDeletedEntity (deleted char(1) not null check (deleted in ('N','Y'))";
		String expectedQueryStartImplicit = isDbType( MYSQL ) || isDbType( MARIA ) ?
				"create table SimpleImplicitEntity (deleted bit not null comment 'Soft-delete indicator'" :
				"create table SimpleImplicitEntity (deleted boolean not null";
		if( isDbType( SQLSERVER ) )  {
			expectedQueryStartImplicit = "create table SimpleImplicitEntity (deleted bit not null";
		} else if( isDbType( ORACLE ) ) {
			expectedQueryStartImplicit = "create table SimpleImplicitEntity (deleted number(1,0) not null";
		}

		String finalExpectedQueryStartImplicit = expectedQueryStartImplicit;

		test( context, getSessionFactory().withSession( s ->
				s.createQuery( "from SimpleActiveEntity where id = 1", ActiveEntity.class )
						.getSingleResult()
						.thenAccept( result -> {
							assertEquals( "first", result.name );
							checkLoggedQueryStartsWith( expectedQueryStartSimple);
							checkLoggedQueryStartsWith( expectedQueryStartDeleted);
							checkLoggedQueryStartsWith( finalExpectedQueryStartImplicit );
						} ) ) );
	}

	@Test
	public void testUpdateRemovedQuery(VertxTestContext context) {
		// The "delete" query executes an "update" query which sets the "active" value to "N" to represent a soft delete
		// Querying SimpleActiveEntity table returns just 2 results
		String expectedQueryStartSimple = isDbType( ORACLE ) ?
				"update SimpleActiveEntity ae1_0 set ae1_0.active='N' where id=1 and active='Y'" :
				"update SimpleActiveEntity set active='N' where id=1 and active='Y'";
		test( context, getSessionFactory().withSession(
				s -> s.createMutationQuery( "delete SimpleActiveEntity where id = 1" )
						.executeUpdate()
						.thenCompose( result -> getSessionFactory().withSession(
								session -> session.createQuery( "from SimpleActiveEntity", ActiveEntity.class ).getResultList()
										.thenAccept( results -> {
											checkLoggedQueryStartsWith( expectedQueryStartSimple );
											assertEquals( 2, results.size() );
										} ) )
						) )
		);
	}

	@Test
	public void testUpdateNotRemovedQuery(VertxTestContext context) {
		test( context, getSessionFactory().withSession(
				session -> session.createMutationQuery( "update SimpleDeletedEntity set name = null where id > 1" )
						.executeUpdate()
						.thenCompose( v -> getSessionFactory().withSession(
								s -> s.createQuery( "from SimpleDeletedEntity", DeletedEntity.class ).getResultList()
										.thenAccept( results -> {
											assertEquals( 3, results.size() );
											assertEquals( 1, results.get(0).id );
											assertNull( results.get( 1 ).name );
											assertNull( results.get( 2 ).name );
										} ) ) ) )
		);
	}

	public static boolean isDbType(DatabaseConfiguration.DBType dbType) {
		return dbType() == dbType;
	}

	private void checkLoggedQueryStartsWith(String expectedQuery) {
		String foundQuery = null;
		for ( String loggedQuery : sqlTracker.getLoggedQueries() ) {
			if ( loggedQuery.startsWith( expectedQuery ) ) {
				foundQuery = loggedQuery;
				if ( foundQuery != null ) {
					return;
				}
			}
		}
		Assertions.assertEquals( expectedQuery, foundQuery );
	}

	@Entity(name = "SimpleActiveEntity")
	@SoftDelete(converter = YesNoConverter.class, strategy = SoftDeleteType.ACTIVE)
	public static class ActiveEntity {
		@Id
		private Integer id;
		@NaturalId
		private String name;

		public ActiveEntity() {
		}

		public ActiveEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	@Entity(name = "SimpleDeletedEntity")
	@SoftDelete(converter = YesNoConverter.class, strategy = SoftDeleteType.DELETED)
	public static class DeletedEntity {
		@Id
		private Integer id;
		@NaturalId
		private String name;

		public DeletedEntity() {
		}

		public DeletedEntity(Integer id, String name) {
			this.id = id;
			this.name = name;
		}
	}

	@Entity(name="SimpleImplicitEntity")
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
	}
}
