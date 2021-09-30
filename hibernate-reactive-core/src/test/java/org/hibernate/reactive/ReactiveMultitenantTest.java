/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.Arrays;
import java.util.Objects;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Version;

import org.hibernate.LockMode;
import org.hibernate.MultiTenancyStrategy;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.testing.DatabaseSelectionRule;
import org.hibernate.reactive.util.impl.CompletionStages;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.DEFAULT;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.TENANT_1;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.TENANT_2;
import static org.hibernate.reactive.MyCurrentTenantIdentifierResolver.Tenant.values;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.POSTGRESQL;

/**
 * This class creates multiple additional databases so that we can check that queries run
 * on the database for the selected tenant.
 */
public class ReactiveMultitenantTest extends BaseReactiveTest {

	private static final MyCurrentTenantIdentifierResolver TENANT_RESOLVER = new MyCurrentTenantIdentifierResolver();

	// To check if we are using the right database we run native queries for PostgreSQL
	@Rule
	public DatabaseSelectionRule selectionRule = DatabaseSelectionRule.runOnlyFor( POSTGRESQL );

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( GuineaPig.class );
		configuration.setProperty( Settings.MULTI_TENANT, MultiTenancyStrategy.DATABASE.name() );
		configuration.getProperties().put( Settings.MULTI_TENANT_IDENTIFIER_RESOLVER, TENANT_RESOLVER );
		// Contains the SQL scripts for the creation of the additional databases
		configuration.setProperty( Settings.HBM2DDL_IMPORT_FILES, "/multitenancy-test.sql" );
		configuration.setProperty( Settings.SQL_CLIENT_POOL, TenantDependentPool.class.getName() );
		return configuration;
	}

	@Test
	public void reactivePersistFindDelete(TestContext context) {
		TENANT_RESOLVER.setTenantIdentifier( DEFAULT );
		final GuineaPig guineaPig = new GuineaPig( 5, "Aloi" );
		test(
				context,
				getSessionFactory().newSession().thenCompose( session -> session
						.persist( guineaPig )
						.thenCompose( v -> session.flush() )
						.thenAccept( v -> session.detach( guineaPig ) )
						.thenAccept( v -> context.assertFalse( session.contains( guineaPig ) ) )
						.thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
						.thenAccept( actualPig -> {
							assertThatPigsAreEqual( context, guineaPig, actualPig );
							context.assertTrue( session.contains( actualPig ) );
							context.assertFalse( session.contains( guineaPig ) );
							context.assertEquals( LockMode.READ, session.getLockMode( actualPig ) );
							session.detach( actualPig );
							context.assertFalse( session.contains( actualPig ) );
						} )
						.thenCompose( v -> session.find( GuineaPig.class, guineaPig.getId() ) )
						.thenCompose( session::remove )
						.thenCompose( v -> session.flush() ) )
		);
	}

	@Test
	public void testTenantSelection(TestContext context) {
		TENANT_RESOLVER.setTenantIdentifier( TENANT_1 );
		test( context, openSession()
				.thenCompose( session -> session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.thenAccept( result -> context.assertEquals( TENANT_1.getDbName(), result ) ) )
				.thenAccept( unused -> TENANT_RESOLVER.setTenantIdentifier( TENANT_2 ) )
				.thenCompose( unused -> openSession() )
				.thenCompose( session -> session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.thenAccept( result -> context.assertEquals( TENANT_2.getDbName(), result ) ) )
		);
	}

	@Test
	public void testTenantSelectionStatelessSession(TestContext context) {
		TENANT_RESOLVER.setTenantIdentifier( TENANT_1 );
		test( context, getSessionFactory().newStatelessSession()
				.thenCompose( t1Session -> t1Session
					.createNativeQuery( "select current_database()" )
					.getSingleResult()
					.thenAccept( result -> context.assertEquals( TENANT_1.getDbName(), result ) )
					.thenCompose( unused -> t1Session.close() ) )
				.thenAccept( unused -> TENANT_RESOLVER.setTenantIdentifier( TENANT_2 ) )
				.thenCompose( v -> getSessionFactory().newStatelessSession() )
				.thenCompose( t2Session -> t2Session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.thenAccept( result -> context.assertEquals( TENANT_2.getDbName(), result ) )
						.thenCompose( v -> t2Session.close() ) )
		);
	}

	@Test
	public void testTenantSelectionStatelessSessionMutiny(TestContext context) {
		TENANT_RESOLVER.setTenantIdentifier( TENANT_1 );
		test( context, getMutinySessionFactory().withStatelessSession( t1Session ->
				t1Session
				.createNativeQuery( "select current_database()" )
				.getSingleResult()
				.invoke( result -> context.assertEquals( TENANT_1.getDbName(), result ) )
			  )
				.invoke( result -> TENANT_RESOLVER.setTenantIdentifier( TENANT_2 ) )
				.chain( () -> getMutinySessionFactory().withStatelessSession( t2Session -> t2Session
						.createNativeQuery( "select current_database()" )
						.getSingleResult()
						.invoke( result -> context.assertEquals( TENANT_2.getDbName(), result ) ) ) )
		);
	}

	@AfterClass
	public static void dropDatabases(TestContext context) {
		if ( factoryManager.isStarted() ) {
			test( context, getSessionFactory()
					.withSession( session -> Arrays
							.stream( values() )
							.filter( tenant -> tenant != DEFAULT )
							.collect(
									CompletionStages::voidFuture,
									(stage, tenant) -> session
											.createNativeQuery( "drop database if exists " + tenant.getDbName() + ";" )
											.executeUpdate()
											.thenCompose( CompletionStages::voidFuture ),
									(stage, stage2) -> stage.thenCompose( v -> stage2 )
							)
					) );
		}
	}

	private void assertThatPigsAreEqual(TestContext context, GuineaPig expected, GuineaPig actual) {
		context.assertNotNull( actual );
		context.assertEquals( expected.getId(), actual.getId() );
		context.assertEquals( expected.getName(), actual.getName() );
	}

	@Entity(name = "GuineaPig")
	@Table(name = "Pig")
	public static class GuineaPig {
		@Id
		private Integer id;
		private String name;
		@Version
		private int version;

		public GuineaPig() {
		}

		public GuineaPig(Integer id, String name) {
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
		public String toString() {
			return id + ": " + name;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			GuineaPig guineaPig = (GuineaPig) o;
			return Objects.equals( name, guineaPig.name );
		}

		@Override
		public int hashCode() {
			return Objects.hash( name );
		}
	}

}
